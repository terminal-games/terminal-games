// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use clap_complete::{ArgValueCandidates, ArgValueCompleter, CompletionCandidate, PathCompleter};
use dialoguer::{Confirm, Password};
use reqwest::multipart;
use terminal_games::{
    control::{
        AuthorEnvDeleteRequest, AuthorEnvListResponse, AuthorEnvSetRequest, AuthorEnvVar,
        AuthorSelfResponse, AuthorTokenClaims, DeleteShortnameRequest, DeleteShortnameResponse,
        RotateAuthorTokenResponse, UploadGameResponse,
    },
    manifest::extract_manifest_from_wasm,
};

use crate::config::{
    default_profile_name_value, format_seconds, list_admin_profiles, list_author_refs, load_author_token_for_shortname,
    load_author_tokens_for_listing, normalize_base_url, parse_author_ref, print_table,
    read_secret_stdin, save_author_token,
};

#[derive(Args)]
pub struct AuthorCli {
    #[command(subcommand)]
    command: AuthorCommand,
}

#[derive(Subcommand)]
enum AuthorCommand {
    /// Save an author token for future uploads and management.
    Auth(AuthorAuthArgs),
    /// Upload a new wasm build for the reserved shortname.
    Upload(AuthorUploadArgs),
    #[command(subcommand)]
    /// Manage encrypted app environment variables.
    Env(AuthorEnvCommand),
    /// List configured author tokens and app playtime.
    #[command(visible_alias = "ls")]
    List,
    /// Rotate the current author token and save the replacement locally.
    RotateToken(AuthorRotateTokenArgs),
    /// Delete the reserved shortname and invalidate its token.
    Delete(AuthorDeleteArgs),
}

#[derive(Args)]
struct AuthorAuthArgs {
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    token_stdin: bool,
}

#[derive(Args)]
struct AuthorUploadArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    path_to_wasm_file: PathBuf,
    /// Set env vars atomically during upload. Repeat as needed.
    #[arg(short = 'e', value_name = "NAME=VALUE")]
    env: Vec<String>,
    /// Load env vars from a dotenv-style file and replace the full set atomically.
    #[arg(long = "env-file", add = ArgValueCompleter::new(PathCompleter::file()))]
    env_file: Option<PathBuf>,
}

#[derive(Args)]
struct AuthorDeleteArgs {
    #[arg(long)]
    force: bool,
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
}

#[derive(Args)]
struct AuthorRotateTokenArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
}

#[derive(Subcommand)]
enum AuthorEnvCommand {
    /// List decrypted env vars for this shortname.
    #[command(visible_alias = "ls")]
    List(AuthorEnvListArgs),
    /// Set or overwrite one env var.
    Set(AuthorEnvSetArgs),
    /// Delete one env var.
    Delete(AuthorEnvDeleteArgs),
}

#[derive(Args)]
struct AuthorEnvListArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
}

#[derive(Args)]
struct AuthorEnvSetArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
    name: String,
    value: String,
}

#[derive(Args)]
struct AuthorEnvDeleteArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
    #[arg(add = ArgValueCandidates::new(complete_author_env_name_candidates))]
    name: String,
}

pub async fn run(cli: AuthorCli) -> Result<()> {
    match cli.command {
        AuthorCommand::Auth(args) => auth(args).await,
        AuthorCommand::Upload(args) => upload(args).await,
        AuthorCommand::Env(command) => env(command).await,
        AuthorCommand::List => list().await,
        AuthorCommand::RotateToken(args) => rotate_token(args).await,
        AuthorCommand::Delete(args) => delete(args).await,
    }
}

fn complete_author_ref_candidates() -> Vec<CompletionCandidate> {
    list_author_refs()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_author_env_name_candidates() -> Vec<CompletionCandidate> {
    std::panic::catch_unwind(complete_author_env_name_candidates_inner)
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_author_env_name_candidates_inner() -> Option<Vec<String>> {
    let author_ref = current_author_ref_from_args()?;
    let claims = load_author_claims_for_ref(&author_ref).ok()?;
    let client = author_client(&claims).ok()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    runtime.block_on(async move {
        let response = client
            .get(format!("{}/control/author/env/list", claims.url))
            .bearer_auth(claims.encode().ok()?)
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .json::<AuthorEnvListResponse>()
            .await
            .ok()?;
        let mut names = response
            .envs
            .into_iter()
            .map(|env| env.name)
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Some(names)
    })
}

async fn auth(args: AuthorAuthArgs) -> Result<()> {
    let token = if let Some(token) = args.token {
        token
    } else if args.token_stdin {
        read_secret_stdin()?
    } else {
        Password::new()
            .with_prompt("Author token")
            .interact()?
    };
    let claims = AuthorTokenClaims::decode(token.trim())?;
    let mut normalized = claims.clone();
    normalized.url = normalize_base_url(&claims.url)?;
    let profile_name = infer_author_profile_name(&normalized.url)?;
    save_author_token(&profile_name, &normalized)?;
    println!(
        "Saved author token for '{}' on {} as profile '{}'",
        normalized.shortname, normalized.url, profile_name
    );
    Ok(())
}

async fn upload(args: AuthorUploadArgs) -> Result<()> {
    let wasm = fs::read(&args.path_to_wasm_file)
        .with_context(|| format!("failed to read {}", args.path_to_wasm_file.display()))?;
    let manifest = extract_manifest_from_wasm(&wasm)?
        .ok_or_else(|| anyhow::anyhow!("missing embedded terminal-games manifest"))?;
    manifest.validate()?;
    let claims = load_author_claims_for_ref(&args.author_ref)?;
    let (_, ref_shortname) = parse_author_ref(&args.author_ref)?;
    anyhow::ensure!(
        manifest.shortname == ref_shortname,
        "manifest shortname '{}' does not match target '{}'",
        manifest.shortname,
        args.author_ref
    );

    let client = author_client(&claims)?;
    let envs = load_upload_envs(&args.env, args.env_file.as_ref())?;
    let mut form = multipart::Form::new().part(
        "wasm",
        multipart::Part::bytes(wasm).file_name(
            args.path_to_wasm_file
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("game.wasm")
                .to_string(),
        ),
    );
    if let Some(envs) = envs {
        form = form.text("envs", serde_json::to_string(&envs)?);
    }
    let response = client
        .post(format!("{}/control/author/upload", claims.url))
        .bearer_auth(claims.encode()?)
        .multipart(form)
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let reason = status.canonical_reason().unwrap_or("error");
        let body = body.trim();
        if body.is_empty() {
            anyhow::bail!("HTTP {} {}", status.as_u16(), reason);
        }
        anyhow::bail!("HTTP {} {}: {}", status.as_u16(), reason, body);
    }
    let response: UploadGameResponse = response.json().await?;
    println!(
        "Uploaded '{}' version {} (game id {})",
        response.shortname,
        response.version,
        response.game_id,
    );
    Ok(())
}

async fn env(command: AuthorEnvCommand) -> Result<()> {
    match command {
        AuthorEnvCommand::List(args) => env_list(args).await,
        AuthorEnvCommand::Set(args) => env_set(args).await,
        AuthorEnvCommand::Delete(args) => env_delete(args).await,
    }
}

async fn list() -> Result<()> {
    let mut rows = Vec::new();
    for entry in load_author_tokens_for_listing(None)? {
        let client = author_client(&entry.claims)?;
        let response = client
            .get(format!("{}/control/author/self", entry.claims.url))
            .bearer_auth(entry.claims.encode()?)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let reason = status.canonical_reason().unwrap_or("error");
            let body = body.trim();
            if body.is_empty() {
                anyhow::bail!(
                    "failed to fetch author info for '{}' on {}: HTTP {} {}",
                    entry.claims.shortname,
                    entry.claims.url,
                    status.as_u16(),
                    reason
                );
            }
                anyhow::bail!(
                    "failed to fetch author info for '{}' on {}: HTTP {} {}: {}",
                    entry.claims.shortname,
                    entry.claims.url,
                    status.as_u16(),
                    reason,
                    body
                );
        }
        let response = response.json::<AuthorSelfResponse>().await?;
        rows.push(vec![
            entry.profile,
            if response.author_name.trim().is_empty() {
                "-".to_string()
            } else {
                response.author_name
            },
            response.shortname,
            response.server,
            format_seconds(response.playtime_seconds),
        ]);
    }
    rows.sort();
    print_table(&["Profile", "Author", "Shortname", "Server", "Playtime"], &rows);
    Ok(())
}

async fn rotate_token(args: AuthorRotateTokenArgs) -> Result<()> {
    let (target_profile, _) = parse_author_ref(&args.author_ref)?;
    let claims = load_author_claims_for_ref(&args.author_ref)?;
    let client = author_client(&claims)?;
    let response: RotateAuthorTokenResponse = client
        .post(format!("{}/control/author/rotate-token", claims.url))
        .bearer_auth(claims.encode()?)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let claims = AuthorTokenClaims::decode(&response.token)?;
    save_author_token(&target_profile, &claims)?;
    println!("Rotated token for '{}'", args.author_ref);
    println!("Token: {}", response.token);
    Ok(())
}

async fn delete(args: AuthorDeleteArgs) -> Result<()> {
    let (_, shortname) = parse_author_ref(&args.author_ref)?;
    if !args.force
        && !Confirm::new()
            .with_prompt(format!(
                "Delete '{}' permanently? This cannot be undone.",
                args.author_ref
            ))
            .default(false)
            .interact()?
    {
        return Ok(());
    }

    let claims = load_author_claims_for_ref(&args.author_ref)?;
    let client = author_client(&claims)?;
    let response: DeleteShortnameResponse = client
        .post(format!("{}/control/author/delete", claims.url))
        .bearer_auth(claims.encode()?)
        .json(&DeleteShortnameRequest {
            shortname,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    println!("Deleted '{}'", response.shortname);
    Ok(())
}

async fn env_list(args: AuthorEnvListArgs) -> Result<()> {
    let claims = load_author_claims_for_ref(&args.author_ref)?;
    let client = author_client(&claims)?;
    let response: AuthorEnvListResponse = client
        .get(format!("{}/control/author/env/list", claims.url))
        .bearer_auth(claims.encode()?)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let rows = response
        .envs
        .into_iter()
        .map(|env| vec![env.name, env.value])
        .collect::<Vec<_>>();
    print_table(&["Name", "Value"], &rows);
    Ok(())
}

async fn env_set(args: AuthorEnvSetArgs) -> Result<()> {
    let claims = load_author_claims_for_ref(&args.author_ref)?;
    let client = author_client(&claims)?;
    client
        .post(format!("{}/control/author/env/set", claims.url))
        .bearer_auth(claims.encode()?)
        .json(&AuthorEnvSetRequest {
            envs: vec![AuthorEnvVar {
                name: args.name,
                value: args.value,
            }],
            replace: false,
        })
        .send()
        .await?
        .error_for_status()?;
    println!("Updated env var for '{}'", args.author_ref);
    Ok(())
}

async fn env_delete(args: AuthorEnvDeleteArgs) -> Result<()> {
    let claims = load_author_claims_for_ref(&args.author_ref)?;
    let client = author_client(&claims)?;
    client
        .post(format!("{}/control/author/env/delete", claims.url))
        .bearer_auth(claims.encode()?)
        .json(&AuthorEnvDeleteRequest { name: args.name })
        .send()
        .await?
        .error_for_status()?;
    println!("Deleted env var for '{}'", args.author_ref);
    Ok(())
}

fn author_client(_claims: &AuthorTokenClaims) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder().build()?)
}

fn load_author_claims_for_shortname(shortname: &str, profile: Option<&str>) -> Result<AuthorTokenClaims> {
    load_author_token_for_shortname(shortname, profile)?
        .ok_or_else(|| anyhow::anyhow!("no author token configured for '{}'", shortname))
}

fn load_author_claims_for_ref(author_ref: &str) -> Result<AuthorTokenClaims> {
    let (profile, shortname) = parse_author_ref(author_ref)?;
    load_author_claims_for_shortname(&shortname, Some(&profile))
}

fn infer_author_profile_name(normalized_url: &str) -> Result<String> {
    let profiles = list_admin_profiles()?;
    let matches = profiles
        .iter()
        .filter(|(_, profile)| profile.url == normalized_url)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [only] => Ok(only.clone()),
        [] => Ok(normalized_url.to_string()),
        many => {
            let default_profile = default_profile_name_value()?;
            if many.iter().any(|name| name == &default_profile) {
                Ok(default_profile)
            } else {
                anyhow::bail!(
                    "multiple admin profiles point at '{}': {}",
                    normalized_url,
                    many.join(", ")
                )
            }
        }
    }
}

fn current_author_ref_from_args() -> Option<String> {
    let args = std::env::args().collect::<Vec<_>>();
    let env_idx = args.iter().position(|arg| arg == "env")?;
    match args.get(env_idx + 1).map(String::as_str) {
        Some("list" | "set" | "delete") => args.get(env_idx + 2).cloned(),
        _ => None,
    }
}

fn load_upload_envs(
    inline: &[String],
    env_file: Option<&PathBuf>,
) -> Result<Option<Vec<AuthorEnvVar>>> {
    if inline.is_empty() && env_file.is_none() {
        return Ok(None);
    }
    let mut envs = Vec::new();
    if let Some(env_file) = env_file {
        let contents = fs::read_to_string(env_file)
            .with_context(|| format!("failed to read {}", env_file.display()))?;
        for (line_no, line) in contents.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let Some((name, value)) = trimmed.split_once('=') else {
                anyhow::bail!(
                    "invalid env file line {} in {}",
                    line_no + 1,
                    env_file.display()
                );
            };
            envs.push(AuthorEnvVar {
                name: name.trim().to_string(),
                value: value.to_string(),
            });
        }
    }
    for pair in inline {
        let Some((name, value)) = pair.split_once('=') else {
            anyhow::bail!("invalid env assignment '{}'; expected NAME=VALUE", pair);
        };
        envs.push(AuthorEnvVar {
            name: name.to_string(),
            value: value.to_string(),
        });
    }
    Ok(Some(envs))
}
