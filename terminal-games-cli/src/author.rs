// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use clap_complete::{ArgValueCandidates, ArgValueCompleter, CompletionCandidate, PathCompleter};
use dialoguer::{Confirm, Password};
use terminal_games::{
    control::{
        AuthorSelfResponse, AuthorTokenClaims, DeleteShortnameRequest, DeleteShortnameResponse,
        UploadGameResponse,
    },
    manifest::extract_manifest_from_wasm,
};

use crate::config::{
    default_profile_name_value, format_seconds, list_admin_profile_names, list_admin_profiles,
    list_author_profile_names, list_author_shortnames, load_author_token_for_shortname,
    load_author_tokens_for_listing, normalize_base_url, print_table, read_secret_stdin,
    save_author_token,
};

#[derive(Args)]
pub struct AuthorCli {
    /// Author profile / cluster to use. Defaults to the CLI default profile.
    #[arg(long, global = true, add = ArgValueCandidates::new(complete_author_profile_candidates))]
    profile: Option<String>,
    #[command(subcommand)]
    command: AuthorCommand,
}

#[derive(Subcommand)]
enum AuthorCommand {
    /// Save an author token for future uploads and management.
    Auth(AuthorAuthArgs),
    /// Upload a new wasm build for the reserved shortname.
    Upload(AuthorUploadArgs),
    /// List configured author tokens and app playtime.
    #[command(visible_alias = "ls")]
    List,
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
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    path_to_wasm_file: PathBuf,
}

#[derive(Args)]
struct AuthorDeleteArgs {
    #[arg(long)]
    force: bool,
    #[arg(add = ArgValueCandidates::new(complete_author_shortname_candidates))]
    shortname: String,
}

pub async fn run(cli: AuthorCli) -> Result<()> {
    let profile = cli.profile;
    match cli.command {
        AuthorCommand::Auth(args) => auth(args, profile).await,
        AuthorCommand::Upload(args) => upload(args, profile).await,
        AuthorCommand::List => list(profile).await,
        AuthorCommand::Delete(args) => delete(args, profile).await,
    }
}

fn complete_author_profile_candidates() -> Vec<CompletionCandidate> {
    let mut names = list_admin_profile_names().unwrap_or_default();
    names.extend(list_author_profile_names().unwrap_or_default());
    names.sort();
    names.dedup();
    names
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_author_shortname_candidates() -> Vec<CompletionCandidate> {
    list_author_shortnames()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

async fn auth(args: AuthorAuthArgs, profile: Option<String>) -> Result<()> {
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
    let profile_name = profile.unwrap_or(default_profile_name_value()?);
    if let Some((_, admin_profile)) = list_admin_profiles()?
        .into_iter()
        .find(|(name, _)| name == &profile_name)
        && admin_profile.url != normalized.url
    {
        anyhow::bail!(
            "author token server '{}' does not match admin profile '{}' server '{}'",
            normalized.url,
            profile_name,
            admin_profile.url
        );
    }
    save_author_token(&profile_name, &normalized)?;
    println!(
        "Saved author token for '{}' on {} as profile '{}'",
        normalized.shortname, normalized.url, profile_name
    );
    Ok(())
}

async fn upload(args: AuthorUploadArgs, profile: Option<String>) -> Result<()> {
    let wasm = fs::read(&args.path_to_wasm_file)
        .with_context(|| format!("failed to read {}", args.path_to_wasm_file.display()))?;
    let manifest = extract_manifest_from_wasm(&wasm)?
        .ok_or_else(|| anyhow::anyhow!("missing embedded terminal-games manifest"))?;
    manifest.validate()?;
    let profile_name = profile.unwrap_or(default_profile_name_value()?);
    let claims = load_author_token_for_shortname(&manifest.shortname, Some(&profile_name))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no author token configured for '{}' in profile '{}'; run `terminal-games-cli author --profile {} auth` first",
                manifest.shortname,
                profile_name,
                profile_name
            )
        })?;

    let client = author_client(&claims)?;
    let response = client
        .post(format!("{}/control/author/upload", claims.url))
        .bearer_auth(claims.encode()?)
        .body(wasm)
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

async fn list(profile: Option<String>) -> Result<()> {
    let mut rows = Vec::new();
    for entry in load_author_tokens_for_listing(profile.as_deref())? {
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

async fn delete(args: AuthorDeleteArgs, profile: Option<String>) -> Result<()> {
    if !args.force
        && !Confirm::new()
            .with_prompt(format!(
                "Delete '{}' permanently? This cannot be undone.",
                args.shortname
            ))
            .default(false)
            .interact()?
    {
        return Ok(());
    }

    let profile_name = profile.unwrap_or(default_profile_name_value()?);
    let claims = load_author_token_for_shortname(&args.shortname, Some(&profile_name))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no author token configured for '{}' in profile '{}'; pass --profile or run `terminal-games-cli author --profile {} auth`",
                args.shortname,
                profile_name,
                profile_name
            )
        })?;
    let client = author_client(&claims)?;
    let response: DeleteShortnameResponse = client
        .post(format!("{}/control/author/delete", claims.url))
        .bearer_auth(claims.encode()?)
        .json(&DeleteShortnameRequest {
            shortname: args.shortname,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    println!("Deleted '{}'", response.shortname);
    Ok(())
}

fn author_client(_claims: &AuthorTokenClaims) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder().build()?)
}
