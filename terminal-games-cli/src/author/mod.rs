// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod auth;
mod delete;
mod env;
mod list;
mod rotate_token;
mod upload;

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use clap_complete::{ArgValueCandidates, ArgValueCompleter, CompletionCandidate, PathCompleter};
use terminal_games::control::AuthorEnvVar;

use crate::config::{default_profile_name_value, list_admin_profiles, list_author_refs};
use crate::control_client::{AuthorClient, completion_runtime};

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
pub(super) struct AuthorAuthArgs {
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    token_stdin: bool,
}

#[derive(Args)]
pub(super) struct AuthorUploadArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    path_to_wasm_file: PathBuf,
    /// Set env vars atomically during upload. Repeat as needed.
    #[arg(short = 'e', long = "env", value_name = "NAME=VALUE")]
    env: Vec<String>,
    /// Load env vars from a dotenv-style file and replace the full set atomically.
    #[arg(long = "env-file", add = ArgValueCompleter::new(PathCompleter::file()))]
    env_file: Option<PathBuf>,
}

#[derive(Args)]
pub(super) struct AuthorDeleteArgs {
    #[arg(long)]
    force: bool,
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
}

#[derive(Args)]
pub(super) struct AuthorRotateTokenArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
}

#[derive(Subcommand)]
pub(super) enum AuthorEnvCommand {
    /// List decrypted env vars for this shortname.
    #[command(visible_alias = "ls")]
    List(AuthorEnvListArgs),
    /// Set or overwrite one env var.
    Set(AuthorEnvSetArgs),
    /// Delete one env var.
    Delete(AuthorEnvDeleteArgs),
}

#[derive(Args)]
pub(super) struct AuthorEnvListArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
}

#[derive(Args)]
pub(super) struct AuthorEnvSetArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
    name: String,
    value: String,
}

#[derive(Args)]
pub(super) struct AuthorEnvDeleteArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_ref_candidates))]
    author_ref: String,
    #[arg(add = ArgValueCandidates::new(complete_author_env_name_candidates))]
    name: String,
}

pub async fn run(cli: AuthorCli) -> Result<()> {
    match cli.command {
        AuthorCommand::Auth(args) => auth::run(args).await,
        AuthorCommand::Upload(args) => upload::run(args).await,
        AuthorCommand::Env(command) => env::run(command).await,
        AuthorCommand::List => list::run().await,
        AuthorCommand::RotateToken(args) => rotate_token::run(args).await,
        AuthorCommand::Delete(args) => delete::run(args).await,
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
    let client = AuthorClient::from_ref(&author_ref).ok()?;
    let runtime = completion_runtime()?;
    runtime.block_on(async move {
        let response = client
            .rpc()
            .await
            .ok()?
            .env_list(terminal_games::control::rpc_context())
            .await
            .ok()?
            .map_err(anyhow::Error::msg)
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

pub(super) fn infer_author_profile_name(normalized_url: &str) -> Result<String> {
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

pub(crate) fn load_upload_envs(
    inline: &[String],
    env_file: Option<&Path>,
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
