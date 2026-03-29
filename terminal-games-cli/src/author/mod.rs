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
    time::Duration,
};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use clap_complete::{ArgValueCandidates, ArgValueCompleter, CompletionCandidate, PathCompleter};
use terminal_games::control::AuthorEnvVar;

use crate::config::{list_author_shortnames, list_author_urls};
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
    #[arg(long, add = ArgValueCandidates::new(complete_author_url_candidates))]
    url: Option<String>,
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    path_to_wasm_file: PathBuf,
    #[arg(add = ArgValueCandidates::new(complete_author_shortname_candidates))]
    shortname: Option<String>,
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
    #[arg(long, add = ArgValueCandidates::new(complete_author_url_candidates))]
    url: Option<String>,
    #[arg(add = ArgValueCandidates::new(complete_author_shortname_candidates))]
    shortname: String,
}

#[derive(Args)]
pub(super) struct AuthorRotateTokenArgs {
    #[arg(long, add = ArgValueCandidates::new(complete_author_url_candidates))]
    url: Option<String>,
    #[arg(add = ArgValueCandidates::new(complete_author_shortname_candidates))]
    shortname: String,
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
    #[arg(long, add = ArgValueCandidates::new(complete_author_url_candidates))]
    url: Option<String>,
    #[arg(add = ArgValueCandidates::new(complete_author_shortname_candidates))]
    shortname: String,
}

#[derive(Args)]
pub(super) struct AuthorEnvSetArgs {
    #[arg(long, add = ArgValueCandidates::new(complete_author_url_candidates))]
    url: Option<String>,
    #[arg(add = ArgValueCandidates::new(complete_author_shortname_candidates))]
    shortname: String,
    name: String,
    value: String,
}

#[derive(Args)]
pub(super) struct AuthorEnvDeleteArgs {
    #[arg(long, add = ArgValueCandidates::new(complete_author_url_candidates))]
    url: Option<String>,
    #[arg(add = ArgValueCandidates::new(complete_author_shortname_candidates))]
    shortname: String,
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

fn complete_author_shortname_candidates() -> Vec<CompletionCandidate> {
    let url = current_author_url_from_args();
    list_author_shortnames(url.as_deref())
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_author_url_candidates() -> Vec<CompletionCandidate> {
    list_author_urls()
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
    let shortname = current_author_shortname_from_args()?;
    let url = current_author_url_from_args();
    let client = AuthorClient::from_target(&shortname, url.as_deref()).ok()?;
    let runtime = completion_runtime()?;
    let result = runtime.block_on(async move {
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
    });
    runtime.shutdown_timeout(Duration::from_millis(100));
    result
}

fn current_author_url_from_args() -> Option<String> {
    let args = std::env::args().collect::<Vec<_>>();
    for (index, arg) in args.iter().enumerate() {
        if let Some(value) = arg.strip_prefix("--url=") {
            return Some(value.to_string());
        }
        if arg == "--url" {
            return args.get(index + 1).cloned();
        }
    }
    None
}

fn current_author_shortname_from_args() -> Option<String> {
    let args = std::env::args().collect::<Vec<_>>();
    let env_idx = args.iter().position(|arg| arg == "env")?;
    let subcommand = args.get(env_idx + 1)?.as_str();
    let mut values = Vec::new();
    let mut index = env_idx + 2;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--url" {
            index += 2;
            continue;
        }
        if arg.starts_with("--url=") {
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            index += 1;
            continue;
        }
        values.push(arg.clone());
        index += 1;
    }
    match subcommand {
        "list" | "set" | "delete" => values.first().cloned(),
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
