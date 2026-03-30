// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod auth;
mod delete;
mod env;
mod list;
mod manifest;
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
use terminal_games::control::AppEnvVar;

use crate::config::list_app_shortnames;
use crate::control_client::{AppClient, completion_runtime};

#[derive(Args)]
pub struct AppCli {
    #[command(subcommand)]
    command: AppCommand,
}

#[derive(Subcommand)]
enum AppCommand {
    /// Save an app token for future uploads and management.
    Auth(AppAuthArgs),
    /// Upload a new wasm build for the reserved shortname.
    /// `TERMINAL_GAMES_AUTHOR_TOKEN` can be used instead of prior `app auth` setup.
    Upload(AppUploadArgs),
    /// Extract, validate, and print the embedded manifest from a wasm file.
    Manifest(AppManifestArgs),
    #[command(subcommand)]
    /// Manage encrypted app environment variables.
    Env(AppEnvCommand),
    /// List configured app tokens and app playtime.
    #[command(visible_alias = "ls")]
    List,
    /// Rotate the current app token and save the replacement locally.
    RotateToken(AppRotateTokenArgs),
    /// Delete the reserved shortname and invalidate its token.
    Delete(AppDeleteArgs),
}

#[derive(Args)]
pub(super) struct AppAuthArgs {
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    token_stdin: bool,
}

#[derive(Args)]
pub(super) struct AppUploadArgs {
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    path_to_wasm_file: PathBuf,
    #[arg(add = ArgValueCandidates::new(complete_app_shortname_candidates))]
    shortname: Option<String>,
    /// Set env vars atomically during upload. Repeat as needed.
    #[arg(short = 'e', long = "env", value_name = "NAME=VALUE")]
    env: Vec<String>,
    /// Load env vars from a dotenv-style file and replace the full set atomically.
    #[arg(long = "env-file", add = ArgValueCompleter::new(PathCompleter::file()))]
    env_file: Option<PathBuf>,
}

#[derive(Args)]
pub(super) struct AppManifestArgs {
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    path_to_wasm_file: PathBuf,
}

#[derive(Args)]
pub(super) struct AppDeleteArgs {
    #[arg(long)]
    force: bool,
    #[arg(add = ArgValueCandidates::new(complete_app_shortname_candidates))]
    shortname: String,
}

#[derive(Args)]
pub(super) struct AppRotateTokenArgs {
    #[arg(add = ArgValueCandidates::new(complete_app_shortname_candidates))]
    shortname: String,
}

#[derive(Subcommand)]
pub(super) enum AppEnvCommand {
    /// List decrypted env vars for this shortname.
    #[command(visible_alias = "ls")]
    List(AppEnvListArgs),
    /// Set or overwrite one env var.
    Set(AppEnvSetArgs),
    /// Delete one env var.
    Delete(AppEnvDeleteArgs),
}

#[derive(Args)]
pub(super) struct AppEnvListArgs {
    #[arg(add = ArgValueCandidates::new(complete_app_shortname_candidates))]
    shortname: String,
}

#[derive(Args)]
pub(super) struct AppEnvSetArgs {
    #[arg(add = ArgValueCandidates::new(complete_app_shortname_candidates))]
    shortname: String,
    name: String,
    value: String,
}

#[derive(Args)]
pub(super) struct AppEnvDeleteArgs {
    #[arg(add = ArgValueCandidates::new(complete_app_shortname_candidates))]
    shortname: String,
    #[arg(add = ArgValueCandidates::new(complete_app_env_name_candidates))]
    name: String,
}

pub async fn run(cli: AppCli, profile: Option<String>) -> Result<()> {
    match cli.command {
        AppCommand::Auth(args) => auth::run(args).await,
        AppCommand::Upload(args) => upload::run(args, profile).await,
        AppCommand::Manifest(args) => manifest::run(args).await,
        AppCommand::Env(command) => env::run(command, profile).await,
        AppCommand::List => list::run().await,
        AppCommand::RotateToken(args) => rotate_token::run(args, profile).await,
        AppCommand::Delete(args) => delete::run(args, profile).await,
    }
}

fn complete_app_shortname_candidates() -> Vec<CompletionCandidate> {
    let profile = current_profile_from_args();
    list_app_shortnames(profile.as_deref())
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_app_env_name_candidates() -> Vec<CompletionCandidate> {
    std::panic::catch_unwind(complete_app_env_name_candidates_inner)
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_app_env_name_candidates_inner() -> Option<Vec<String>> {
    let shortname = current_app_shortname_from_args()?;
    let profile = current_profile_from_args();
    let client = AppClient::from_target(&shortname, profile.as_deref()).ok()?;
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

fn current_profile_from_args() -> Option<String> {
    let args = std::env::args().collect::<Vec<_>>();
    for (index, arg) in args.iter().enumerate() {
        if let Some(value) = arg.strip_prefix("--profile=") {
            return Some(value.to_string());
        }
        if arg == "--profile" {
            return args.get(index + 1).cloned();
        }
    }
    None
}

fn current_app_shortname_from_args() -> Option<String> {
    let args = std::env::args().collect::<Vec<_>>();
    let env_idx = args.iter().position(|arg| arg == "env")?;
    let subcommand = args.get(env_idx + 1)?.as_str();
    let mut values = Vec::new();
    let mut index = env_idx + 2;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--profile" {
            index += 2;
            continue;
        }
        if arg.starts_with("--profile=") {
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
) -> Result<Option<Vec<AppEnvVar>>> {
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
            envs.push(AppEnvVar {
                name: name.trim().to_string(),
                value: value.to_string(),
            });
        }
    }
    for pair in inline {
        let Some((name, value)) = pair.split_once('=') else {
            anyhow::bail!("invalid env assignment '{}'; expected NAME=VALUE", pair);
        };
        envs.push(AppEnvVar {
            name: name.to_string(),
            value: value.to_string(),
        });
    }
    Ok(Some(envs))
}
