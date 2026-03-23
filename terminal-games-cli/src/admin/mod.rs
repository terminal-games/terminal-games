// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod auth;
mod author;
mod ban_ip;
mod broadcast;
mod regions;
mod session;
mod ticker;

use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use clap_complete::{ArgValueCandidates, CompletionCandidate};
use terminal_games::control::AuthorSummary;

use crate::config::list_admin_profile_names;
use crate::control_client::{AdminClient, completion_runtime};

#[derive(Args)]
pub struct AdminCli {
    /// Admin profile / cluster to use. Defaults to the CLI default profile.
    #[arg(long, global = true, add = ArgValueCandidates::new(complete_admin_profile_candidates))]
    profile: Option<String>,
    #[command(subcommand)]
    command: AdminCommand,
}

#[derive(Subcommand)]
enum AdminCommand {
    /// Configure an admin profile with server URL and shared secret.
    Auth(AdminAuthArgs),
    #[command(subcommand)]
    /// Manage IP bans across the fleet.
    BanIp(AdminBanIpCommand),
    #[command(subcommand)]
    /// Manage status-bar tickers across the fleet.
    Ticker(AdminTickerCommand),
    /// Show a temporary broadcast notification to users.
    Broadcast(AdminBroadcastArgs),
    /// Show runtime status for each region.
    Regions,
    #[command(subcommand)]
    /// Inspect and control live sessions.
    Session(AdminSessionCommand),
    #[command(subcommand)]
    /// Manage author tokens and reservations.
    Author(AdminAuthorCommand),
}

#[derive(Args)]
pub(super) struct AdminAuthArgs {
    #[arg(long)]
    url: Option<String>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long)]
    password_stdin: bool,
}

#[derive(Args, Clone)]
pub(super) struct AdminBanIpExpiryArgs {
    /// Relative duration like 1h, 1 day, or 1 week.
    #[arg(long)]
    duration: Option<String>,
    /// Absolute UTC expiry in RFC3339 format.
    #[arg(long = "expires-at")]
    expires_at: Option<String>,
}

#[derive(Subcommand)]
pub(super) enum AdminBanIpCommand {
    /// Add or update an IP/CIDR ban.
    Add(AdminBanIpAddArgs),
    /// List active IP bans.
    #[command(visible_alias = "ls")]
    List,
    /// Remove an IP/CIDR ban.
    #[command(visible_alias = "rm")]
    Remove(AdminBanIpRemoveArgs),
}

#[derive(Args)]
pub(super) struct AdminBanIpAddArgs {
    ip: String,
    reason: String,
    #[command(flatten)]
    expiry: AdminBanIpExpiryArgs,
}

#[derive(Args)]
pub(super) struct AdminBanIpRemoveArgs {
    ip: String,
}

#[derive(Subcommand)]
pub(super) enum AdminTickerCommand {
    /// List active tickers.
    #[command(visible_alias = "ls")]
    List,
    /// Add a ticker entry.
    Add(AdminTickerAddArgs),
    /// Replace the full ticker order with all ticker ids.
    Reorder(AdminTickerReorderArgs),
    /// Remove a ticker entry by id.
    #[command(visible_alias = "rm")]
    Remove(AdminTickerRemoveArgs),
}

#[derive(Args)]
pub(super) struct AdminTickerAddArgs {
    content: String,
    #[command(flatten)]
    expiry: AdminBanIpExpiryArgs,
}

#[derive(Args)]
pub(super) struct AdminTickerRemoveArgs {
    #[arg(add = ArgValueCandidates::new(complete_ticker_id_candidates))]
    ticker_id: u64,
}

#[derive(Args)]
pub(super) struct AdminTickerReorderArgs {
    #[arg(
        add = ArgValueCandidates::new(complete_ticker_id_candidates),
        value_delimiter = ',',
        num_args = 1..
    )]
    ticker_ids: Vec<u64>,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum BroadcastLevelArg {
    Info,
    Warning,
    Error,
}

#[derive(Args)]
pub(super) struct AdminBroadcastArgs {
    /// Notification level. Defaults to info.
    #[arg(long, value_enum, default_value = "info")]
    level: BroadcastLevelArg,
    /// Comma-separated region ids. Defaults to all regions.
    #[arg(long)]
    regions: Option<String>,
    message: String,
    duration: String,
}

#[derive(Subcommand)]
pub(super) enum AdminSessionCommand {
    /// List all live sessions across all discovered regions.
    #[command(visible_alias = "ls")]
    List,
    /// Disconnect a live session by its region-scoped session id.
    Kick(AdminSessionKickArgs),
    /// Attach to a live session for monitoring or read-write control.
    Spy(AdminSessionSpyArgs),
}

#[derive(Args)]
pub(super) struct AdminSessionKickArgs {
    #[arg(add = ArgValueCandidates::new(complete_session_id_candidates))]
    session_id: String,
}

#[derive(Args)]
pub(super) struct AdminSessionSpyArgs {
    /// Allow sending input to the session instead of attaching read-only.
    #[arg(long)]
    rw: bool,
    /// Start with the remote user input overlay hidden.
    #[arg(long = "hide-input")]
    hide_input: bool,
    #[arg(add = ArgValueCandidates::new(complete_session_id_candidates))]
    session_id: String,
}

#[derive(Subcommand)]
pub(super) enum AdminAuthorCommand {
    /// Reserve a shortname and mint a new author token.
    Create(AdminAuthorCreateArgs),
    /// List all reserved author shortnames and their playtime.
    #[command(visible_alias = "ls")]
    List,
    /// Rotate an author token and print the new value.
    RotateToken(AdminAuthorRotateTokenArgs),
    /// Delete an author reservation and its shortname permanently.
    Delete(AdminAuthorDeleteArgs),
}

#[derive(Args)]
pub(super) struct AdminAuthorCreateArgs {
    shortname: String,
}

#[derive(Args)]
pub(super) struct AdminAuthorDeleteArgs {
    #[arg(long)]
    force: bool,
    #[arg(add = ArgValueCandidates::new(complete_author_id_candidates))]
    author_id: u64,
}

#[derive(Args)]
pub(super) struct AdminAuthorRotateTokenArgs {
    #[arg(add = ArgValueCandidates::new(complete_author_id_candidates))]
    author_id: u64,
}

pub async fn run(cli: AdminCli) -> Result<()> {
    let profile = cli.profile;
    match cli.command {
        AdminCommand::Auth(args) => auth::run(args, profile).await,
        AdminCommand::BanIp(command) => ban_ip::run(command, profile).await,
        AdminCommand::Ticker(command) => ticker::run(command, profile).await,
        AdminCommand::Broadcast(args) => broadcast::run(args, profile).await,
        AdminCommand::Regions => regions::run(profile).await,
        AdminCommand::Session(command) => session::run(command, profile).await,
        AdminCommand::Author(command) => author::run(command, profile).await,
    }
}

fn complete_admin_profile_candidates() -> Vec<CompletionCandidate> {
    list_admin_profile_names()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_session_id_candidates() -> Vec<CompletionCandidate> {
    std::panic::catch_unwind(complete_session_id_candidates_inner)
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_session_id_candidates_inner() -> Option<Vec<String>> {
    completion_runtime()?.block_on(async {
        let api = load_api(None).ok()?;
        let mut session_ids = api
            .all_sessions()
            .await
            .ok()?
            .into_iter()
            .map(|session| session.session_id)
            .collect::<Vec<_>>();
        session_ids.sort();
        session_ids.dedup();
        Some(session_ids)
    })
}

fn complete_author_id_candidates() -> Vec<CompletionCandidate> {
    std::panic::catch_unwind(complete_author_id_candidates_inner)
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_author_id_candidates_inner() -> Option<Vec<String>> {
    completion_runtime()?.block_on(async {
        let api = load_api(None).ok()?;
        let authors: Vec<AuthorSummary> = api
            .rpc()
            .await
            .ok()?
            .author_list(terminal_games::control::rpc_context())
            .await
            .ok()?
            .map_err(anyhow::Error::msg)
            .ok()?;
        let mut ids = authors
            .into_iter()
            .map(|author| author.author_id.to_string())
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        Some(ids)
    })
}

fn complete_ticker_id_candidates() -> Vec<CompletionCandidate> {
    std::panic::catch_unwind(complete_ticker_id_candidates_inner)
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_ticker_id_candidates_inner() -> Option<Vec<String>> {
    completion_runtime()?.block_on(async {
        let api = load_api(None).ok()?;
        let mut ids = api
            .rpc()
            .await
            .ok()?
            .ticker_list(terminal_games::control::rpc_context())
            .await
            .ok()?
            .map_err(anyhow::Error::msg)
            .ok()?
            .into_iter()
            .map(|ticker| ticker.ticker_id.to_string())
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        Some(ids)
    })
}

pub(super) fn load_api(profile_override: Option<&str>) -> Result<AdminClient> {
    AdminClient::load(profile_override)
}

pub(super) async fn refresh_status_bar_state(api: &AdminClient) -> Result<()> {
    api.fanout(|rpc| async move {
        rpc.status_bar_refresh(terminal_games::control::rpc_context())
            .await?
            .map_err(anyhow::Error::msg)
    })
    .await?;
    Ok(())
}

pub(super) fn parse_session_ref(value: &str) -> Result<(String, u64)> {
    let (region, local_id) = value
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("session id must be in REGION:ID format"))?;
    Ok((
        region.to_string(),
        local_id
            .parse::<u64>()
            .with_context(|| format!("invalid session id '{local_id}'"))?,
    ))
}

pub(super) fn parse_regions_arg(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|region| !region.is_empty())
        .map(str::to_string)
        .collect()
}

pub(super) fn format_optional_unix(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "never".to_string())
}
