// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod app;
mod auth;
mod ban_ip;
mod broadcast;
mod cluster_ip;
mod regions;
mod session;
mod ticker;

use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use clap_complete::{ArgValueCandidates, CompletionCandidate};

use crate::{
    completion_cache::{self, CacheLookup, RefreshKind},
    config::{default_url_value, normalize_base_url},
    control_client::{AdminClient, completion_runtime},
};

#[derive(Args)]
pub struct AdminCli {
    #[command(subcommand)]
    command: AdminCommand,
}

#[derive(Subcommand)]
enum AdminCommand {
    /// Configure a server URL with its shared admin secret.
    Auth(AdminAuthArgs),
    #[command(subcommand)]
    /// Manage IP bans across the fleet.
    BanIp(AdminBanIpCommand),
    #[command(subcommand)]
    /// Manage status-bar tickers across the fleet.
    Ticker(AdminTickerCommand),
    /// Inspect repeat offender IPs observed by cluster bot detection.
    ClusterIpKicks,
    /// Show a temporary broadcast notification to users.
    Broadcast(AdminBroadcastArgs),
    /// Show runtime status for each region.
    Regions,
    #[command(subcommand)]
    /// Inspect and control live sessions.
    Session(AdminSessionCommand),
    #[command(subcommand)]
    /// Manage app tokens and reservations.
    App(AdminAppCommand),
}

#[derive(Args)]
pub(super) struct AdminAuthArgs {
    /// Server URL to save admin auth for.
    #[arg(long)]
    url: Option<String>,
    /// Shared admin secret to save without prompting.
    #[arg(long)]
    password: Option<String>,
    /// Read the shared admin secret from stdin.
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
    #[arg(add = ArgValueCandidates::new(complete_ban_ip_candidates))]
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
pub(super) enum AdminAppCommand {
    /// Reserve a shortname and mint a new app token.
    Create(AdminAppCreateArgs),
    /// List all reserved app shortnames and their playtime.
    #[command(visible_alias = "ls")]
    List,
    /// Rotate an app token and print the new value.
    RotateToken(AdminAppRotateTokenArgs),
    /// Delete an app reservation and its shortname permanently.
    Delete(AdminAppDeleteArgs),
}

#[derive(Args)]
pub(super) struct AdminAppCreateArgs {
    shortname: String,
}

#[derive(Args)]
pub(super) struct AdminAppDeleteArgs {
    #[arg(long)]
    force: bool,
    #[arg(add = ArgValueCandidates::new(complete_app_delete_candidates))]
    app: String,
}

#[derive(Args)]
pub(super) struct AdminAppRotateTokenArgs {
    #[arg(add = ArgValueCandidates::new(complete_app_id_candidates))]
    app_id: u64,
}

pub async fn run(cli: AdminCli, profile: Option<String>) -> Result<()> {
    match cli.command {
        AdminCommand::Auth(args) => auth::run(args, profile).await,
        AdminCommand::BanIp(command) => ban_ip::run(command, profile).await,
        AdminCommand::Ticker(command) => ticker::run(command, profile).await,
        AdminCommand::ClusterIpKicks => cluster_ip::run(profile).await,
        AdminCommand::Broadcast(args) => broadcast::run(args, profile).await,
        AdminCommand::Regions => regions::run(profile).await,
        AdminCommand::Session(command) => session::run(command, profile).await,
        AdminCommand::App(command) => app::run(command, profile).await,
    }
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
    if let Some(session_ids) = cached_session_id_candidates() {
        return Some(session_ids);
    }
    completion_runtime()?.block_on(async {
        let profile = current_profile_from_args();
        let api = load_api(profile.as_deref()).ok()?;
        let mut session_ids = api
            .completion_all_sessions()
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

fn complete_app_id_candidates() -> Vec<CompletionCandidate> {
    std::panic::catch_unwind(complete_app_id_candidates_inner)
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_app_delete_candidates() -> Vec<CompletionCandidate> {
    std::panic::catch_unwind(complete_app_delete_candidates_inner)
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_app_id_candidates_inner() -> Option<Vec<String>> {
    if let Some(app_ids) = cached_app_id_candidates() {
        return Some(app_ids);
    }
    completion_runtime()?.block_on(async {
        let profile = current_profile_from_args();
        let api = load_api(profile.as_deref()).ok()?;
        api.completion_app_ids().await.ok()
    })
}

fn complete_app_delete_candidates_inner() -> Option<Vec<String>> {
    if let Some(app_targets) = cached_app_delete_candidates() {
        return Some(app_targets);
    }
    completion_runtime()?.block_on(async {
        let profile = current_profile_from_args();
        let api = load_api(profile.as_deref()).ok()?;
        api.completion_app_targets().await.ok()
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
    if let Some(ticker_ids) = cached_ticker_id_candidates() {
        return Some(ticker_ids);
    }
    completion_runtime()?.block_on(async {
        let profile = current_profile_from_args();
        let api = load_api(profile.as_deref()).ok()?;
        api.completion_ticker_ids().await.ok()
    })
}

fn complete_ban_ip_candidates() -> Vec<CompletionCandidate> {
    std::panic::catch_unwind(complete_ban_ip_candidates_inner)
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

fn complete_ban_ip_candidates_inner() -> Option<Vec<String>> {
    if let Some(ban_ips) = cached_ban_ip_candidates() {
        return Some(ban_ips);
    }
    completion_runtime()?.block_on(async {
        let profile = current_profile_from_args();
        let api = load_api(profile.as_deref()).ok()?;
        api.completion_ban_ip_cidrs().await.ok()
    })
}

pub(super) fn load_api(url_override: Option<&str>) -> Result<AdminClient> {
    AdminClient::load(url_override)
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

pub(super) fn parse_app_delete_ref(value: &str) -> Result<(u64, &str)> {
    let (app_id, shortname) = value
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("app must be in ID:SHORTNAME format"))?;
    let app_id = app_id
        .parse::<u64>()
        .with_context(|| format!("invalid app id '{app_id}'"))?;
    let shortname = shortname.trim();
    anyhow::ensure!(!shortname.is_empty(), "app shortname cannot be empty");
    Ok((app_id, shortname))
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

fn current_completion_profile_url() -> Option<String> {
    if let Some(profile) = current_profile_from_args() {
        return normalize_base_url(&profile).ok();
    }
    default_url_value().ok().flatten()
}

fn cached_session_id_candidates() -> Option<Vec<String>> {
    let profile_url = current_completion_profile_url()?;
    let lookup = completion_cache::load_sessions(&profile_url).ok()?;
    cached_admin_candidates(lookup, RefreshKind::Sessions, &profile_url)
}

fn cached_app_id_candidates() -> Option<Vec<String>> {
    let profile_url = current_completion_profile_url()?;
    let lookup = completion_cache::load_apps(&profile_url).ok()?;
    cached_admin_candidates(lookup, RefreshKind::Apps, &profile_url).map(|apps| {
        apps.into_iter()
            .filter_map(|app| app.split_once(':').map(|(app_id, _)| app_id.to_string()))
            .collect()
    })
}

fn cached_app_delete_candidates() -> Option<Vec<String>> {
    let profile_url = current_completion_profile_url()?;
    let lookup = completion_cache::load_apps(&profile_url).ok()?;
    cached_admin_candidates(lookup, RefreshKind::Apps, &profile_url)
}

fn cached_ticker_id_candidates() -> Option<Vec<String>> {
    let profile_url = current_completion_profile_url()?;
    let lookup = completion_cache::load_tickers(&profile_url).ok()?;
    cached_admin_candidates(lookup, RefreshKind::Tickers, &profile_url)
}

fn cached_ban_ip_candidates() -> Option<Vec<String>> {
    let profile_url = current_completion_profile_url()?;
    let lookup = completion_cache::load_bans(&profile_url).ok()?;
    cached_admin_candidates(lookup, RefreshKind::Bans, &profile_url)
}

fn cached_admin_candidates(
    lookup: CacheLookup<Vec<String>>,
    kind: RefreshKind,
    profile_url: &str,
) -> Option<Vec<String>> {
    match lookup {
        CacheLookup::Missing => None,
        CacheLookup::Present {
            mut value,
            needs_refresh,
        } => {
            if needs_refresh {
                let _ = completion_cache::spawn_admin_refresh(kind, profile_url);
            }
            value.sort();
            value.dedup();
            Some(value)
        }
    }
}
