// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{collections::VecDeque, time::Instant};

use anyhow::{Context, Result};
use avt::{Color, Pen};
use clap::{Args, Subcommand};
use clap_complete::{ArgValueCandidates, CompletionCandidate};
use dialoguer::{Confirm, Input, Password};
use futures::{SinkExt, StreamExt, future::join_all};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::de::DeserializeOwned;
use terminal_games::control::{
    AuthorSummary, BanIpRequest, CreateAuthorRequest, CreateAuthorResponse, DeleteAuthorRequest,
    DeleteShortnameResponse, KickSessionRequest, RegionDiscoveryResponse, RegionRuntimeStatus, SessionSummary,
    SpyControlMessage,
};
use terminal_games::palette::{self, Color as PaletteColor};
use terminal_games::terminal_profile::TerminalProfile;
use terminput::{
    Event as TerminputEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind, ScrollDirection,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::config::{
    AdminProfile, CliConfig, derive_region_urls, format_bytes_per_second, format_duration,
    format_seconds, list_admin_profile_names, load_cli_config, normalize_base_url, print_table,
    read_secret_stdin, resolve_admin_profile, save_admin_profile, save_cli_config,
};

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
    /// Ban an IP address or CIDR range across the fleet.
    BanIp(AdminBanIpArgs),
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
struct AdminAuthArgs {
    #[arg(long)]
    url: Option<String>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long)]
    password_stdin: bool,
}

#[derive(Args)]
struct AdminBanIpArgs {
    ip: String,
    reason: String,
}

#[derive(Subcommand)]
enum AdminSessionCommand {
    /// List all live sessions across all discovered regions.
    #[command(visible_alias = "ls")]
    List,
    /// Disconnect a live session by its region-scoped session id.
    Kick(AdminSessionKickArgs),
    /// Attach to a live session for monitoring or read-write control.
    Spy(AdminSessionSpyArgs),
}

#[derive(Args)]
struct AdminSessionKickArgs {
    #[arg(add = ArgValueCandidates::new(complete_session_id_candidates))]
    session_id: String,
}

#[derive(Args)]
struct AdminSessionSpyArgs {
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
enum AdminAuthorCommand {
    /// Reserve a shortname and mint a new author token.
    Create(AdminAuthorCreateArgs),
    /// List all reserved author shortnames and their playtime.
    #[command(visible_alias = "ls")]
    List,
    /// Delete an author reservation and its shortname permanently.
    Delete(AdminAuthorDeleteArgs),
}

#[derive(Args)]
struct AdminAuthorCreateArgs {
    shortname: String,
}

#[derive(Args)]
struct AdminAuthorDeleteArgs {
    #[arg(long)]
    force: bool,
    #[arg(add = ArgValueCandidates::new(complete_author_id_candidates))]
    author_id: u64,
}

#[derive(Clone)]
struct AdminApi {
    client: reqwest::Client,
    profile: AdminProfile,
}

impl AdminApi {
    fn new(_profile_name: String, profile: AdminProfile) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", profile.password))
                .context("invalid password for authorization header")?,
        );
        let client = reqwest::Client::builder()
            .no_proxy()
            .default_headers(headers)
            .build()?;
        Ok(Self {
            client,
            profile,
        })
    }

    async fn get_json<T: DeserializeOwned>(&self, base_url: &str, path: &str) -> Result<T> {
        let url = format!("{base_url}{path}");
        Ok(self
            .client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json::<T>()
            .await?)
    }

    async fn post_json<B: serde::Serialize, T: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = format!("{base_url}{path}");
        Ok(self
            .client
            .post(url)
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json::<T>()
            .await?)
    }

    async fn post_empty<B: serde::Serialize>(&self, base_url: &str, path: &str, body: &B) -> Result<()> {
        let url = format!("{base_url}{path}");
        self.client
            .post(url)
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn discover(&self) -> Result<(RegionDiscoveryResponse, std::collections::BTreeMap<String, String>)> {
        let discovery: RegionDiscoveryResponse = self
            .get_json(&self.profile.url, "/control/admin/discover")
            .await?;
        let urls = derive_region_urls(&self.profile.url, &discovery)?;
        Ok((discovery, urls))
    }
}

pub async fn run(cli: AdminCli) -> Result<()> {
    let profile = cli.profile;
    match cli.command {
        AdminCommand::Auth(args) => auth(args, profile).await,
        AdminCommand::BanIp(args) => ban_ip(args, profile).await,
        AdminCommand::Regions => regions(profile).await,
        AdminCommand::Session(command) => session(command, profile).await,
        AdminCommand::Author(command) => author(command, profile).await,
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
    std::panic::catch_unwind(|| complete_session_id_candidates_inner())
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
        let (_, region_urls) = api.discover().await.ok()?;
        let futures = region_urls
            .values()
            .map(|base_url| api.get_json::<Vec<SessionSummary>>(base_url, "/control/admin/sessions"));
        let mut session_ids = Vec::new();
        for result in join_all(futures).await {
            let Ok(sessions) = result else { continue };
            session_ids.extend(sessions.into_iter().map(|session| session.session_id));
        }
        session_ids.sort();
        session_ids.dedup();
        Some(session_ids)
    })
}

fn complete_author_id_candidates() -> Vec<CompletionCandidate> {
    std::panic::catch_unwind(|| complete_author_id_candidates_inner())
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
            .get_json(&api.profile.url, "/control/admin/author/list")
            .await
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

async fn auth(args: AdminAuthArgs, profile_override: Option<String>) -> Result<()> {
    let current_config = load_cli_config()?;
    let url = match args.url {
        Some(url) => normalize_base_url(&url)?,
        None => normalize_base_url(
            &Input::<String>::new()
                .with_prompt("Server URL")
                .default("terminalgames.net".to_string())
                .interact_text()?,
        )?,
    };
    let password = if let Some(password) = args.password {
        password
    } else if args.password_stdin {
        read_secret_stdin()?
    } else {
        Password::new()
            .with_prompt("Shared admin secret")
            .interact()?
    };

    let profile_name = profile_override.unwrap_or_else(|| "default".to_string());
    save_admin_profile(
        &profile_name,
        AdminProfile {
            url: url.clone(),
            password,
        },
    )?;

    if profile_name == current_config.default_profile
        || Confirm::new()
            .with_prompt(format!(
                "Set '{}' as the default admin profile?",
                profile_name
            ))
            .default(profile_name == "default")
            .interact()?
    {
        save_cli_config(&CliConfig {
            default_profile: profile_name.clone(),
        })?;
    }

    println!("Saved admin profile '{}' for {}", profile_name, url);
    Ok(())
}

async fn ban_ip(args: AdminBanIpArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let request = BanIpRequest {
        ip: args.ip,
        reason: args.reason,
    };
    api.post_empty(&api.profile.url, "/control/admin/ban-ip", &request)
        .await?;
    let (_, region_urls) = api.discover().await?;
    let futures = region_urls
        .values()
        .map(|base_url| api.post_empty(base_url, "/control/admin/apply-ban", &request));
    for result in join_all(futures).await {
        result?;
    }
    println!("Applied ban across {} regions", region_urls.len());
    Ok(())
}

async fn regions(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let (_, region_urls) = api.discover().await?;
    let futures = region_urls.iter().map(|(_, base_url)| async {
        api.get_json::<RegionRuntimeStatus>(base_url, "/control/admin/regions")
            .await
    });
    let mut rows = Vec::new();
    for result in join_all(futures).await {
        let status = result?;
        rows.push(vec![
            status.region_id,
            status.current_sessions.to_string(),
            status.max_capacity.to_string(),
            format!("{:.1}%", status.cpu_usage_percent),
            format!(
                "{}/{} MiB",
                status.memory_used_bytes / 1024 / 1024,
                status.memory_total_bytes / 1024 / 1024
            ),
            format_bytes_per_second(status.bandwidth_bytes_per_second),
        ]);
    }
    rows.sort_by(|left, right| left[0].cmp(&right[0]));
    print_table(
        &["Region", "Sessions", "Capacity", "CPU", "Memory", "Bandwidth"],
        &rows,
    );
    Ok(())
}

async fn session(command: AdminSessionCommand, profile: Option<String>) -> Result<()> {
    match command {
        AdminSessionCommand::List => session_list(profile).await,
        AdminSessionCommand::Kick(args) => session_kick(args, profile).await,
        AdminSessionCommand::Spy(args) => session_spy(args, profile).await,
    }
}

async fn session_list(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let (_, region_urls) = api.discover().await?;
    let futures = region_urls.iter().map(|(_, base_url)| async {
        api.get_json::<Vec<SessionSummary>>(base_url, "/control/admin/sessions")
            .await
    });
    let mut sessions = Vec::new();
    for result in join_all(futures).await {
        sessions.extend(result?);
    }
    sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    let rows = sessions
        .into_iter()
        .map(|session| {
            vec![
                session.session_id,
                session
                    .user_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                session.region_id,
                session.transport,
                session.shortname,
                format_duration(session.duration_seconds),
                session.username,
                session.ip_address,
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &["Session", "User ID", "Region", "Transport", "Shortname", "Duration", "Username", "IP"],
        &rows,
    );
    Ok(())
}

async fn session_kick(args: AdminSessionKickArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let (region, local_id) = parse_session_ref(&args.session_id)?;
    let (_, region_urls) = api.discover().await?;
    let base_url = region_urls
        .get(&region)
        .ok_or_else(|| anyhow::anyhow!("unknown region '{region}'"))?;
    api.post_empty(
        base_url,
        "/control/admin/session/kick",
        &KickSessionRequest {
            local_session_id: local_id,
        },
    )
    .await?;
    println!("Kicked session {}", args.session_id);
    Ok(())
}

async fn session_spy(args: AdminSessionSpyArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let (region, local_id) = parse_session_ref(&args.session_id)?;
    let (_, region_urls) = api.discover().await?;
    let base_url = region_urls
        .get(&region)
        .ok_or_else(|| anyhow::anyhow!("unknown region '{region}'"))?;
    let session = load_session_summary(&api, base_url, local_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("unknown session '{}'", args.session_id))?;
    let show_input = !args.hide_input;
    let mut status_bar = SpyStatusBar::new(session, args.rw, show_input);
    let mut input_overlay = SpyInputOverlay::default();
    let ws_url = websocket_url(
        base_url,
        &format!("/control/admin/session/spy/{local_id}"),
        Some(&format!("rw={}&show_input=true", args.rw)),
    )?;

    let mut request = ws_url.into_client_request()?;
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", api.profile.password))?,
    );
    let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();

    let mut stdout = tokio::io::stdout();
    crossterm::terminal::enable_raw_mode()?;
    crossterm::execute!(
        std::io::stdout(),
        crossterm::terminal::EnterAlternateScreen,
        crossterm::cursor::Hide
    )?;

    let result = async {
        let mut stdin = tokio::io::stdin();
        let mut stdin_buf = [0u8; 4096];
        let mut vt: Option<avt::Vt> = None;
        let mut mouse_modes = std::collections::BTreeSet::new();
        let mut local_mouse_capture = false;
        let mut local_size = crossterm::terminal::size()?;
        let mut exit_message: Option<String> = None;
        let mut resize_tick = tokio::time::interval(std::time::Duration::from_millis(100));
        let mut status_tick = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut overlay_tick = tokio::time::interval(std::time::Duration::from_millis(120));
        loop {
            tokio::select! {
                message = read.next() => {
                    let Some(message) = message else {
                        if exit_message.is_none() {
                            exit_message = Some("Spy connection closed.".to_string());
                        }
                        break;
                    };
                    match message {
                        Ok(message) => match message {
                        Message::Binary(data) => {
                            let vt = vt.as_mut().ok_or_else(|| anyhow::anyhow!("missing spy init frame"))?;
                            vt.feed_str(&String::from_utf8_lossy(&data));
                            if args.rw {
                                sync_mouse_capture(
                                    scan_mouse_modes(&String::from_utf8_lossy(&data), &mut mouse_modes),
                                    &mut local_mouse_capture,
                                )?;
                            }
                            render_spy_view(vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                        }
                        Message::Text(text) => {
                            match serde_json::from_str::<SpyControlMessage>(&text)? {
                                SpyControlMessage::Init { cols, rows, dump } => {
                                    let mut next_vt = avt::Vt::new((cols as usize).max(1), (rows as usize).max(1));
                                    next_vt.feed_str(&dump);
                                    if args.rw {
                                        mouse_modes.clear();
                                        sync_mouse_capture(
                                            scan_mouse_modes(&dump, &mut mouse_modes),
                                            &mut local_mouse_capture,
                                        )?;
                                    }
                                    render_spy_view(&next_vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                                    vt = Some(next_vt);
                                }
                                SpyControlMessage::Resize { cols, rows } => {
                                    let vt = vt.as_mut().ok_or_else(|| anyhow::anyhow!("missing spy init frame"))?;
                                    vt.resize((cols as usize).max(1), (rows as usize).max(1));
                                    render_spy_view(vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                                }
                                SpyControlMessage::Metadata { username } => {
                                    status_bar.set_username(username);
                                    if let Some(vt) = vt.as_ref() {
                                        render_spy_view(vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                                    }
                                }
                                SpyControlMessage::Input { data } => {
                                    input_overlay.ingest(&data);
                                    if status_bar.show_input_overlay() {
                                        if let Some(vt) = vt.as_ref() {
                                            render_spy_view(vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                                        }
                                    }
                                }
                                SpyControlMessage::Closed { reason_slug: _, message } => {
                                    exit_message = Some(format!("Spy ended: {message}"));
                                    break;
                                }
                            }
                        }
                        Message::Close(frame) => {
                            exit_message = Some(match frame {
                                Some(frame) if !frame.reason.is_empty() => {
                                    format!("Spy ended: {}", frame.reason)
                                }
                                _ => "Spy connection closed.".to_string(),
                            });
                            break;
                        }
                        _ => {}
                        },
                        Err(error) => {
                            exit_message = Some(format!(
                                "Spy connection error: {}",
                                format_websocket_error(&error)
                            ));
                            break;
                        }
                    }
                }
                read_len = stdin.read(&mut stdin_buf) => {
                    let read_len = read_len?;
                    if read_len == 0 {
                        break;
                    }
                    let data = &stdin_buf[..read_len];
                    let Some(forwarded) = apply_spy_local_shortcuts(
                        data,
                        &mut status_bar,
                        vt.as_ref(),
                        local_size,
                        &input_overlay,
                        &mut stdout,
                    ).await? else {
                        break;
                    };
                    if args.rw {
                        if !forwarded.is_empty() {
                            write.send(Message::Binary(forwarded.into())).await?;
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    break;
                }
                _ = resize_tick.tick() => {
                    let next_size = crossterm::terminal::size()?;
                    if next_size != local_size {
                        local_size = next_size;
                        if let Some(vt) = vt.as_ref() {
                            render_spy_view(vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                        }
                    }
                }
                _ = status_tick.tick() => {
                    if let Some(vt) = vt.as_ref() {
                        render_spy_view(vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                    }
                }
                _ = overlay_tick.tick() => {
                    if input_overlay.tick() {
                        if status_bar.show_input_overlay() {
                            if let Some(vt) = vt.as_ref() {
                                render_spy_view(vt, local_size, &status_bar, &input_overlay, &mut stdout).await?;
                            }
                        }
                    }
                }
            }
        }
        Result::<Option<String>>::Ok(exit_message)
    }
    .await;

    let _ = crossterm::execute!(
        std::io::stdout(),
        crossterm::event::DisableMouseCapture,
        crossterm::cursor::Show,
        crossterm::terminal::LeaveAlternateScreen
    );
    let _ = crossterm::terminal::disable_raw_mode();
    match result {
        Ok(Some(message)) => {
            eprintln!("{message}");
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn author(command: AdminAuthorCommand, profile: Option<String>) -> Result<()> {
    match command {
        AdminAuthorCommand::Create(args) => author_create(args, profile).await,
        AdminAuthorCommand::List => author_list(profile).await,
        AdminAuthorCommand::Delete(args) => author_delete(args, profile).await,
    }
}

async fn author_create(args: AdminAuthorCreateArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let response: CreateAuthorResponse = api
        .post_json(
            &api.profile.url,
            "/control/admin/author/create",
            &CreateAuthorRequest {
                shortname: args.shortname,
                base_url: api.profile.url.clone(),
            },
        )
        .await?;
    println!("Author ID: {}", response.author.author_id);
    println!("Shortname: {}", response.author.shortname);
    println!("Token: {}", response.token);
    Ok(())
}

async fn author_list(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let authors: Vec<AuthorSummary> = api
        .get_json(&api.profile.url, "/control/admin/author/list")
        .await?;
    let rows = authors
        .into_iter()
        .map(|author| {
            vec![
                author.author_id.to_string(),
                if author.author_name.trim().is_empty() {
                    "-".to_string()
                } else {
                    author.author_name
                },
                author.shortname,
                format_seconds(author.playtime_seconds),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&["Author ID", "Author", "Shortname", "Playtime"], &rows);
    Ok(())
}

async fn author_delete(args: AdminAuthorDeleteArgs, profile: Option<String>) -> Result<()> {
    if !args.force
        && !Confirm::new()
            .with_prompt(format!(
                "Delete author {} permanently? This cannot be undone.",
                args.author_id
            ))
            .default(false)
            .interact()?
    {
        return Ok(());
    }
    let api = load_api(profile.as_deref())?;
    let deleted: DeleteShortnameResponse = api
        .post_json(
            &api.profile.url,
            "/control/admin/author/delete",
            &DeleteAuthorRequest {
                author_id: args.author_id,
            },
        )
        .await?;
    let (_, region_urls) = api.discover().await?;
    let invalidate_body = serde_json::json!({ "shortname": deleted.shortname });
    let futures = region_urls.values().map(|base_url| {
        api.post_empty(
            base_url,
            "/control/admin/cache/invalidate",
            &invalidate_body,
        )
    });
    for result in join_all(futures).await {
        result?;
    }
    println!("Deleted author {}", args.author_id);
    Ok(())
}

fn load_api(profile_override: Option<&str>) -> Result<AdminApi> {
    let (profile_name, profile) = resolve_admin_profile(profile_override)?;
    AdminApi::new(profile_name, profile)
}

fn completion_runtime() -> Option<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()
}

async fn load_session_summary(api: &AdminApi, base_url: &str, local_id: u64) -> Result<Option<SessionSummary>> {
    let sessions = api
        .get_json::<Vec<SessionSummary>>(base_url, "/control/admin/sessions")
        .await?;
    Ok(sessions
        .into_iter()
        .find(|session| session.local_session_id == local_id))
}

fn parse_session_ref(value: &str) -> Result<(String, u64)> {
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

fn websocket_url(base_url: &str, path: &str, query: Option<&str>) -> Result<String> {
    let mut url = reqwest::Url::parse(base_url)?;
    url.set_scheme(match url.scheme() {
        "https" => "wss",
        _ => "ws",
    })
    .map_err(|_| anyhow::anyhow!("invalid websocket scheme"))?;
    url.set_path(path);
    url.set_query(query);
    Ok(url.to_string())
}

async fn render_spy_view(
    vt: &avt::Vt,
    local_size: (u16, u16),
    status_bar: &SpyStatusBar,
    input_overlay: &SpyInputOverlay,
    stdout: &mut tokio::io::Stdout,
) -> Result<()> {
    let width = local_size.0 as usize;
    let height = local_size.1 as usize;
    let content_height = height.saturating_sub(1);
    let lines = vt.view().collect::<Vec<_>>();
    let app_size = vt.size();
    let default_pen = Pen::default();
    let mut current_pen = default_pen;
    let mut frame = String::from("\x1b[H");
    for row in 0..content_height {
        frame.push_str(&format!("\x1b[{};1H", row + 1));
        let line = lines.get(row).copied();
        let mut col = 0usize;
        while col < width {
            if row >= app_size.1 || col >= app_size.0 {
                push_spy_margin_fill(&mut frame, status_bar, row, col);
                current_pen = default_pen;
                col += 1;
                continue;
            }
            let Some(line) = line else {
                apply_pen(&mut frame, &mut current_pen, default_pen);
                frame.push(' ');
                col += 1;
                continue;
            };
            if col >= line.len() {
                apply_pen(&mut frame, &mut current_pen, default_pen);
                frame.push(' ');
                col += 1;
                continue;
            }
            let cell = &line[col];
            if cell.width() == 0 {
                col += 1;
                continue;
            }
            if cell.width() == 2 && col + 1 >= width {
                apply_pen(&mut frame, &mut current_pen, *cell.pen());
                frame.push(' ');
                col += 1;
                continue;
            }
            apply_pen(&mut frame, &mut current_pen, *cell.pen());
            frame.push(cell.char());
            col += cell.width().max(1);
        }
        apply_pen(&mut frame, &mut current_pen, default_pen);
        frame.push_str("\x1b[K");
    }

    if height > 0 {
        frame.push_str(&format!("\x1b[{};1H", height));
        frame.push_str(&status_bar.render(width, vt.size()));
    }
    if status_bar.show_input_overlay() {
        render_spy_input_overlay(&mut frame, input_overlay, status_bar, width, content_height, app_size);
    }

    let cursor = vt.cursor();
    if cursor.visible && cursor.row < content_height && cursor.col < width {
        frame.push_str(&format!("\x1b[{};{}H\x1b[?25h", cursor.row + 1, cursor.col + 1));
    } else {
        frame.push_str("\x1b[?25l");
    }
    stdout.write_all(frame.as_bytes()).await?;
    stdout.flush().await?;
    Ok(())
}

fn push_spy_margin_fill(frame: &mut String, status_bar: &SpyStatusBar, _row: usize, _col: usize) {
    let (fg, bg) = status_bar.spy_margin_colors();
    frame.push_str(&format!(
        "\x1b[{};{}m{}\x1b[0m",
        palette::render_color_code(fg, false),
        palette::render_color_code(bg, true),
        '/',
    ));
}

fn render_spy_input_overlay(
    frame: &mut String,
    input_overlay: &SpyInputOverlay,
    status_bar: &SpyStatusBar,
    width: usize,
    content_height: usize,
    app_size: (usize, usize),
) {
    if content_height == 0 {
        return;
    }
    if let Some((col, row)) = input_overlay.mouse_position() {
        if col < width && col < app_size.0 && row < content_height && row < app_size.1 {
            frame.push_str(&format!(
                "\x1b[{};{}H\x1b[38;2;255;0;0m•\x1b[0m",
                row + 1,
                col + 1
            ));
        }
    }
    let items = input_overlay.visible_items(status_bar.profile);
    if items.is_empty() || width == 0 {
        return;
    }
    let total_width = items.iter().map(|item| item.width).sum::<usize>()
        + items.len().saturating_sub(1);
    let start_col = width.saturating_sub(total_width) / 2 + 1;
    let overlay_row = content_height.saturating_sub(1).max(1);
    frame.push_str(&format!("\x1b[{};{}H", overlay_row, start_col));
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            frame.push(' ');
        }
        frame.push_str(&item.ansi);
    }
    frame.push_str("\x1b[0m");
}

fn apply_pen(frame: &mut String, current_pen: &mut Pen, next_pen: Pen) {
    if *current_pen == next_pen {
        return;
    }
    frame.push_str("\x1b[0");
    if let Some(color) = next_pen.foreground() {
        append_color_param(frame, color, 30);
    }
    if let Some(color) = next_pen.background() {
        append_color_param(frame, color, 40);
    }
    if next_pen.is_bold() {
        frame.push_str(";1");
    } else if next_pen.is_faint() {
        frame.push_str(";2");
    }
    if next_pen.is_italic() {
        frame.push_str(";3");
    }
    if next_pen.is_underline() {
        frame.push_str(";4");
    }
    if next_pen.is_blink() {
        frame.push_str(";5");
    }
    if next_pen.is_inverse() {
        frame.push_str(";7");
    }
    if next_pen.is_strikethrough() {
        frame.push_str(";9");
    }
    frame.push('m');
    *current_pen = next_pen;
}

fn append_color_param(frame: &mut String, color: Color, base: u8) {
    match color {
        Color::Indexed(index) if index < 8 => {
            frame.push(';');
            frame.push_str(&(base + index).to_string());
        }
        Color::Indexed(index) if index < 16 => {
            frame.push(';');
            frame.push_str(&(base + 52 + index).to_string());
        }
        Color::Indexed(index) => {
            frame.push_str(&format!(";{};5;{}", base + 8, index));
        }
        Color::RGB(rgb) => {
            frame.push_str(&format!(";{};2;{};{};{}", base + 8, rgb.r, rgb.g, rgb.b));
        }
    }
}

fn scan_mouse_modes(text: &str, mouse_modes: &mut std::collections::BTreeSet<u16>) -> bool {
    let bytes = text.as_bytes();
    let mut idx = 0usize;
    while idx + 3 < bytes.len() {
        if bytes[idx] != 0x1b || bytes[idx + 1] != b'[' || bytes[idx + 2] != b'?' {
            idx += 1;
            continue;
        }
        let mut end = idx + 3;
        while end < bytes.len() && !matches!(bytes[end], b'h' | b'l') {
            end += 1;
        }
        if end >= bytes.len() {
            break;
        }
        let Ok(params) = std::str::from_utf8(&bytes[idx + 3..end]) else {
            idx = end + 1;
            continue;
        };
        let enable = bytes[end] == b'h';
        for value in params.split(';').filter_map(|value| value.parse::<u16>().ok()) {
            if !matches!(value, 1000 | 1002 | 1003 | 1005 | 1006 | 1015) {
                continue;
            }
            if enable {
                mouse_modes.insert(value);
            } else {
                mouse_modes.remove(&value);
            }
        }
        idx = end + 1;
    }
    !mouse_modes.is_empty()
}

fn sync_mouse_capture(enabled: bool, local_mouse_capture: &mut bool) -> Result<()> {
    if enabled == *local_mouse_capture {
        return Ok(());
    }
    if enabled {
        crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture)?;
    } else {
        crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture)?;
    }
    *local_mouse_capture = enabled;
    Ok(())
}

fn format_websocket_error(error: &tokio_tungstenite::tungstenite::Error) -> String {
    match error {
        tokio_tungstenite::tungstenite::Error::Protocol(
            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => "connection reset without a closing handshake".to_string(),
        tokio_tungstenite::tungstenite::Error::ConnectionClosed => {
            "connection closed".to_string()
        }
        _ => error.to_string(),
    }
}

async fn apply_spy_local_shortcuts(
    data: &[u8],
    status_bar: &mut SpyStatusBar,
    vt: Option<&avt::Vt>,
    local_size: (u16, u16),
    input_overlay: &SpyInputOverlay,
    stdout: &mut tokio::io::Stdout,
) -> Result<Option<Vec<u8>>> {
    let mut forwarded = Vec::with_capacity(data.len());
    let mut toggled = false;
    for &byte in data {
        match byte {
            0x03 => return Ok(None),
            0x14 => {
                status_bar.toggle_input_overlay();
                toggled = true;
            }
            _ => forwarded.push(byte),
        }
    }
    if toggled {
        if let Some(vt) = vt {
            render_spy_view(vt, local_size, status_bar, input_overlay, stdout).await?;
        }
    }
    Ok(Some(forwarded))
}

struct SpyStatusBar {
    session: SessionSummary,
    read_write: bool,
    show_input_overlay: bool,
    attached_at: std::time::Instant,
    profile: TerminalProfile,
}

impl SpyStatusBar {
    fn new(session: SessionSummary, read_write: bool, show_input_overlay: bool) -> Self {
        Self {
            session,
            read_write,
            show_input_overlay,
            attached_at: std::time::Instant::now(),
            profile: TerminalProfile::from_term(
                std::env::var("TERM").ok().as_deref(),
                std::env::var("COLORTERM").ok().as_deref(),
            ),
        }
    }

    fn render(&self, width: usize, app_size: (usize, usize)) -> String {
        if width == 0 {
            return String::new();
        }
        let pink = match self.profile.color_mode {
            terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0xec, 0x48, 0x99),
            terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(205),
            terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(13),
        };
        let surface = match self.profile.has_dark_background() {
            true => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0x26, 0x26, 0x26),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(235),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(0),
            },
            false => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0xf5, 0xf5, 0xf5),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(255),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(7),
            },
        };
        let text = match self.profile.has_dark_background() {
            true => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0xf5, 0xf5, 0xf5),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(255),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(15),
            },
            false => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0x26, 0x26, 0x26),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(235),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(0),
            },
        };
        let on_pink = match self.profile.color_mode {
            terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0xff, 0xf7, 0xfb),
            terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(255),
            terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(15),
        };
        let muted = match self.profile.has_dark_background() {
            true => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0xa3, 0xa3, 0xa3),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(248),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(8),
            },
            false => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0x73, 0x73, 0x73),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(243),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(8),
            },
        };
        let duration = self.session.duration_seconds + self.attached_at.elapsed().as_secs();
        let label = format!(" spy {} ", self.session.session_id);
        let label_width = visible_width(&label);
        if width <= label_width {
            return format!(
                "\x1b[{};{}m{}\x1b[K\x1b[0m",
                palette::render_color_code(on_pink, false),
                palette::render_color_code(pink, true),
                label.chars().take(width).collect::<String>(),
            );
        }
        let gap = " ";
        let remaining_width = width - label_width;
        let input_hint_value = if self.show_input_overlay {
            "hide input"
        } else {
            "show input"
        };
        let hint_plain = format!("ctrl+t: {input_hint_value} ctrl+c: exit");
        let hint = StatusField {
            width: visible_width(&hint_plain),
            ansi: format!(
                "\x1b[{}mctrl+t:\x1b[{}m {} \x1b[{}mctrl+c:\x1b[{}m exit",
                palette::render_color_code(muted, false),
                palette::render_color_code(text, false),
                input_hint_value,
                palette::render_color_code(muted, false),
                palette::render_color_code(text, false),
            ),
        };
        let hint_total_width = hint.width + 1;
        let reserve_hint = remaining_width > hint_total_width + 24;
        let left_budget = if reserve_hint {
            remaining_width.saturating_sub(hint_total_width)
        } else {
            remaining_width
        };
        let fields = [
            status_field(
                muted,
                text,
                "mode",
                if self.read_write { "rw" } else { "readonly" },
            ),
            status_field(
                muted,
                text,
                "uid",
                &self
                    .session
                    .user_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
            status_field(muted, text, "transport", &self.session.transport),
            status_field(muted, text, "duration", &format_duration(duration)),
            status_field(muted, text, "size", &format!("{}x{}", app_size.0, app_size.1)),
            status_field(muted, text, "user", &self.session.username),
            status_field(muted, text, "region", &self.session.region_id),
            status_field(muted, text, "ip", &self.session.ip_address),
        ];
        let mut left = String::new();
        let mut used = 0usize;
        let leading_padding = " ";
        if left_budget > 0 {
            left.push_str(leading_padding);
            used += visible_width(leading_padding);
        }
        for (index, field) in fields.into_iter().enumerate() {
            let gap_width = if index == 0 { 0 } else { visible_width(gap) };
            if used + gap_width + field.width > left_budget {
                break;
            }
            if index > 0 {
                left.push_str(gap);
                used += gap_width;
            }
            left.push_str(&field.ansi);
            used += field.width;
        }
        let mut right = String::new();
        let padding = if reserve_hint {
            let middle = remaining_width.saturating_sub(used + hint_total_width);
            right.push_str(&hint.ansi);
            right.push(' ');
            " ".repeat(middle)
        } else {
            " ".repeat(remaining_width.saturating_sub(used))
        };
        format!(
            "\x1b[{};{}m{}\x1b[{};{}m{}\x1b[K\x1b[0m",
            palette::render_color_code(on_pink, false),
            palette::render_color_code(pink, true),
            label,
            palette::render_color_code(text, false),
            palette::render_color_code(surface, true),
            format!("{left}{padding}{right}"),
        )
    }

    fn set_username(&mut self, username: String) {
        self.session.username = username;
    }

    fn toggle_input_overlay(&mut self) {
        self.show_input_overlay = !self.show_input_overlay;
    }

    fn show_input_overlay(&self) -> bool {
        self.show_input_overlay
    }

    fn spy_margin_colors(&self) -> (PaletteColor, PaletteColor) {
        let muted = match self.profile.has_dark_background() {
            true => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0xa3, 0xa3, 0xa3),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(248),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(8),
            },
            false => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0x73, 0x73, 0x73),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(243),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(8),
            },
        };
        let surface = match self.profile.has_dark_background() {
            true => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0x26, 0x26, 0x26),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(235),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(0),
            },
            false => match self.profile.color_mode {
                terminal_games::terminal_profile::TerminalColorMode::TrueColor => PaletteColor::Rgb(0xf5, 0xf5, 0xf5),
                terminal_games::terminal_profile::TerminalColorMode::Color256 => PaletteColor::Fixed(255),
                terminal_games::terminal_profile::TerminalColorMode::Color16 => PaletteColor::Basic(7),
            },
        };
        (muted, surface)
    }
}

#[derive(Default)]
struct SpyInputOverlay {
    items: VecDeque<SpyInputItem>,
    mouse: Option<SpyMousePointer>,
}

struct SpyInputItem {
    label: String,
    created_at: Instant,
}

struct SpyMousePointer {
    col: usize,
    row: usize,
    seen_at: Instant,
}

struct RenderedOverlayItem {
    ansi: String,
    width: usize,
}

impl SpyInputOverlay {
    fn ingest(&mut self, data: &[u8]) {
        let Ok(Some(event)) = TerminputEvent::parse_from(data) else {
            return;
        };
        let now = Instant::now();
        match event {
            TerminputEvent::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
                self.push_item(format_key_event(key), now);
            }
            TerminputEvent::Mouse(mouse) => {
                self.mouse = Some(SpyMousePointer {
                    col: mouse.column as usize,
                    row: mouse.row as usize,
                    seen_at: now,
                });
                if let Some(label) = format_mouse_event(mouse) {
                    self.push_item(label, now);
                }
            }
            TerminputEvent::Paste(text) => {
                self.push_item(format!("paste {}", text.chars().count()), now);
            }
            _ => {}
        }
    }

    fn tick(&mut self) -> bool {
        let before_len = self.items.len();
        let had_mouse = self.mouse.is_some();
        let now = Instant::now();
        self.items
            .retain(|item| now.duration_since(item.created_at) <= std::time::Duration::from_secs(4));
        if self
            .mouse
            .as_ref()
            .is_some_and(|mouse| now.duration_since(mouse.seen_at) > std::time::Duration::from_secs(2))
        {
            self.mouse = None;
        }
        before_len != self.items.len() || had_mouse != self.mouse.is_some()
    }

    fn visible_items(&self, profile: TerminalProfile) -> Vec<RenderedOverlayItem> {
        let now = Instant::now();
        self.items
            .iter()
            .filter_map(|item| {
                let age = now.duration_since(item.created_at);
                let step = (age.as_millis() / 900) as usize;
                let color = overlay_fade_color(profile, step)?;
                Some(RenderedOverlayItem {
                    width: visible_width(&item.label),
                    ansi: format!(
                        "\x1b[{}m{}\x1b[0m",
                        palette::render_color_code(color, false),
                        item.label
                    ),
                })
            })
            .collect()
    }

    fn mouse_position(&self) -> Option<(usize, usize)> {
        self.mouse.as_ref().map(|mouse| (mouse.col, mouse.row))
    }

    fn push_item(&mut self, label: String, created_at: Instant) {
        self.items.push_back(SpyInputItem { label, created_at });
        while self.items.len() > 6 {
            self.items.pop_front();
        }
    }
}

fn overlay_fade_color(profile: TerminalProfile, step: usize) -> Option<PaletteColor> {
    let dark = profile.has_dark_background();
    Some(match profile.color_mode {
        terminal_games::terminal_profile::TerminalColorMode::TrueColor => match (dark, step.min(4)) {
            (true, 0) => PaletteColor::Rgb(0xf5, 0xf5, 0xf5),
            (true, 1) => PaletteColor::Rgb(0xd4, 0xd4, 0xd4),
            (true, 2) => PaletteColor::Rgb(0xa3, 0xa3, 0xa3),
            (true, 3) => PaletteColor::Rgb(0x73, 0x73, 0x73),
            (true, _) => return None,
            (false, 0) => PaletteColor::Rgb(0x26, 0x26, 0x26),
            (false, 1) => PaletteColor::Rgb(0x52, 0x52, 0x52),
            (false, 2) => PaletteColor::Rgb(0x73, 0x73, 0x73),
            (false, 3) => PaletteColor::Rgb(0xa3, 0xa3, 0xa3),
            (false, _) => return None,
        },
        terminal_games::terminal_profile::TerminalColorMode::Color256 => match (dark, step.min(4)) {
            (true, 0) => PaletteColor::Fixed(255),
            (true, 1) => PaletteColor::Fixed(252),
            (true, 2) => PaletteColor::Fixed(248),
            (true, 3) => PaletteColor::Fixed(243),
            (true, _) => return None,
            (false, 0) => PaletteColor::Fixed(235),
            (false, 1) => PaletteColor::Fixed(240),
            (false, 2) => PaletteColor::Fixed(243),
            (false, 3) => PaletteColor::Fixed(248),
            (false, _) => return None,
        },
        terminal_games::terminal_profile::TerminalColorMode::Color16 => match step.min(4) {
            0 => PaletteColor::Basic(if dark { 15 } else { 0 }),
            1 => PaletteColor::Basic(7),
            2 => PaletteColor::Basic(8),
            3 => PaletteColor::Basic(8),
            _ => return None,
        },
    })
}

fn format_key_event(event: KeyEvent) -> String {
    let mut parts = Vec::new();
    if event.modifiers.contains(KeyModifiers::CTRL) {
        parts.push("ctrl".to_string());
    }
    if event.modifiers.contains(KeyModifiers::ALT) {
        parts.push("alt".to_string());
    }
    if event.modifiers.contains(KeyModifiers::SHIFT) {
        parts.push("shift".to_string());
    }
    if event.modifiers.contains(KeyModifiers::SUPER) {
        parts.push("super".to_string());
    }
    parts.push(match event.code {
        KeyCode::Backspace => "backspace".to_string(),
        KeyCode::Enter => "enter".to_string(),
        KeyCode::Left => "left".to_string(),
        KeyCode::Right => "right".to_string(),
        KeyCode::Up => "up".to_string(),
        KeyCode::Down => "down".to_string(),
        KeyCode::Home => "home".to_string(),
        KeyCode::End => "end".to_string(),
        KeyCode::PageUp => "pageup".to_string(),
        KeyCode::PageDown => "pagedown".to_string(),
        KeyCode::Tab => "tab".to_string(),
        KeyCode::Delete => "delete".to_string(),
        KeyCode::Insert => "insert".to_string(),
        KeyCode::F(n) => format!("f{n}"),
        KeyCode::Char(ch) => ch.to_string(),
        KeyCode::Esc => "esc".to_string(),
        KeyCode::CapsLock => "caps".to_string(),
        KeyCode::ScrollLock => "scroll".to_string(),
        KeyCode::NumLock => "numlock".to_string(),
        KeyCode::PrintScreen => "print".to_string(),
        KeyCode::Pause => "pause".to_string(),
        KeyCode::Menu => "menu".to_string(),
        KeyCode::KeypadBegin => "kpbegin".to_string(),
        KeyCode::Media(_) => "media".to_string(),
        KeyCode::Modifier(_, _) => "modifier".to_string(),
    });
    parts.join("+")
}

fn format_mouse_event(event: MouseEvent) -> Option<String> {
    Some(match event.kind {
        MouseEventKind::Down(button) => format!("{} click", format_mouse_button(button)),
        MouseEventKind::Up(button) => format!("{} release", format_mouse_button(button)),
        MouseEventKind::Drag(button) => format!("{} drag", format_mouse_button(button)),
        MouseEventKind::Moved => return None,
        MouseEventKind::Scroll(direction) => format!("wheel {}", format_scroll_direction(direction)),
    })
}

fn format_mouse_button(button: MouseButton) -> &'static str {
    match button {
        MouseButton::Left => "left",
        MouseButton::Right => "right",
        MouseButton::Middle => "middle",
        MouseButton::Unknown => "mouse",
    }
}

fn format_scroll_direction(direction: ScrollDirection) -> &'static str {
    match direction {
        ScrollDirection::Up => "up",
        ScrollDirection::Down => "down",
        ScrollDirection::Left => "left",
        ScrollDirection::Right => "right",
    }
}

struct StatusField {
    ansi: String,
    width: usize,
}

fn status_field(
    muted: PaletteColor,
    text: PaletteColor,
    label: &str,
    value: &str,
) -> StatusField {
    let plain = format!("{label}: {value}");
    StatusField {
        width: visible_width(&plain),
        ansi: format!(
        "\x1b[{}m{}:\x1b[{}m {}",
        palette::render_color_code(muted, false),
        label,
        palette::render_color_code(text, false),
        value,
        ),
    }
}

fn visible_width(text: &str) -> usize {
    text.chars().count()
}
