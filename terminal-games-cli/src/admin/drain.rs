// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::BTreeMap,
    io::{self, Write},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use futures::future::join_all;
use terminal_games::control::{
    DrainStartRequest, NodeRuntimeStatus, ShutdownPhase, parse_duration_string, parse_utc_timestamp,
};
use unicode_width::UnicodeWidthStr;

use super::{
    AdminNodesDrainAttachArgs, AdminNodesDrainCancelArgs, AdminNodesDrainCommand,
    AdminNodesDrainStartArgs, load_api, parse_nodes_arg,
};
use crate::config::{format_bytes_per_second, format_duration};

enum HoldDrainOutcome {
    Completed,
    Detached,
}

pub(super) async fn run(command: AdminNodesDrainCommand, profile: Option<String>) -> Result<()> {
    match command {
        AdminNodesDrainCommand::Start(args) => start(args, profile).await,
        AdminNodesDrainCommand::Attach(args) => attach(args, profile).await,
        AdminNodesDrainCommand::Cancel(args) => cancel(args, profile).await,
    }
}

async fn start(args: AdminNodesDrainStartArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let (_, all_node_urls) = api.discover().await?;
    let node_urls = select_node_urls(&all_node_urls, parse_nodes_arg(args.nodes.clone()))?;
    if node_urls.is_empty() {
        anyhow::bail!("no nodes discovered");
    }

    let duration = resolve_drain_duration(&args)?;
    let duration_seconds = duration.as_secs();
    let request = DrainStartRequest { duration_seconds };

    let started = match start_drain_on_all_nodes(&api, &node_urls, request).await {
        Ok(started) => started,
        Err(error) => {
            let _ = cancel_drain_on_nodes(&api, &node_urls).await;
            return Err(error);
        }
    };
    if args.detach {
        println!(
            "Scheduled drain on {} node(s) for {} and detached.",
            started.node_urls.len(),
            format_duration(duration_seconds)
        );
        return Ok(());
    }

    let attached_result = hold_drain(&api, &started.node_urls, started.deadline_unix_ms).await;
    if attached_result.is_err() {
        let _ = cancel_drain_on_nodes(&api, &started.node_urls).await;
    }
    attached_result.map(|_| ())
}

async fn attach(args: AdminNodesDrainAttachArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let (_, all_node_urls) = api.discover().await?;
    let requested_nodes = parse_nodes_arg(args.nodes);
    let attached = select_attached_node_urls(&api, &all_node_urls, requested_nodes).await?;
    hold_drain(&api, &attached.node_urls, attached.deadline_unix_ms)
        .await
        .map(|_| ())
}

async fn cancel(args: AdminNodesDrainCancelArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let (_, all_node_urls) = api.discover().await?;
    let node_urls = select_node_urls(&all_node_urls, parse_nodes_arg(args.nodes))?;
    let cancelled = cancel_drain_on_nodes(&api, &node_urls).await?;
    println!("Cancelled drain on {} node(s).", cancelled);
    Ok(())
}

struct SelectedDrain {
    node_urls: BTreeMap<String, String>,
    deadline_unix_ms: i64,
}

async fn start_drain_on_all_nodes(
    api: &crate::control_client::AdminClient,
    node_urls: &BTreeMap<String, String>,
    request: DrainStartRequest,
) -> Result<SelectedDrain> {
    let mut started = BTreeMap::new();
    let mut deadline_unix_ms: Option<i64> = None;
    for (node_id, base_url) in node_urls {
        let status = api
            .drain_start_at(base_url, request.clone())
            .await
            .with_context(|| format!("failed to start drain on node '{node_id}'"))?;
        deadline_unix_ms = Some(
            deadline_unix_ms
                .unwrap_or_default()
                .max(status.deadline_unix_ms.unwrap_or_default()),
        );
        started.insert(node_id.clone(), base_url.clone());
    }
    Ok(SelectedDrain {
        node_urls: started,
        deadline_unix_ms: deadline_unix_ms.unwrap_or_else(current_unix_ms),
    })
}

async fn cancel_drain_on_nodes(
    api: &crate::control_client::AdminClient,
    node_urls: &BTreeMap<String, String>,
) -> Result<usize> {
    let futures = node_urls.iter().map(|(node_id, base_url)| async move {
        api.drain_cancel_at(base_url)
            .await
            .with_context(|| format!("failed to cancel drain on node '{node_id}'"))
    });
    let mut cancelled = 0usize;
    for result in join_all(futures).await {
        result?;
        cancelled += 1;
    }
    Ok(cancelled)
}

async fn hold_drain(
    api: &crate::control_client::AdminClient,
    node_urls: &BTreeMap<String, String>,
    deadline_unix_ms: i64,
) -> Result<HoldDrainOutcome> {
    let mut view = InlineView::default();
    loop {
        let countdown = remaining_until(deadline_unix_ms);
        let shutdown_started = countdown.is_zero();
        let states = fetch_node_states(api, node_urls).await;
        if !shutdown_started && all_nodes_at_zero_sessions(&states) {
            view.finish()?;
            println!();
            println!("All sessions drained.");
            return Ok(HoldDrainOutcome::Completed);
        }
        render_drain_view(&mut view, countdown, &states, shutdown_started)?;

        if shutdown_started && all_nodes_at_zero_sessions(&states) {
            view.finish()?;
            println!();
            println!("Drain completed.");
            return Ok(HoldDrainOutcome::Completed);
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                view.finish()?;
                println!();
                println!("Detached from drain view.");
                return Ok(HoldDrainOutcome::Detached);
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn select_attached_node_urls(
    api: &crate::control_client::AdminClient,
    all_node_urls: &BTreeMap<String, String>,
    requested_nodes: Vec<String>,
) -> Result<SelectedDrain> {
    let explicit_selection = !requested_nodes.is_empty();
    let candidate_urls = select_node_urls(all_node_urls, requested_nodes)?;
    if candidate_urls.is_empty() {
        anyhow::bail!("no nodes discovered");
    }

    let states = fetch_node_states(api, &candidate_urls).await;
    let mut attached = BTreeMap::new();
    let mut deadline_unix_ms: Option<i64> = None;

    for state in states {
        match state {
            NodePollState::Live(status) if status.shutdown.phase == ShutdownPhase::Draining => {
                let base_url = candidate_urls
                    .get(&status.node_id)
                    .expect("selected node must have a URL");
                attached.insert(status.node_id.clone(), base_url.clone());
                deadline_unix_ms = Some(
                    deadline_unix_ms
                        .unwrap_or_default()
                        .max(status.shutdown.deadline_unix_ms.unwrap_or_default()),
                );
            }
            NodePollState::Live(status) if explicit_selection => {
                anyhow::bail!(
                    "node '{}' is not draining (current state: {})",
                    status.node_id,
                    shutdown_label(&status)
                );
            }
            NodePollState::Error { node_id, detail } if explicit_selection => {
                return Err(anyhow!("failed to inspect node '{node_id}': {detail}"));
            }
            NodePollState::Live(_) | NodePollState::Error { .. } => {}
        }
    }

    if attached.is_empty() {
        anyhow::bail!("no draining nodes found");
    }

    Ok(SelectedDrain {
        node_urls: attached,
        deadline_unix_ms: deadline_unix_ms.unwrap_or_else(current_unix_ms),
    })
}

async fn fetch_node_states(
    api: &crate::control_client::AdminClient,
    node_urls: &BTreeMap<String, String>,
) -> Vec<NodePollState> {
    let futures = node_urls.iter().map(|(node_id, base_url)| async move {
        match api.local_node_status_at(base_url).await {
            Ok(status) => NodePollState::Live(status),
            Err(error) => NodePollState::Error {
                node_id: node_id.clone(),
                detail: error.to_string(),
            },
        }
    });
    let mut states = join_all(futures).await;
    states.sort_by_key(|state| state.node_id().to_string());
    states
}

fn render_drain_view(
    view: &mut InlineView,
    countdown: Duration,
    states: &[NodePollState],
    shutdown_started: bool,
) -> Result<()> {
    let mut lines = vec![format!(
        "Drain countdown: {}",
        format_duration(countdown.as_secs())
    )];
    if shutdown_started {
        if countdown.is_zero() {
            lines.push(
                "Deadline reached. Gracefully closing any remaining active sessions.".to_string(),
            );
        } else {
            lines.push("All sessions drained early.".to_string());
        }
    } else {
        lines.push(
            "Ctrl-C detaches. Use `admin nodes drain cancel` to cancel the drain.".to_string(),
        );
    }
    lines.push(String::new());

    let rows = states.iter().map(render_row).collect::<Vec<_>>();
    lines.extend(render_table_lines(
        &[
            "Node",
            "State",
            "Sessions",
            "Capacity",
            "CPU",
            "Memory",
            "Bandwidth",
            "Detail",
        ],
        &rows,
    ));
    view.render(&lines)?;
    Ok(())
}

fn all_nodes_at_zero_sessions(states: &[NodePollState]) -> bool {
    states.iter().all(|state| match state {
        NodePollState::Live(status) => status.current_sessions == 0,
        NodePollState::Error { .. } => false,
    })
}

fn render_row(state: &NodePollState) -> Vec<String> {
    match state {
        NodePollState::Live(status) => vec![
            status.node_id.clone(),
            shutdown_label(status),
            status.current_sessions.to_string(),
            status.max_capacity.to_string(),
            format!("{:.1}%", status.cpu_usage_percent),
            format!(
                "{}/{} MiB",
                status.memory_used_bytes / 1024 / 1024,
                status.memory_total_bytes / 1024 / 1024
            ),
            format_bytes_per_second(status.bandwidth_bytes_per_second),
            shutdown_detail(status),
        ],
        NodePollState::Error { node_id, detail } => vec![
            node_id.clone(),
            "error".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            detail.clone(),
        ],
    }
}

fn shutdown_label(status: &NodeRuntimeStatus) -> String {
    match status.shutdown.phase {
        ShutdownPhase::Running => "running".to_string(),
        ShutdownPhase::Draining => "draining".to_string(),
        ShutdownPhase::ShuttingDown => "shutting_down".to_string(),
    }
}

fn shutdown_detail(status: &NodeRuntimeStatus) -> String {
    match status.shutdown.phase {
        ShutdownPhase::Running => "accepting new sessions".to_string(),
        ShutdownPhase::Draining => "new sessions disabled".to_string(),
        ShutdownPhase::ShuttingDown => "graceful shutdown active".to_string(),
    }
}

enum NodePollState {
    Live(NodeRuntimeStatus),
    Error { node_id: String, detail: String },
}

impl NodePollState {
    fn node_id(&self) -> &str {
        match self {
            Self::Live(status) => &status.node_id,
            Self::Error { node_id, .. } => node_id,
        }
    }
}

#[derive(Default)]
struct InlineView {
    rendered_lines: usize,
}

impl InlineView {
    fn render(&mut self, lines: &[String]) -> Result<()> {
        if self.rendered_lines > 0 {
            print!("\x1b[{}F\x1b[J", self.rendered_lines);
        }
        for line in lines {
            println!("{line}");
        }
        io::stdout().flush()?;
        self.rendered_lines = lines.len();
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        io::stdout().flush()?;
        self.rendered_lines = 0;
        Ok(())
    }
}

fn render_table_lines(headers: &[&str], rows: &[Vec<String>]) -> Vec<String> {
    let mut widths = headers
        .iter()
        .map(|header| UnicodeWidthStr::width(*header))
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            let width = UnicodeWidthStr::width(cell.as_str());
            if index >= widths.len() {
                widths.push(width);
            } else {
                widths[index] = widths[index].max(width);
            }
        }
    }

    let mut lines = Vec::with_capacity(rows.len() + 2);
    lines.push(join_padded(headers.iter().copied(), &widths));
    lines.push(
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("  "),
    );
    for row in rows {
        lines.push(join_padded(row.iter().map(String::as_str), &widths));
    }
    lines
}

fn join_padded<'a>(cells: impl Iterator<Item = &'a str>, widths: &[usize]) -> String {
    cells
        .enumerate()
        .map(|(index, cell)| {
            let width = UnicodeWidthStr::width(cell);
            format!("{cell}{}", " ".repeat(widths[index].saturating_sub(width)))
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn resolve_drain_duration(args: &AdminNodesDrainStartArgs) -> Result<Duration> {
    match (args.duration.as_deref(), args.ends_at.as_deref()) {
        (Some(duration), None) => {
            let duration = parse_duration_string(duration)?;
            let seconds = u64::try_from(duration.whole_seconds())
                .map_err(|_| anyhow!("duration must be positive"))?;
            Ok(Duration::from_secs(seconds))
        }
        (None, Some(ends_at)) => {
            let deadline_unix = parse_utc_timestamp(ends_at)?;
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or_default();
            let remaining = deadline_unix.saturating_sub(now_unix);
            anyhow::ensure!(
                remaining > 0,
                "--ends-at must be in the future and use RFC3339, for example 2026-04-04T18:30:00Z"
            );
            Ok(Duration::from_secs(remaining as u64))
        }
        (None, None) => Ok(Duration::from_secs(5 * 60)),
        (Some(_), Some(_)) => unreachable!("clap enforces conflicts"),
    }
}

fn remaining_until(deadline_unix_ms: i64) -> Duration {
    Duration::from_millis(
        deadline_unix_ms
            .saturating_sub(current_unix_ms())
            .max(0)
            .try_into()
            .unwrap_or_default(),
    )
}

fn current_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn select_node_urls(
    all_node_urls: &BTreeMap<String, String>,
    requested_nodes: Vec<String>,
) -> Result<BTreeMap<String, String>> {
    if requested_nodes.is_empty() {
        return Ok(all_node_urls.clone());
    }

    let mut selected = BTreeMap::new();
    for node in requested_nodes {
        let Some(base_url) = all_node_urls.get(&node) else {
            anyhow::bail!("unknown node '{node}'");
        };
        selected.insert(node, base_url.clone());
    }
    Ok(selected)
}
