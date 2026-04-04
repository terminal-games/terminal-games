// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Url;
use tokio::sync::mpsc;

use crate::cluster_kicked_ips::ClusterKickedIpReview;

#[derive(Clone)]
pub(crate) struct Notifications {
    tx: Option<mpsc::UnboundedSender<NotificationEvent>>,
}

#[derive(Clone, Debug)]
pub(crate) struct LongSessionNotification {
    pub(crate) session_id: String,
    pub(crate) app_shortname: String,
    pub(crate) duration: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct ClusterEnforcementNotification {
    pub(crate) node_id: String,
    pub(crate) current_sessions: usize,
    pub(crate) max_capacity: usize,
    pub(crate) suspicious_cluster_count: usize,
    pub(crate) max_cluster_score: f64,
    pub(crate) sessions: Vec<ClusterEnforcementSession>,
    pub(crate) ip_review: ClusterKickedIpReview,
}

#[derive(Clone, Debug)]
pub(crate) struct ClusterEnforcementSession {
    pub(crate) session_id: String,
    pub(crate) transport: String,
    pub(crate) app_shortname: String,
}

#[derive(Clone, Debug)]
pub(crate) struct CapacityThresholdNotification {
    pub(crate) node_id: String,
    pub(crate) current_sessions: usize,
    pub(crate) max_capacity: usize,
    pub(crate) threshold_percent: usize,
}

#[derive(Clone, Debug)]
enum NotificationEvent {
    LongSession(LongSessionNotification),
    ClusterEnforcement(ClusterEnforcementNotification),
    CapacityThreshold(CapacityThresholdNotification),
}

impl NotificationEvent {
    fn kind(&self) -> &'static str {
        match self {
            Self::LongSession(_) => "long_session",
            Self::ClusterEnforcement(_) => "cluster_enforcement",
            Self::CapacityThreshold(_) => "capacity_threshold",
        }
    }
}

impl Notifications {
    pub(crate) fn from_env() -> Self {
        let mut backends = Vec::new();
        if let Some(url) = load_discord_webhook_url() {
            backends.push(NotificationBackend::Discord(DiscordWebhookBackend::new(
                url,
            )));
        }
        if backends.is_empty() {
            return Self { tx: None };
        }

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(NotificationWorker { backends, rx }.run());
        Self { tx: Some(tx) }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.tx.is_some()
    }

    pub(crate) fn notify_long_session(&self, notification: LongSessionNotification) {
        self.send(NotificationEvent::LongSession(notification));
    }

    pub(crate) fn notify_cluster_enforcement(&self, notification: ClusterEnforcementNotification) {
        self.send(NotificationEvent::ClusterEnforcement(notification));
    }

    pub(crate) fn notify_capacity_threshold(&self, notification: CapacityThresholdNotification) {
        self.send(NotificationEvent::CapacityThreshold(notification));
    }

    fn send(&self, event: NotificationEvent) {
        let Some(tx) = &self.tx else {
            return;
        };
        if let Err(error) = tx.send(event) {
            tracing::warn!(?error, "failed to queue notification");
        }
    }
}

struct NotificationWorker {
    backends: Vec<NotificationBackend>,
    rx: mpsc::UnboundedReceiver<NotificationEvent>,
}

impl NotificationWorker {
    async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            for backend in &self.backends {
                if let Err(error) = backend.send(&event).await {
                    tracing::warn!(
                        error = ?error,
                        backend = backend.name(),
                        notification = event.kind(),
                        "failed to send notification"
                    );
                }
            }
        }
    }
}

enum NotificationBackend {
    Discord(DiscordWebhookBackend),
}

impl NotificationBackend {
    fn name(&self) -> &'static str {
        match self {
            Self::Discord(_) => "discord",
        }
    }

    async fn send(&self, event: &NotificationEvent) -> Result<()> {
        match self {
            Self::Discord(backend) => backend.send(event).await,
        }
    }
}

struct DiscordWebhookBackend {
    client: reqwest::Client,
    url: Url,
}

impl DiscordWebhookBackend {
    fn new(url: Url) -> Self {
        Self {
            client: reqwest::Client::new(),
            url,
        }
    }

    async fn send(&self, event: &NotificationEvent) -> Result<()> {
        let payload = match event {
            NotificationEvent::LongSession(notification) => self.format_long_session(notification),
            NotificationEvent::ClusterEnforcement(notification) => {
                self.format_cluster_enforcement(notification)
            }
            NotificationEvent::CapacityThreshold(notification) => {
                self.format_capacity_threshold(notification)
            }
        };
        let response = self
            .client
            .post(self.url.clone())
            .json(&payload)
            .send()
            .await
            .context("failed to send discord webhook request")?;
        if !response.status().is_success() {
            bail!(
                "discord webhook returned non-success status {}",
                response.status()
            );
        }
        Ok(())
    }

    fn format_long_session(&self, notification: &LongSessionNotification) -> serde_json::Value {
        self.embed_payload(
            ":warning: Long Session",
            vec![
                embed_field(
                    ":identification_card: Session ID",
                    &notification.session_id,
                    true,
                ),
                embed_field(":pencil: App Name", &notification.app_shortname, true),
                embed_field(
                    ":clock2: Duration",
                    &format_duration(notification.duration),
                    true,
                ),
            ],
        )
    }

    fn format_cluster_enforcement(
        &self,
        notification: &ClusterEnforcementNotification,
    ) -> serde_json::Value {
        let sessions = format_session_table(&notification.sessions);
        let ip_review = format_cluster_kicked_ip_review(&notification.ip_review);

        self.embed_payload(
            ":warning: Bot Cluster Enforcement",
            vec![
                embed_field(":round_pushpin: Node", &notification.node_id, true),
                embed_field(
                    ":bar_chart: Capacity",
                    &format!(
                        "{}/{}",
                        notification.current_sessions, notification.max_capacity
                    ),
                    true,
                ),
                embed_field(
                    ":no_entry: Kicked Sessions",
                    &notification.sessions.len().to_string(),
                    true,
                ),
                embed_field(
                    ":mag: Suspicious Clusters",
                    &notification.suspicious_cluster_count.to_string(),
                    true,
                ),
                embed_field(
                    ":chart_with_upwards_trend: Max Score",
                    &format!("{:.3}", notification.max_cluster_score),
                    true,
                ),
                embed_field(":busts_in_silhouette: Sessions", &sessions, false),
                embed_field(":shield: IP Review", &ip_review, false),
            ],
        )
    }

    fn format_capacity_threshold(
        &self,
        notification: &CapacityThresholdNotification,
    ) -> serde_json::Value {
        self.embed_payload(
            ":warning: Capacity Alert",
            vec![
                embed_field(":round_pushpin: Node", &notification.node_id, true),
                embed_field(
                    ":busts_in_silhouette: Current Sessions",
                    &notification.current_sessions.to_string(),
                    true,
                ),
                embed_field(
                    ":straight_ruler: Max Capacity",
                    &notification.max_capacity.to_string(),
                    true,
                ),
                embed_field(
                    ":chart_with_upwards_trend: Utilization",
                    &format!(
                        "{:.1}%",
                        utilization_percent(
                            notification.current_sessions,
                            notification.max_capacity
                        )
                    ),
                    true,
                ),
                embed_field(
                    ":triangular_flag_on_post: Threshold",
                    &format!("{}%", notification.threshold_percent),
                    true,
                ),
            ],
        )
    }

    fn embed_payload(&self, title: &str, fields: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({
            "content": serde_json::Value::Null,
            "embeds": [
                {
                    "title": title,
                    "color": serde_json::Value::Null,
                    "fields": fields,
                }
            ],
            "username": "Terminal Games",
            "attachments": [],
        })
    }
}

fn load_discord_webhook_url() -> Option<Url> {
    let raw = std::env::var("DISCORD_WEBHOOK_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())?;
    match Url::parse(&raw) {
        Ok(url) => Some(url),
        Err(error) => {
            tracing::warn!(?error, "Ignoring invalid DISCORD_WEBHOOK_URL");
            None
        }
    }
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let minutes = seconds / 60;
    let seconds = seconds % 60;
    if minutes == 0 {
        format!("{seconds}s")
    } else {
        format!("{minutes}m{seconds:02}s")
    }
}

fn utilization_percent(current_sessions: usize, max_capacity: usize) -> f64 {
    if max_capacity == 0 {
        return 0.0;
    }
    (current_sessions as f64 / max_capacity as f64) * 100.0
}

fn format_cluster_kicked_ip_review(review: &ClusterKickedIpReview) -> String {
    if review.entries.is_empty() {
        return format!(
            "no retained IP counts updated\nretention window: {}d",
            review.retention_days
        );
    }

    let mut rows = Vec::with_capacity(review.entries.len() + 1);
    rows.push(vec![
        "#".to_string(),
        format!("Total {}d", review.retention_days),
        "Delta".to_string(),
    ]);
    for (index, entry) in review.entries.iter().enumerate() {
        rows.push(vec![
            (index + 1).to_string(),
            entry.total_count.to_string(),
            format!("+{}", entry.incremented_by),
        ]);
    }
    format_table(&rows)
}

fn format_session_table(sessions: &[ClusterEnforcementSession]) -> String {
    if sessions.is_empty() {
        return "<none>".to_string();
    }
    let mut rows = Vec::with_capacity(sessions.len() + 1);
    rows.push(vec![
        "Session ID".to_string(),
        "Transport".to_string(),
        "App".to_string(),
    ]);
    for session in sessions {
        rows.push(vec![
            session.session_id.clone(),
            session.transport.clone(),
            session.app_shortname.clone(),
        ]);
    }
    format_table(&rows)
}

fn format_table(rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return "<none>".to_string();
    }
    let column_count = rows.iter().map(Vec::len).max().unwrap_or(0);
    if column_count == 0 {
        return "<none>".to_string();
    }

    let mut widths = vec![0_usize; column_count];
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.len());
        }
    }

    let mut lines = Vec::with_capacity(rows.len() + 1);
    for (row_index, row) in rows.iter().enumerate() {
        let mut padded = Vec::with_capacity(column_count);
        for (index, width) in widths.iter().enumerate() {
            let value = row.get(index).map(String::as_str).unwrap_or("");
            padded.push(format!("{value:<width$}", width = *width));
        }
        lines.push(padded.join(" | "));
        if row_index == 0 {
            lines.push(
                widths
                    .iter()
                    .map(|width| "-".repeat(*width))
                    .collect::<Vec<_>>()
                    .join("-+-"),
            );
        }
    }
    lines.join("\n")
}

fn embed_field(name: &str, value: &str, inline: bool) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "value": discord_code_block(value),
        "inline": inline,
    })
}

fn discord_code_block(value: &str) -> String {
    format!("```{value}```")
}
