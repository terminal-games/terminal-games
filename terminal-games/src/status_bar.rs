// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use terminput::{Event, MouseButton, MouseEventKind};
use tokio::sync::{mpsc, watch};
use unicode_width::UnicodeWidthStr;

use crate::app::SessionAppState;
use crate::control::{BroadcastLevel, ShutdownPhase, StatusBarState, TickerEntry};
use crate::palette::{self, Color};
use crate::rate_limiting::NetworkInfo;
use crate::terminal_profile::TerminalProfile;

fn terminal_width(str: &str) -> usize {
    strip_ansi_escapes::strip_str(str).width()
}

#[derive(Clone, Copy)]
pub enum StatusNotificationKind {
    Info,
    Warning,
    Error,
}

pub struct StatusNotification {
    content: String,
    duration: Duration,
    kind: StatusNotificationKind,
}

struct ActiveNotification {
    content: String,
    expires: Instant,
    kind: StatusNotificationKind,
}

impl StatusNotification {
    pub fn info(content: impl Into<String>, duration: Duration) -> Self {
        Self {
            content: content.into(),
            duration,
            kind: StatusNotificationKind::Info,
        }
    }

    pub fn warning(content: impl Into<String>, duration: Duration) -> Self {
        Self {
            content: content.into(),
            duration,
            kind: StatusNotificationKind::Warning,
        }
    }

    pub fn error(content: impl Into<String>, duration: Duration) -> Self {
        Self {
            content: content.into(),
            duration,
            kind: StatusNotificationKind::Error,
        }
    }
}

pub struct StatusBar {
    shortname: String,
    app_rx: watch::Receiver<SessionAppState>,
    username: String,
    username_rx: watch::Receiver<String>,
    session_start_time: std::time::Instant,
    prev_size: (u16, u16),
    prev_status_bar_content: Bytes,
    network_info: Arc<dyn NetworkInfo>,
    base_terminal_profile: TerminalProfile,
    notification: Option<ActiveNotification>,
    notification_rx: mpsc::Receiver<StatusNotification>,
    shared_state: StatusBarState,
    shared_state_rx: watch::Receiver<StatusBarState>,
    input: StatusBarInput,
    input_rx: watch::Receiver<u64>,
    tick: tokio::time::Interval,
}

struct RenderedTicker {
    text: String,
    click_cols: Vec<usize>,
}

#[derive(Default)]
struct StatusBarInputState {
    selected_ticker: Option<usize>,
    ticker_click_row: Option<u16>,
    ticker_click_cols: Vec<usize>,
}

#[derive(Clone)]
pub struct StatusBarInput {
    state: Arc<Mutex<StatusBarInputState>>,
    changed_tx: watch::Sender<u64>,
}

fn style(text: impl AsRef<str>, fg: Option<Color>, bg: Option<Color>, bold: bool) -> String {
    let Some(prefix) = style_prefix(fg, bg, bold) else {
        return text.as_ref().to_string();
    };
    format!("{prefix}{}\x1b[0m", text.as_ref())
}

fn style_prefix(fg: Option<Color>, bg: Option<Color>, bold: bool) -> Option<String> {
    let mut codes = Vec::with_capacity(3);
    if bold {
        codes.push("1".to_string());
    }
    if let Some(fg) = fg {
        codes.push(palette::render_color_code(fg, false));
    }
    if let Some(bg) = bg {
        codes.push(palette::render_color_code(bg, true));
    }
    if codes.is_empty() {
        return None;
    }
    Some(format!("\x1b[{}m", codes.join(";")))
}

fn style_ansi_text(
    text: impl AsRef<str>,
    fg: Option<Color>,
    bg: Option<Color>,
    bold: bool,
) -> String {
    let Some(prefix) = style_prefix(fg, bg, bold) else {
        return text.as_ref().to_string();
    };
    format!("{prefix}{}\x1b]8;;\x1b\\\x1b[0m", text.as_ref())
}

fn style_padded_ansi_text(
    text: impl AsRef<str>,
    fg: Option<Color>,
    bg: Option<Color>,
    bold: bool,
) -> String {
    let mut out = style(" ", fg, bg, bold);
    out.push_str(&style_ansi_text(text, fg, bg, bold));
    out.push_str(&style(" ", fg, bg, bold));
    out
}

impl StatusBar {
    pub fn new(
        app_rx: watch::Receiver<SessionAppState>,
        username_rx: watch::Receiver<String>,
        network_info: Arc<dyn NetworkInfo>,
        terminal_profile: TerminalProfile,
        notification_rx: mpsc::Receiver<StatusNotification>,
        shared_state_rx: watch::Receiver<StatusBarState>,
        input: StatusBarInput,
    ) -> Self {
        let shared_state = shared_state_rx.borrow().clone();
        let shortname = app_rx.borrow().shortname.clone();
        let username = username_rx.borrow().clone();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            shortname,
            app_rx,
            username,
            username_rx,
            session_start_time: std::time::Instant::now(),
            prev_size: (0, 0),
            prev_status_bar_content: Bytes::new(),
            network_info,
            base_terminal_profile: terminal_profile,
            notification: None,
            notification_rx,
            shared_state,
            shared_state_rx,
            input_rx: input.subscribe(),
            input,
            tick,
        }
    }

    fn content(&mut self, screen: &headless_terminal::Screen, width: u16, row: u16) -> Bytes {
        let p = palette::palette(self.terminal_profile(screen));
        let active_tab = style(
            format!(" {} ", self.shortname),
            Some(p.on_primary),
            Some(p.primary),
            true,
        );
        let username = style(
            format!(" {} ", self.username.as_str()),
            Some(p.text),
            Some(p.surface_bright),
            false,
        );
        let net = format!(
            " ↓{}kBps ",
            (self.network_info.bytes_per_sec_out() / 1024.0).ceil() as usize
        );
        let net = style(net, Some(p.text), Some(p.surface), false);
        let latency = if let Ok(latency) = self.network_info.latency() {
            format!("{}ms", latency.as_millis())
        } else {
            "".to_string()
        };
        let latency = style(latency, Some(p.text), Some(p.surface), false);

        let left = active_tab + &username + &net + &latency;

        let ssh_callout = style(
            " ssh -C terminalgames.net ",
            Some(p.on_primary),
            Some(p.primary),
            true,
        );
        let drain_badge = self.render_drain(&p).unwrap_or_default();
        let (right, ticker_click_cols) = if let Some(broadcast) = self.render_broadcast(&p) {
            (broadcast + &drain_badge + &ssh_callout, Vec::new())
        } else if let Some(notification) = self.render_notification(&p) {
            (notification + &drain_badge + &ssh_callout, Vec::new())
        } else if let Some(ticker) = self.render_ticker_content(&p) {
            let click_cols = ticker.click_cols;
            (ticker.text + &drain_badge + &ssh_callout, click_cols)
        } else {
            (drain_badge + &ssh_callout, Vec::new())
        };
        let right_start = (width as usize).saturating_sub(terminal_width(&right));
        self.input.set_ticker_click_targets(
            (!ticker_click_cols.is_empty()).then_some(row.saturating_sub(1)),
            ticker_click_cols
                .into_iter()
                .map(|col| right_start + col)
                .collect(),
        );

        let padding = style(
            " ".repeat(
                (width as usize).saturating_sub(terminal_width(&left) + terminal_width(&right)),
            ),
            None,
            Some(p.surface),
            false,
        );

        let content_str = left + &padding + &right;
        Bytes::from(content_str.into_bytes())
    }

    fn terminal_profile(&self, screen: &headless_terminal::Screen) -> TerminalProfile {
        match screen.terminal_background() {
            Some(headless_terminal::Color::Rgb(r, g, b)) => {
                self.base_terminal_profile.with_background_rgb((r, g, b))
            }
            _ => self.base_terminal_profile,
        }
    }

    fn render_notification(&self, p: &palette::Palette) -> Option<String> {
        let notif = match &self.notification {
            Some(notif) if Instant::now() < notif.expires => notif,
            _ => return None,
        };

        Some(match notif.kind {
            StatusNotificationKind::Info => style(
                notif.content.as_str(),
                Some(p.text),
                Some(p.surface_bright),
                false,
            ),
            StatusNotificationKind::Warning => style(
                notif.content.as_str(),
                Some(p.on_primary),
                Some(p.warning),
                true,
            ),
            StatusNotificationKind::Error => {
                style(notif.content.as_str(), Some(p.text), Some(p.danger), true)
            }
        })
    }

    fn render_broadcast(&self, p: &palette::Palette) -> Option<String> {
        let now = unix_now();
        let broadcast = self
            .shared_state
            .broadcasts
            .iter()
            .filter(|broadcast| broadcast.expires_at > now)
            .max_by_key(|broadcast| broadcast.created_at)?;
        Some(match broadcast.level {
            BroadcastLevel::Info => style_padded_ansi_text(
                broadcast.message.as_str(),
                Some(p.text),
                Some(p.surface_bright),
                false,
            ),
            BroadcastLevel::Warning => style_padded_ansi_text(
                broadcast.message.as_str(),
                Some(p.on_primary),
                Some(p.warning),
                true,
            ),
            BroadcastLevel::Error => style_padded_ansi_text(
                broadcast.message.as_str(),
                Some(p.text),
                Some(p.danger),
                true,
            ),
        })
    }

    fn render_drain(&self, p: &palette::Palette) -> Option<String> {
        let drain = self.shared_state.drain.as_ref()?;
        let text = match drain.phase {
            ShutdownPhase::Running => return None,
            ShutdownPhase::Draining => {
                let remaining = drain
                    .deadline_unix_ms
                    .map(|deadline_unix_ms| {
                        ((deadline_unix_ms / 1000).saturating_sub(unix_now())).max(0)
                    })
                    .unwrap_or_default();
                if remaining == 0 {
                    "Maintenance drain in progress".to_string()
                } else {
                    format!("Maintenance in {}", format_hhmmss(remaining as u64))
                }
            }
            ShutdownPhase::ShuttingDown => "Maintenance shutdown in progress".to_string(),
        };
        Some(style_padded_ansi_text(
            text,
            Some(p.on_primary),
            Some(p.warning),
            true,
        ))
    }

    fn render_ticker_content(&mut self, p: &palette::Palette) -> Option<RenderedTicker> {
        let now = unix_now();
        let ticker_count = self.active_ticker_count_at(now);
        if ticker_count == 0 {
            self.input.sync_selected_ticker(ticker_count);
            return None;
        }
        self.input.sync_selected_ticker(ticker_count);
        let selected_ticker = self.input.selected_ticker();
        let ticker_index =
            selected_ticker.unwrap_or_else(|| self.rotating_ticker_index(ticker_count));
        let rendered = {
            let tickers = self.active_tickers_at(now);
            if ticker_count == 1 {
                render_single_ticker(tickers[0], p)
            } else {
                render_multi_ticker(&tickers, ticker_index, p)
            }
        };
        Some(rendered)
    }

    fn show_notification(&mut self, notification: StatusNotification) {
        self.notification = Some(ActiveNotification {
            content: notification.content,
            expires: Instant::now() + notification.duration,
            kind: notification.kind,
        });
    }

    pub fn maybe_render_into(
        &mut self,
        screen: &headless_terminal::Screen,
        buf: &mut BytesMut,
        force: bool,
    ) -> bool {
        let (height, width) = screen.size();

        self.sync_inputs_if_needed();

        let content = self.content(screen, width, height);
        let size_changed = self.prev_size != (height, width);
        let content_changed = self.prev_status_bar_content != content;
        if !force && !size_changed && !content_changed {
            return false;
        }
        // tracing::info!(force, size_changed, content_changed, "draw status bar");

        buf.extend_from_slice(format!("\x1b[{};1H\x1b[0m", height).as_bytes());
        buf.extend_from_slice(&content);
        buf.extend_from_slice(&screen.cursor_state_formatted());
        buf.extend_from_slice(&screen.attributes_formatted());
        buf.extend_from_slice(&screen.input_mode_formatted());

        self.prev_size = (height, width);
        self.prev_status_bar_content = content;
        true
    }

    fn active_tickers_at(&self, now: i64) -> Vec<&TickerEntry> {
        self.shared_state
            .tickers
            .iter()
            .filter(|ticker| ticker.expires_at.is_none_or(|expires_at| expires_at > now))
            .collect()
    }

    fn active_ticker_count_at(&self, now: i64) -> usize {
        self.shared_state
            .tickers
            .iter()
            .filter(|ticker| ticker.expires_at.is_none_or(|expires_at| expires_at > now))
            .count()
    }

    fn rotating_ticker_index(&self, ticker_count: usize) -> usize {
        ((std::time::Instant::now() - self.session_start_time).as_secs() / 10) as usize
            % ticker_count
    }

    pub async fn drive(&mut self) -> Option<bool> {
        tokio::select! {
            _ = self.tick.tick() => Some(false),
            result = self.shared_state_rx.changed() => {
                result.ok()?;
                self.shared_state = self.shared_state_rx.borrow_and_update().clone();
                Some(false)
            }
            result = self.app_rx.changed() => {
                result.ok()?;
                self.shortname = self.app_rx.borrow_and_update().shortname.clone();
                Some(false)
            }
            result = self.username_rx.changed() => {
                result.ok()?;
                self.username = self.username_rx.borrow_and_update().clone();
                Some(false)
            }
            notification = self.notification_rx.recv() => {
                self.show_notification(notification?);
                Some(false)
            }
            result = self.input_rx.changed() => {
                result.ok()?;
                self.input_rx.borrow_and_update();
                Some(true)
            }
        }
    }

    fn sync_inputs_if_needed(&mut self) {
        if self.shared_state_rx.has_changed().unwrap_or(false) {
            self.shared_state = self.shared_state_rx.borrow_and_update().clone();
        }
        if self.app_rx.has_changed().unwrap_or(false) {
            self.shortname = self.app_rx.borrow_and_update().shortname.clone();
        }
        if self.username_rx.has_changed().unwrap_or(false) {
            self.username = self.username_rx.borrow_and_update().clone();
        }
        if self.input_rx.has_changed().unwrap_or(false) {
            self.input_rx.borrow_and_update();
        }
        while let Ok(notification) = self.notification_rx.try_recv() {
            self.show_notification(notification);
        }
    }
}

impl StatusBarInput {
    pub fn new() -> Self {
        let (changed_tx, _) = watch::channel(0);
        Self {
            state: Arc::new(Mutex::new(StatusBarInputState::default())),
            changed_tx,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed_tx.subscribe()
    }

    pub fn handle_terminal_input(&self, data: &[u8]) -> bool {
        let Ok(Some(Event::Mouse(mouse))) = Event::parse_from(data) else {
            return false;
        };
        if !matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
            return false;
        }
        let selected = {
            let mut state = self.state.lock().unwrap();
            if state.ticker_click_row != Some(mouse.row) {
                return false;
            }
            let Some(index) = state
                .ticker_click_cols
                .iter()
                .position(|col| *col == mouse.column as usize)
            else {
                return false;
            };
            state.selected_ticker = Some(index);
            true
        };
        if selected {
            self.notify_changed();
        }
        selected
    }

    pub fn set_ticker_click_targets(&self, row: Option<u16>, cols: Vec<usize>) {
        let mut state = self.state.lock().unwrap();
        state.ticker_click_row = row;
        state.ticker_click_cols = cols;
        if state
            .selected_ticker
            .is_some_and(|selected| selected >= state.ticker_click_cols.len())
        {
            state.selected_ticker = None;
        }
    }

    pub fn sync_selected_ticker(&self, ticker_count: usize) {
        let mut state = self.state.lock().unwrap();
        if ticker_count <= 1
            || state
                .selected_ticker
                .is_some_and(|selected| selected >= ticker_count)
        {
            state.selected_ticker = None;
        }
    }

    pub fn selected_ticker(&self) -> Option<usize> {
        self.state.lock().unwrap().selected_ticker
    }

    fn notify_changed(&self) {
        let next = self.changed_tx.borrow().wrapping_add(1);
        self.changed_tx.send_replace(next);
    }
}

fn render_single_ticker(ticker: &TickerEntry, p: &palette::Palette) -> RenderedTicker {
    RenderedTicker {
        text: style_padded_ansi_text(
            ticker.content.as_str(),
            Some(p.text),
            Some(p.surface_bright),
            false,
        ),
        click_cols: Vec::new(),
    }
}

fn render_multi_ticker(
    tickers: &[&TickerEntry],
    ticker_index: usize,
    p: &palette::Palette,
) -> RenderedTicker {
    let content_width = terminal_width(&tickers[ticker_index].content);
    let dots_start = 1 + content_width + 1;
    let mut text = style(" ", Some(p.text), Some(p.surface_bright), false);
    text.push_str(&style_ansi_text(
        tickers[ticker_index].content.as_str(),
        Some(p.text),
        Some(p.surface_bright),
        false,
    ));
    text.push_str(&style(" ", Some(p.text), Some(p.surface_bright), false));
    text.push_str(&style(
        "•".repeat(ticker_index),
        Some(p.text_subtle),
        Some(p.surface_bright),
        false,
    ));
    text.push_str(&style("•", Some(p.text), Some(p.surface_bright), false));
    text.push_str(&style(
        "•".repeat(tickers.len().saturating_sub(ticker_index + 1)),
        Some(p.text_subtle),
        Some(p.surface_bright),
        false,
    ));
    text.push_str(&style(" ", Some(p.text), Some(p.surface_bright), false));
    RenderedTicker {
        text,
        click_cols: (0..tickers.len()).map(|idx| dots_start + idx).collect(),
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

fn format_hhmmss(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds / 60) % 60;
    let secs = seconds % 60;
    format!("{hours:02}:{minutes:02}:{secs:02}")
}
