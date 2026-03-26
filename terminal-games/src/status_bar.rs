// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use terminput::{Event, MouseButton, MouseEventKind};
use tokio::sync::{mpsc, watch};
use unicode_width::UnicodeWidthStr;

use crate::app::SessionAppState;
use crate::control::{BroadcastLevel, StatusBarState, TickerEntry};
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
    prev_status_bar_content: Vec<u8>,
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
        return text.as_ref().to_string();
    }
    format!("\x1b[{}m{}\x1b[0m", codes.join(";"), text.as_ref())
}

fn style_inline_fg(text: impl AsRef<str>, fg: Color) -> String {
    format!(
        "\x1b[{}m{}",
        palette::render_color_code(fg, false),
        text.as_ref()
    )
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
            prev_status_bar_content: Vec::new(),
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

    fn content(&mut self, screen: &headless_terminal::Screen, width: u16, row: u16) -> Vec<u8> {
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
        let (right, ticker_click_cols) = if let Some(broadcast) = self.render_broadcast(&p) {
            (broadcast + &ssh_callout, Vec::new())
        } else if let Some(notification) = self.render_notification(&p) {
            (notification + &ssh_callout, Vec::new())
        } else if let Some(ticker) = self.render_ticker_content(&p) {
            let click_cols = ticker.click_cols;
            (ticker.text + &ssh_callout, click_cols)
        } else {
            (ssh_callout, Vec::new())
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
        content_str.into_bytes()
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
        let content = format!(" {} ", broadcast.message);
        Some(match broadcast.level {
            BroadcastLevel::Info => style(
                content.as_str(),
                Some(p.text),
                Some(p.surface_bright),
                false,
            ),
            BroadcastLevel::Warning => {
                style(content.as_str(), Some(p.on_primary), Some(p.warning), true)
            }
            BroadcastLevel::Error => style(content.as_str(), Some(p.text), Some(p.danger), true),
        })
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
        buf: &mut Vec<u8>,
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
        text: style(
            format!(" {} ", ticker.content),
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
    let content_width = tickers[ticker_index].content.width();
    let dots_start = 1 + content_width + 1;
    RenderedTicker {
        text: style(
            format!(
                " {} {}{}{} ",
                tickers[ticker_index].content,
                style_inline_fg("•".repeat(ticker_index), p.text_subtle),
                style_inline_fg("•", p.text),
                style_inline_fg(
                    "•".repeat(tickers.len().saturating_sub(ticker_index + 1)),
                    p.text_subtle
                ),
            ),
            Some(p.text),
            Some(p.surface_bright),
            false,
        ),
        click_cols: (0..tickers.len()).map(|idx| dots_start + idx).collect(),
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
