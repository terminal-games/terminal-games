// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use terminput::{Event, MouseButton, MouseEventKind};
use unicode_width::UnicodeWidthStr;

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
    pub shortname: String,
    username: String,
    tickers: Vec<String>,
    session_start_time: std::time::Instant,
    prev_size: (u16, u16),
    prev_status_bar_content: Vec<u8>,
    network_info: Arc<dyn NetworkInfo>,
    base_terminal_profile: TerminalProfile,
    notification: Option<ActiveNotification>,
    notification_rx: mpsc::Receiver<StatusNotification>,
    shared_state: StatusBarState,
    shared_state_rx: watch::Receiver<StatusBarState>,
    selected_ticker: Option<usize>,
    ticker_click_row: Option<u16>,
    ticker_click_cols: Vec<usize>,
}

struct RenderedTicker {
    text: String,
    click_cols: Vec<usize>,
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
        shortname: String,
        username: &str,
        network_info: Arc<dyn NetworkInfo>,
        terminal_profile: TerminalProfile,
        notification_rx: mpsc::Receiver<StatusNotification>,
        shared_state_rx: watch::Receiver<StatusBarState>,
    ) -> Self {
        let shared_state = shared_state_rx.borrow().clone();
        Self {
            shortname,
            username: username.to_string(),
            tickers: Vec::new(),
            session_start_time: std::time::Instant::now(),
            prev_size: (0, 0),
            prev_status_bar_content: Vec::new(),
            network_info,
            base_terminal_profile: terminal_profile,
            notification: None,
            notification_rx,
            shared_state,
            shared_state_rx,
            selected_ticker: None,
            ticker_click_row: None,
            ticker_click_cols: Vec::new(),
        }
    }

    fn content(
        &mut self,
        screen: &headless_terminal::Screen,
        width: u16,
        row: u16,
    ) -> Vec<u8> {
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
        self.ticker_click_row = (!ticker_click_cols.is_empty()).then_some(row.saturating_sub(1));
        self.ticker_click_cols = ticker_click_cols
            .into_iter()
            .map(|col| right_start + col)
            .collect();
        if self
            .selected_ticker
            .is_some_and(|selected| selected >= self.ticker_click_cols.len())
        {
            self.selected_ticker = None;
        }

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
            BroadcastLevel::Warning => style(
                content.as_str(),
                Some(p.on_primary),
                Some(p.warning),
                true,
            ),
            BroadcastLevel::Error => style(
                content.as_str(),
                Some(p.text),
                Some(p.danger),
                true,
            ),
        })
    }

    fn render_ticker_content(&self, p: &palette::Palette) -> Option<RenderedTicker> {
        let tickers = self.active_tickers();
        if tickers.is_empty() {
            return None;
        }
        let ticker_index = self.selected_ticker.unwrap_or_else(|| {
            ((std::time::Instant::now() - self.session_start_time).as_secs() / 10) as usize
                % tickers.len()
        });
        if tickers.len() == 1 {
            return Some(RenderedTicker {
                text: style(
                    format!(" {} ", tickers[0].content),
                    Some(p.text),
                    Some(p.surface_bright),
                    false,
                ),
                click_cols: Vec::new(),
            });
        }
        let content_width = tickers[ticker_index].content.width();
        let dots_start = 1 + content_width + 1;
        Some(RenderedTicker {
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
        })
    }

    fn show_notification(&mut self, notification: StatusNotification) {
        self.notification = Some(ActiveNotification {
            content: notification.content,
            expires: Instant::now() + notification.duration,
            kind: notification.kind,
        });
    }

    pub fn set_username(&mut self, username: &str) {
        self.username = username.to_string();
    }

    pub fn handle_terminal_input(&mut self, data: &[u8]) -> bool {
        let Ok(Some(Event::Mouse(mouse))) = Event::parse_from(data) else {
            return false;
        };
        if !matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
            return false;
        }
        if self.ticker_click_row != Some(mouse.row as u16) {
            return false;
        }
        let Some(index) = self
            .ticker_click_cols
            .iter()
            .position(|col| *col == mouse.column as usize)
        else {
            return false;
        };
        self.selected_ticker = Some(index);
        true
    }

    pub fn maybe_render_into(
        &mut self,
        screen: &headless_terminal::Screen,
        buf: &mut Vec<u8>,
        force: bool,
    ) -> bool {
        let (height, width) = screen.size();

        if self.shared_state_rx.has_changed().unwrap_or(false) {
            self.shared_state = self.shared_state_rx.borrow_and_update().clone();
            self.tickers = self
                .shared_state
                .tickers
                .iter()
                .map(|ticker| ticker.content.clone())
                .collect();
            let ticker_count = self.active_tickers().len();
            if ticker_count <= 1
                || self
                    .selected_ticker
                    .is_some_and(|selected| selected >= ticker_count)
            {
                self.selected_ticker = None;
            }
        }

        match self.notification_rx.try_recv() {
            Ok(notification) => {
                self.show_notification(notification);
            }
            Err(_) => {}
        }

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

    fn active_tickers(&self) -> Vec<&TickerEntry> {
        let now = unix_now();
        self.shared_state
            .tickers
            .iter()
            .filter(|ticker| ticker.expires_at.is_none_or(|expires_at| expires_at > now))
            .collect()
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
