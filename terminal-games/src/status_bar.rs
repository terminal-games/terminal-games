// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use unicode_width::UnicodeWidthStr;

use crate::palette::{self, Color};
use crate::rate_limiting::NetworkInfo;
use crate::terminal_profile::TerminalProfile;

fn terminal_width(str: &str) -> usize {
    strip_ansi_escapes::strip_str(str).width()
}

const NOTIFICATION_DURATION: Duration = Duration::from_secs(10);

struct Notification {
    content: String,
    expires: Instant,
}

pub struct StatusBar {
    pub shortname: String,
    username_rx: watch::Receiver<String>,
    tickers: Vec<String>,
    session_start_time: std::time::Instant,
    prev_size: (u16, u16),
    prev_status_bar_content: Vec<u8>,
    network_info: Arc<dyn NetworkInfo>,
    terminal_profile: TerminalProfile,
    notification: Option<Notification>,
    notification_rx: mpsc::Receiver<String>,
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
        username_rx: watch::Receiver<String>,
        network_info: Arc<dyn NetworkInfo>,
        terminal_profile: TerminalProfile,
        notification_rx: mpsc::Receiver<String>,
    ) -> Self {
        Self {
            shortname,
            username_rx,
            tickers: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            session_start_time: std::time::Instant::now(),
            prev_size: (0, 0),
            prev_status_bar_content: Vec::new(),
            network_info,
            terminal_profile,
            notification: None,
            notification_rx,
        }
    }

    fn content(&self, width: u16) -> Vec<u8> {
        let p = palette::palette(self.terminal_profile);
        let active_tab = style(
            format!(" {} ", self.shortname),
            Some(p.on_primary),
            Some(p.primary),
            true,
        );
        let username = style(
            format!(" {} ", self.username_rx.borrow().as_str()),
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

        let notification = match &self.notification {
            Some(notif) if Instant::now() < notif.expires => {
                format!(
                    "\x1b[{}m{}",
                    palette::render_color_code(p.surface, true),
                    notif.content
                )
            }
            _ => String::new(),
        };

        let left = active_tab + &username + &net + &latency + &notification;

        let ssh_callout = style(
            " ssh -C terminalgames.net ",
            Some(p.on_primary),
            Some(p.primary),
            true,
        );
        let ticker_index = ((std::time::Instant::now() - self.session_start_time).as_secs() / 10)
            as usize
            % self.tickers.len();
        let ticker = style(
            format!(
                " {} {}{}{} ",
                self.tickers[ticker_index],
                style_inline_fg("•".repeat(ticker_index), p.text_subtle),
                style_inline_fg("•", p.text),
                style_inline_fg(
                    "•".repeat(self.tickers.len().saturating_sub(ticker_index + 1)),
                    p.text_subtle
                ),
            ),
            Some(p.text),
            Some(p.surface_bright),
            false,
        );
        let right = ticker + &ssh_callout;

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

    pub fn maybe_render_into(
        &mut self,
        screen: &headless_terminal::Screen,
        buf: &mut Vec<u8>,
        force: bool,
    ) -> bool {
        let (height, width) = screen.size();

        match self.notification_rx.try_recv() {
            Ok(content) => {
                self.notification = Some(Notification {
                    content,
                    expires: Instant::now() + NOTIFICATION_DURATION,
                });
            }
            Err(_) => {}
        }

        let content = self.content(width);
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
}
