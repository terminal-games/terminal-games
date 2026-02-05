// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use unicode_width::UnicodeWidthStr;
use yansi::Paint;

use crate::rate_limiting::NetworkInfo;

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
    username: String,
    tickers: Vec<String>,
    session_start_time: std::time::Instant,
    prev_size: (u16, u16),
    prev_status_bar_content: Vec<u8>,
    network_info: Arc<dyn NetworkInfo>,
    notification: Option<Notification>,
    notification_rx: mpsc::Receiver<String>,
}

impl StatusBar {
    pub fn new(
        shortname: String,
        username: String,
        network_info: Arc<dyn NetworkInfo>,
        notification_rx: mpsc::Receiver<String>,
    ) -> Self {
        Self {
            shortname,
            username,
            tickers: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            session_start_time: std::time::Instant::now(),
            prev_size: (0, 0),
            prev_status_bar_content: Vec::new(),
            network_info,
            notification: None,
            notification_rx,
        }
    }

    fn content(&self, width: u16) -> Vec<u8> {
        let active_tab = format!(" {} ", self.shortname)
            .bold()
            .black()
            .on_green()
            .to_string();
        let username = format!(" {} ", self.username)
            .white()
            .on_fixed(237)
            .to_string();
        let net = format!(
            " ↓{}kBps ",
            (self.network_info.bytes_per_sec_out() / 1024.0).ceil() as usize
        )
        .white()
        .on_fixed(236)
        .to_string();
        let latency = if let Ok(latency) = self.network_info.latency() {
            format!("{}ms", latency.as_millis())
        } else {
            "".to_string()
        }
        .white()
        .on_fixed(236)
        .to_string();

        let notification = match &self.notification {
            Some(notif) if Instant::now() < notif.expires => {
                format!("\x1b[48;5;236m{}", notif.content)
            }
            _ => String::new(),
        };

        let left = active_tab + &username + &net + &latency + &notification;

        let ssh_callout = " ssh -C terminal-games.fly.dev ".bold().black().on_green();
        let ticker_index = ((std::time::Instant::now() - self.session_start_time).as_secs() / 10)
            as usize
            % self.tickers.len();
        let ticker = format!(
            " {} {}{}{} ",
            self.tickers[ticker_index],
            "•".repeat(ticker_index).fixed(241),
            "•".fixed(249),
            "•"
                .repeat(self.tickers.len().saturating_sub(ticker_index + 1))
                .fixed(241),
        )
        .on_fixed(237)
        .wrap()
        .to_string();
        let right = ticker + &ssh_callout.to_string();

        let padding = " "
            .repeat((width as usize).saturating_sub(terminal_width(&left) + terminal_width(&right)))
            .on_fixed(236)
            .to_string();

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
            Err(_) => {},
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
