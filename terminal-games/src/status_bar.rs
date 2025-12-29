// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::Arc;

use tokio::sync::Mutex;
use unicode_width::UnicodeWidthStr;
use yansi::Paint;

use crate::ssh::Terminal;

fn terminal_width(str: &str) -> usize {
    strip_ansi_escapes::strip_str(str).width()
}

pub struct StatusBar {
    terminal: Arc<Mutex<Terminal>>,
    current_app_shortname: Arc<Mutex<String>>,
    username: String,
    tickers: Vec<String>,
    session_start_time: std::time::Instant,
    prev_size: (u16, u16),
    prev_status_bar_content: Vec<u8>,
}

impl StatusBar {
    pub fn new(
        terminal: Arc<Mutex<Terminal>>,
        current_app_shortname: Arc<Mutex<String>>,
        username: String,
        session_start_time: std::time::Instant,
    ) -> Self {
        Self {
            terminal,
            current_app_shortname,
            username,
            tickers: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            session_start_time,
            prev_size: (0, 0),
            prev_status_bar_content: Vec::new(),
        }
    }

    async fn content(&self, width: u16) -> Vec<u8> {
        let shortname = self.current_app_shortname.lock().await.clone();

        let active_tab_text = format!(" {} ", shortname);
        let active_tab = active_tab_text.bold().black().on_green().to_string();
        let username_text = format!(" {} ", self.username);
        let username = username_text.white().on_fixed(237).to_string();
        let left = active_tab + &username;

        let ssh_callout = " ssh terminal-games.fly.dev ".bold().black().on_green();
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
        let content_bytes = content_str.as_bytes().to_vec();
        content_bytes
    }

    pub async fn maybe_render_into(&mut self, buf: &mut Vec<u8>, force: bool) -> bool {
        let terminal = self.terminal.lock().await;
        let screen = terminal.screen();
        let (height, width) = screen.size();

        let content = self.content(width).await;
        let size_changed = self.prev_size != (height, width);
        let content_changed = self.prev_status_bar_content != content;
        if !force && !size_changed && !content_changed {
            return false;
        }
        tracing::info!(force, size_changed, content_changed, "draw status bar");

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
