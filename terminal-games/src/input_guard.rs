// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use headless_terminal::{Color, Parser};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::terminal_profile::TerminalProfile;

pub type TerminalParser = Parser;

#[derive(Debug)]
pub enum InputForwardError {
    InputClosed,
}

impl fmt::Display for InputForwardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputClosed => write!(f, "input channel closed"),
        }
    }
}

impl std::error::Error for InputForwardError {}

pub struct PendingInput {
    input_tx: mpsc::Sender<Bytes>,
    data: Bytes,
}

impl PendingInput {
    pub fn try_send(self) -> Result<(), InputForwardError> {
        self.input_tx
            .try_send(self.data)
            .map_err(|_| InputForwardError::InputClosed)
    }

    pub async fn send(self) -> Result<(), InputForwardError> {
        self.input_tx
            .send(self.data)
            .await
            .map_err(|_| InputForwardError::InputClosed)
    }
}

pub struct InputForwarder {
    cancellation_token: CancellationToken,
    input_tx: mpsc::Sender<Bytes>,
    replay_request_tx: mpsc::Sender<()>,
}

impl InputForwarder {
    pub fn new(
        cancellation_token: CancellationToken,
        replay_request_tx: mpsc::Sender<()>,
    ) -> (Self, mpsc::Receiver<Bytes>) {
        let (input_tx, input_rx) = mpsc::channel(12);
        (
            Self::new_with_sender(cancellation_token, input_tx, replay_request_tx),
            input_rx,
        )
    }

    pub fn new_with_sender(
        cancellation_token: CancellationToken,
        input_tx: mpsc::Sender<Bytes>,
        replay_request_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            cancellation_token,
            input_tx,
            replay_request_tx,
        }
    }

    pub fn prepare_input(&self, data: Bytes) -> PendingInput {
        self.observe_input(data.as_ref());
        PendingInput {
            input_tx: self.input_tx.clone(),
            data,
        }
    }

    fn observe_input(&self, data: &[u8]) {
        if is_interrupt(data) {
            self.cancellation_token.cancel();
        }
        if is_replay_request(data) {
            let _ = self.replay_request_tx.try_send(());
        }
    }
}

pub struct TerminalBackgroundTracker {
    parser: Parser,
}

impl Default for TerminalBackgroundTracker {
    fn default() -> Self {
        Self {
            parser: Parser::default(),
        }
    }
}

impl TerminalBackgroundTracker {
    pub fn observe(&mut self, data: &[u8]) {
        self.parser.process(data);
    }

    pub fn terminal_profile(&self, profile: TerminalProfile) -> TerminalProfile {
        self.terminal_background_rgb()
            .map_or(profile, |rgb| profile.with_background_rgb(rgb))
    }

    pub async fn wait_for_terminal_background(
        &mut self,
        raw_input_rx: &mut mpsc::Receiver<Bytes>,
        cancellation_token: &CancellationToken,
        timeout: Duration,
    ) -> Option<(u8, u8, u8)> {
        let deadline = tokio::time::Instant::now() + timeout;
        while self.terminal_background_rgb().is_none() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() || cancellation_token.is_cancelled() {
                break;
            }
            tokio::select! {
                _ = cancellation_token.cancelled() => break,
                result = tokio::time::timeout(remaining, raw_input_rx.recv()) => match result {
                    Ok(Some(data)) => self.observe(data.as_ref()),
                    _ => break,
                }
            }
        }
        self.terminal_background_rgb()
    }

    pub fn into_terminal_parser(self, rows: u16, cols: u16) -> Parser {
        let background = self.terminal_background_rgb();
        let mut parser = Parser::default();
        parser.screen_mut().set_size(rows, cols);
        if let Some((r, g, b)) = background {
            let seq = format!("\x1b]11;rgb:{r:02x}{r:02x}/{g:02x}{g:02x}/{b:02x}{b:02x}\x07");
            parser.process(seq.as_bytes());
        }
        parser
    }

    fn terminal_background_rgb(&self) -> Option<(u8, u8, u8)> {
        match self.parser.screen().terminal_background() {
            Some(Color::Rgb(r, g, b)) => Some((r, g, b)),
            _ => None,
        }
    }
}

fn is_interrupt(data: &[u8]) -> bool {
    data == b"\x03"
}

fn is_replay_request(data: &[u8]) -> bool {
    data == b"\x12"
        || data == b"\x1b[27;5;114~"
        || (data.starts_with(b"\x1b[114;5") && data.ends_with(b"u"))
}
