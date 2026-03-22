// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use headless_terminal::{Color, Parser};
use terminput::Event;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::terminal_profile::TerminalProfile;

const FORWARD_CHANNEL_CAPACITY: usize = 12;
const INITIAL_FUEL: i32 = 60;
const FUEL_LOSS_PER_TICK: i32 = 1;
const FUEL_GAIN_WITH_INPUT_PER_TICK: i32 = FUEL_LOSS_PER_TICK + 5;

pub type TerminalParser = Parser;

#[derive(Debug)]
pub enum InputGuardError {
    InputClosed,
}

impl fmt::Display for InputGuardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputClosed => write!(f, "input channel closed"),
        }
    }
}

impl std::error::Error for InputGuardError {}

pub struct PendingInput {
    input_tx: mpsc::Sender<Bytes>,
    data: Bytes,
}

impl PendingInput {
    pub fn try_send(self) -> Result<(), InputGuardError> {
        self.input_tx
            .try_send(self.data)
            .map_err(|_| InputGuardError::InputClosed)
    }

    pub async fn send(self) -> Result<(), InputGuardError> {
        self.input_tx
            .send(self.data)
            .await
            .map_err(|_| InputGuardError::InputClosed)
    }
}

pub struct InputGuard {
    cancellation_token: CancellationToken,
    input_tx: mpsc::Sender<Bytes>,
    replay_request_tx: mpsc::Sender<()>,
    fuel_tx: watch::Sender<i32>,
    current_fuel: i32,
    saw_input_this_tick: bool,
    terminal_parser: Option<Parser>,
}

impl InputGuard {
    pub fn new(
        cancellation_token: CancellationToken,
        replay_request_tx: mpsc::Sender<()>,
    ) -> (Self, mpsc::Receiver<Bytes>, watch::Receiver<i32>) {
        let (input_tx, input_rx) = mpsc::channel(FORWARD_CHANNEL_CAPACITY);
        let (fuel_tx, fuel_rx) = watch::channel(INITIAL_FUEL);
        (
            Self {
                cancellation_token,
                input_tx,
                replay_request_tx,
                fuel_tx,
                current_fuel: INITIAL_FUEL,
                saw_input_this_tick: false,
                terminal_parser: Some(Parser::default()),
            },
            input_rx,
            fuel_rx,
        )
    }

    pub fn tick_interval() -> tokio::time::Interval {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick
    }

    pub fn prepare_input(&mut self, data: Bytes) -> PendingInput {
        if let Some(parser) = &mut self.terminal_parser {
            parser.process(&data);
        }
        self.observe_input(data.as_ref());
        PendingInput {
            input_tx: self.input_tx.clone(),
            data,
        }
    }

    pub async fn handle_input(&mut self, data: Bytes) -> Result<(), InputGuardError> {
        self.prepare_input(data).send().await
    }

    pub fn terminal_profile(&self, profile: TerminalProfile) -> TerminalProfile {
        self.terminal_background_rgb()
            .map_or(profile, |rgb| profile.with_background_rgb(rgb))
    }

    pub async fn wait_for_terminal_background(
        &mut self,
        raw_input_rx: &mut mpsc::Receiver<Bytes>,
        timeout: Duration,
    ) -> Result<Option<(u8, u8, u8)>, InputGuardError> {
        let deadline = tokio::time::Instant::now() + timeout;
        while self.terminal_background_rgb().is_none() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, raw_input_rx.recv()).await {
                Ok(Some(data)) => {
                    let _ = self.prepare_input(data);
                }
                _ => break,
            }
        }
        Ok(self.terminal_background_rgb())
    }

    pub fn take_terminal_parser(&mut self, rows: u16, cols: u16) -> Parser {
        let background = self.terminal_background_rgb();
        self.terminal_parser = None;
        let mut parser = Parser::default();
        parser.screen_mut().set_size(rows, cols);
        if let Some((r, g, b)) = background {
            let seq = format!("\x1b]11;rgb:{r:02x}{r:02x}/{g:02x}{g:02x}/{b:02x}{b:02x}\x07");
            parser.process(seq.as_bytes());
        }
        parser
    }

    pub fn tick(&mut self) {
        self.current_fuel = (self.current_fuel - FUEL_LOSS_PER_TICK
            + if self.saw_input_this_tick {
                FUEL_GAIN_WITH_INPUT_PER_TICK
            } else {
                0
            })
        .clamp(0, INITIAL_FUEL);
        self.saw_input_this_tick = false;
        let _ = self.fuel_tx.send(self.current_fuel);
        if self.current_fuel == 0 {
            self.cancellation_token.cancel();
        }
    }

    pub fn is_idle_timed_out(&self) -> bool {
        self.current_fuel == 0
    }

    fn observe_input(&mut self, data: &[u8]) {
        if is_interrupt(data) {
            self.cancellation_token.cancel();
        }
        if is_replay_request(data) {
            let _ = self.replay_request_tx.try_send(());
        }
        if has_user_input(data) {
            self.saw_input_this_tick = true;
        }
    }

    fn terminal_background_rgb(&self) -> Option<(u8, u8, u8)> {
        match self
            .terminal_parser
            .as_ref()
            .map(|parser| parser.screen().terminal_background())
        {
            Some(Some(Color::Rgb(r, g, b))) => Some((r, g, b)),
            _ => None,
        }
    }
}

fn has_user_input(data: &[u8]) -> bool {
    if data.starts_with(b"\x1b]") {
        return false;
    }
    match Event::parse_from(data) {
        Ok(Some(Event::Key(_) | Event::Mouse(_))) => true,
        Ok(Some(_)) | Ok(None) | Err(_) => false,
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
