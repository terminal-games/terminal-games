// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI32, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use terminput::Event;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const FORWARD_CHANNEL_CAPACITY: usize = 12;
const INITIAL_FUEL: i32 = 60;
const FUEL_LOSS_PER_TICK: i32 = 1;
const FUEL_GAIN_WITH_INPUT_PER_TICK: i32 = FUEL_LOSS_PER_TICK + 5;

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

struct InputGuardShared {
    cancellation_token: CancellationToken,
    input_tx: mpsc::Sender<Bytes>,
    replay_request_tx: mpsc::Sender<()>,
    fuel_tx: watch::Sender<i32>,
    current_fuel: AtomicI32,
    saw_input_this_tick: AtomicBool,
}

#[derive(Clone)]
pub struct InputGuard {
    shared: Arc<InputGuardShared>,
}

pub struct InputGuardTicker {
    shared: Arc<InputGuardShared>,
    detector: IdleDetector,
    tick: tokio::time::Interval,
}

impl InputGuard {
    pub fn new(
        cancellation_token: CancellationToken,
        replay_request_tx: mpsc::Sender<()>,
    ) -> (
        Self,
        InputGuardTicker,
        mpsc::Receiver<Bytes>,
        watch::Receiver<i32>,
    ) {
        let (input_tx, input_rx) = mpsc::channel(FORWARD_CHANNEL_CAPACITY);
        let (fuel_tx, fuel_rx) = watch::channel(INITIAL_FUEL);
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let guard = Self {
            shared: Arc::new(InputGuardShared {
                cancellation_token,
                input_tx,
                replay_request_tx,
                fuel_tx,
                current_fuel: AtomicI32::new(INITIAL_FUEL),
                saw_input_this_tick: AtomicBool::new(false),
            }),
        };

        (
            guard.clone(),
            InputGuardTicker {
                shared: guard.shared.clone(),
                detector: IdleDetector::new(),
                tick,
            },
            input_rx,
            fuel_rx,
        )
    }

    pub fn handle_input(&self, data: Bytes) -> Result<(), InputGuardError> {
        if data.is_empty() {
            return Ok(());
        }
        if is_interrupt(data.as_ref()) {
            self.shared.cancellation_token.cancel();
        }
        if is_replay_request(data.as_ref()) {
            let _ = self.shared.replay_request_tx.try_send(());
        }
        if has_user_input(data.as_ref()) {
            self.shared
                .saw_input_this_tick
                .store(true, Ordering::Release);
            let _ = self
                .shared
                .fuel_tx
                .send(self.shared.current_fuel.load(Ordering::Acquire));
        }
        match self.shared.input_tx.try_send(data) {
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(InputGuardError::InputClosed)
            }
        }
    }

    pub fn replay_buffered_input(&self, data: Bytes) -> Result<(), InputGuardError> {
        match self.shared.input_tx.try_send(data) {
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(InputGuardError::InputClosed)
            }
        }
    }
}

impl InputGuardTicker {
    pub async fn next_tick(&mut self) {
        self.tick.tick().await;
    }

    pub fn handle_tick(&mut self) {
        let saw_input = self
            .shared
            .saw_input_this_tick
            .swap(false, Ordering::AcqRel);
        let status = self.detector.tick(saw_input);
        self.shared
            .current_fuel
            .store(status.fuel, Ordering::Release);
        let _ = self.shared.fuel_tx.send(status.fuel);
        if status.should_kick {
            self.shared.cancellation_token.cancel();
        }
    }
}

struct IdleDetector {
    fuel: i32,
}

struct DetectorStatus {
    fuel: i32,
    should_kick: bool,
}

impl IdleDetector {
    fn new() -> Self {
        Self { fuel: INITIAL_FUEL }
    }

    fn tick(&mut self, saw_input_this_tick: bool) -> DetectorStatus {
        self.fuel -= FUEL_LOSS_PER_TICK;
        if saw_input_this_tick {
            self.fuel += FUEL_GAIN_WITH_INPUT_PER_TICK;
        }
        self.fuel = self.fuel.clamp(0, INITIAL_FUEL);
        DetectorStatus {
            fuel: self.fuel,
            should_kick: self.fuel <= 0,
        }
    }
}

fn has_user_input(data: &[u8]) -> bool {
    if data.starts_with(b"\x1b]") {
        return false;
    }
    match Event::parse_from(data) {
        Ok(Some(Event::Key(_) | Event::Mouse(_))) => true,
        Ok(Some(_)) => false,
        Ok(None) | Err(_) => false,
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
