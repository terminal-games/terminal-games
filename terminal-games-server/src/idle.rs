// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::time::Duration;

use terminal_games::app::{SessionEndReason, SessionNotificationSender};
use terminput::Event;

use crate::sessions::SessionIdleState;

pub const INITIAL_FUEL_SECS: i32 = 60;
const FUEL_LOSS_PER_TICK: i32 = 1;
const FUEL_GAIN_WITH_INPUT_PER_TICK: i32 = FUEL_LOSS_PER_TICK + 5;
const WARNING_THRESHOLD_SECS: i32 = 10;

pub struct IdleMonitor {
    notification_tx: SessionNotificationSender,
    tick: tokio::time::Interval,
    current_fuel: i32,
    paused: bool,
    saw_input_this_tick: bool,
    warning_sent: bool,
}

impl IdleMonitor {
    pub fn new(notification_tx: SessionNotificationSender) -> Self {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            notification_tx,
            tick,
            current_fuel: INITIAL_FUEL_SECS,
            paused: false,
            saw_input_this_tick: false,
            warning_sent: false,
        }
    }

    pub async fn wait_for_tick(&mut self) {
        self.tick.tick().await;
    }

    pub fn observe_input(&mut self, data: &[u8]) {
        if has_user_input(data) {
            self.saw_input_this_tick = true;
        }
    }

    pub fn observe_resize(&mut self) {
        self.saw_input_this_tick = true;
    }

    pub fn on_tick(&mut self) -> Option<SessionEndReason> {
        if self.paused {
            self.saw_input_this_tick = false;
            self.warning_sent = false;
            return None;
        }

        self.current_fuel = (self.current_fuel - FUEL_LOSS_PER_TICK
            + if self.saw_input_this_tick {
                FUEL_GAIN_WITH_INPUT_PER_TICK
            } else {
                0
            })
        .clamp(0, INITIAL_FUEL_SECS);
        self.saw_input_this_tick = false;

        if self.current_fuel == 0 {
            self.warning_sent = true;
            return Some(SessionEndReason::IdleTimeout);
        }

        if self.current_fuel <= WARNING_THRESHOLD_SECS {
            if !self.warning_sent {
                self.notification_tx.try_warning(
                    format!(" Idle timeout in {} seconds. ", self.current_fuel),
                    Duration::from_secs(self.current_fuel as u64),
                );
                self.warning_sent = true;
            }
            return None;
        }

        self.warning_sent = false;
        None
    }

    pub fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
        if paused {
            self.saw_input_this_tick = false;
            self.warning_sent = false;
        }
    }

    pub fn idle_state(&self) -> SessionIdleState {
        SessionIdleState {
            fuel_seconds: self.current_fuel,
            paused: self.paused,
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
