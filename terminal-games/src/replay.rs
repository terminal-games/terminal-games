// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::VecDeque,
    time::{Duration, Instant, SystemTime},
};

use bytes::Bytes;
use serde::Serialize;

use crate::mesh::AppId;

const REPLAY_DURATION: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub enum ReplayEvent {
    Output(Bytes),
    Resize { cols: u16, rows: u16 },
    AppSwitch { app_id: AppId, shortname: String },
}

struct TimestampedEvent {
    timestamp: Instant,
    event: ReplayEvent,
}

pub struct ReplayBuffer {
    events: VecDeque<TimestampedEvent>,
    vt: avt::Vt,
    vt_timestamp: Option<Instant>,
    initial_shortname: String,
    initial_app_id: AppId,
    term_type: Option<String>,
}

pub struct ReplayTerminalSnapshot {
    pub cols: u16,
    pub rows: u16,
    pub dump: String,
}

#[derive(Serialize)]
struct AsciicastHeader<'a> {
    version: u32,
    term: AsciicastTerm<'a>,
    timestamp: u64,
    title: String,
    command: String,
    tags: Vec<&'a str>,
}

#[derive(Serialize)]
struct AsciicastTerm<'a> {
    cols: usize,
    rows: usize,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    term_type: Option<&'a str>,
}

impl ReplayBuffer {
    pub fn new(
        cols: u16,
        rows: u16,
        shortname: &str,
        app_id: AppId,
        term_type: Option<&str>,
    ) -> Self {
        Self {
            events: VecDeque::with_capacity(1024),
            vt: avt::Vt::new((cols as usize).max(1), (rows as usize).max(1)),
            vt_timestamp: None,
            initial_shortname: shortname.to_string(),
            initial_app_id: app_id,
            term_type: term_type.map(String::from),
        }
    }

    pub fn push_output(&mut self, data: Bytes) {
        let now = Instant::now();
        self.prune(now);
        self.events.push_back(TimestampedEvent {
            timestamp: now,
            event: ReplayEvent::Output(data),
        });
    }

    pub fn push_resize(&mut self, cols: u16, rows: u16) {
        let now = Instant::now();
        self.prune(now);
        self.events.push_back(TimestampedEvent {
            timestamp: now,
            event: ReplayEvent::Resize { cols, rows },
        });
    }

    pub fn push_app_switch(&mut self, app_id: AppId, shortname: &str) {
        let now = Instant::now();
        self.prune(now);
        self.events.push_back(TimestampedEvent {
            timestamp: now,
            event: ReplayEvent::AppSwitch {
                app_id,
                shortname: shortname.to_string(),
            },
        });
    }

    fn prune(&mut self, now: Instant) {
        let cutoff = now - REPLAY_DURATION;

        while let Some(front) = self.events.front() {
            if front.timestamp >= cutoff {
                break;
            }
            let Some(event) = self.events.pop_front() else {
                break;
            };
            match event.event {
                ReplayEvent::Output(data) => {
                    self.vt.feed_str(&String::from_utf8_lossy(&data));
                }
                ReplayEvent::Resize { cols, rows } => {
                    self.vt
                        .resize((cols as usize).max(1), (rows as usize).max(1));
                }
                ReplayEvent::AppSwitch { app_id, shortname } => {
                    self.initial_shortname = shortname;
                    self.initial_app_id = app_id;
                }
            }
            self.vt_timestamp = Some(event.timestamp);
        }
    }

    pub fn terminal_snapshot(&self) -> ReplayTerminalSnapshot {
        let (cols, rows) = self.vt.size();
        let mut vt = avt::Vt::new(cols.max(1), rows.max(1));
        let dump = self.vt.dump();
        if !dump.is_empty() {
            vt.feed_str(&dump);
        }
        for event in &self.events {
            if self
                .vt_timestamp
                .is_some_and(|timestamp| event.timestamp <= timestamp)
            {
                continue;
            }
            match &event.event {
                ReplayEvent::Output(data) => {
                    vt.feed_str(&String::from_utf8_lossy(data));
                }
                ReplayEvent::Resize { cols, rows } => {
                    vt.resize((*cols as usize).max(1), (*rows as usize).max(1));
                }
                ReplayEvent::AppSwitch { .. } => {}
            }
        }
        let (cols, rows) = vt.size();
        ReplayTerminalSnapshot {
            cols: cols as u16,
            rows: rows as u16,
            dump: vt.dump(),
        }
    }

    pub fn serialize_asciicast(&self) -> Result<(AppId, Vec<u8>), serde_json::Error> {
        let mut output = Vec::with_capacity(16 * 1024);
        let now = Instant::now();

        let system_start = SystemTime::now()
            - self
                .events
                .front()
                .map(|e| now - e.timestamp)
                .unwrap_or(Duration::ZERO);
        let timestamp = system_start
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let (cols, rows) = self.vt.size();

        let header = AsciicastHeader {
            version: 3,
            term: AsciicastTerm {
                cols,
                rows,
                term_type: self.term_type.as_deref(),
            },
            timestamp,
            title: format!("Terminal Games | {}", self.initial_shortname),
            command: format!("ssh -C terminalgames.net -t {}", self.initial_shortname),
            tags: vec!["terminal-games", &self.initial_shortname],
        };
        serde_json::to_writer(&mut output, &header)?;
        output.push(b'\n');

        let mut prev_timestamp = self.vt_timestamp;
        let vt_output = self.vt.dump();
        if !vt_output.is_empty() {
            write_event(&mut output, 0.0, "o", &vt_output)?;
            if prev_timestamp.is_none() {
                prev_timestamp = self.events.front().map(|e| e.timestamp);
            }
        }

        for event in &self.events {
            let interval = prev_timestamp
                .map(|prev| event.timestamp.saturating_duration_since(prev))
                .unwrap_or(Duration::ZERO);
            let interval_secs = interval.as_secs_f64();
            prev_timestamp = Some(event.timestamp);

            match &event.event {
                ReplayEvent::Output(data) => {
                    let text = String::from_utf8_lossy(data);
                    write_event(&mut output, interval_secs, "o", &text)?;
                }
                ReplayEvent::Resize { cols, rows } => {
                    write_event(
                        &mut output,
                        interval_secs,
                        "r",
                        &format!("{}x{}", cols, rows),
                    )?;
                }
                ReplayEvent::AppSwitch {
                    app_id: _,
                    shortname,
                } => {
                    write_event(
                        &mut output,
                        interval_secs,
                        "m",
                        &format!("app:{}", shortname),
                    )?;
                }
            }
        }

        Ok((self.initial_app_id, output))
    }
}

fn write_event(
    output: &mut Vec<u8>,
    interval_secs: f64,
    code: &str,
    data: &str,
) -> Result<(), serde_json::Error> {
    let interval_secs = (interval_secs * 1000.0).round() / 1000.0;
    let event: (f64, &str, &str) = (interval_secs, code, data);
    serde_json::to_writer(&mut *output, &event)?;
    output.push(b'\n');
    Ok(())
}
