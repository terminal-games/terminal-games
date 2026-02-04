// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::VecDeque,
    time::{Duration, Instant, SystemTime},
};

use serde::Serialize;

const REPLAY_DURATION: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub enum ReplayEvent {
    Output(Vec<u8>),
    Resize { cols: u16, rows: u16 },
    AppSwitch { shortname: String },
}

struct TimestampedEvent {
    timestamp: Instant,
    event: ReplayEvent,
}

pub struct ReplayBuffer {
    events: VecDeque<TimestampedEvent>,
    vt: avt::Vt,
    vt_timestamp: Option<Instant>,
    initial_cols: u16,
    initial_rows: u16,
    current_cols: u16,
    current_rows: u16,
    initial_shortname: String,
    current_shortname: String,
    term_type: Option<String>,
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
    cols: u16,
    rows: u16,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    term_type: Option<&'a str>,
}

impl ReplayBuffer {
    pub fn new(cols: u16, rows: u16, shortname: String, term_type: Option<String>) -> Self {
        Self {
            events: VecDeque::with_capacity(1024),
            vt: avt::Vt::new((cols as usize).max(1), (rows as usize).max(1)),
            vt_timestamp: None,
            initial_cols: cols,
            initial_rows: rows,
            current_cols: cols,
            current_rows: rows,
            initial_shortname: shortname.clone(),
            current_shortname: shortname,
            term_type,
        }
    }

    pub fn push_output(&mut self, data: Vec<u8>) {
        let now = Instant::now();
        self.prune(now);
        self.events.push_back(TimestampedEvent {
            timestamp: now,
            event: ReplayEvent::Output(data),
        });
    }

    pub fn push_resize(&mut self, cols: u16, rows: u16) {
        if cols == self.current_cols && rows == self.current_rows {
            return;
        }
        self.current_cols = cols;
        self.current_rows = rows;
        let now = Instant::now();
        self.prune(now);
        self.events.push_back(TimestampedEvent {
            timestamp: now,
            event: ReplayEvent::Resize { cols, rows },
        });
    }

    pub fn push_app_switch(&mut self, shortname: String) {
        self.current_shortname = shortname.clone();
        let now = Instant::now();
        self.prune(now);
        self.events.push_back(TimestampedEvent {
            timestamp: now,
            event: ReplayEvent::AppSwitch { shortname },
        });
    }

    fn prune(&mut self, now: Instant) {
        let cutoff = now - REPLAY_DURATION;

        while let Some(front) = self.events.front() {
            if front.timestamp >= cutoff {
                break;
            }
            let event = self.events.pop_front().unwrap();
            match &event.event {
                ReplayEvent::Output(data) => {
                    self.vt.feed_str(&String::from_utf8_lossy(data));
                }
                ReplayEvent::Resize { cols, rows } => {
                    self.vt.resize((*cols as usize).max(1), (*rows as usize).max(1));
                    self.initial_cols = *cols;
                    self.initial_rows = *rows;
                }
                ReplayEvent::AppSwitch { shortname } => {
                    self.initial_shortname = shortname.clone();
                }
            }
            self.vt_timestamp = Some(event.timestamp);
        }
    }

    pub fn serialize_asciicast(&self) -> Vec<u8> {
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

        // Find valid terminal size: prefer initial, fall back to first resize event, then current
        let (cols, rows) = if self.initial_cols > 0 && self.initial_rows > 0 {
            (self.initial_cols, self.initial_rows)
        } else {
            self.events
                .iter()
                .find_map(|e| match &e.event {
                    ReplayEvent::Resize { cols, rows } if *cols > 0 && *rows > 0 => {
                        Some((*cols, *rows))
                    }
                    _ => None,
                })
                .unwrap_or((self.current_cols.max(80), self.current_rows.max(24)))
        };

        let header = AsciicastHeader {
            version: 3,
            term: AsciicastTerm {
                cols,
                rows,
                term_type: self.term_type.as_deref(),
            },
            timestamp,
            title: format!("Terminal Games | {}", self.initial_shortname),
            command: format!("ssh -C terminal-games.fly.dev -t {}", self.initial_shortname),
            tags: vec!["terminal-games", &self.initial_shortname],
        };
        serde_json::to_writer(&mut output, &header).unwrap();
        output.push(b'\n');

        let mut prev_timestamp = self.vt_timestamp;
        let vt_output = self.vt.dump();
        if !vt_output.is_empty() {
            write_event(&mut output, 0.0, "o", &vt_output);
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
                    write_event(&mut output, interval_secs, "o", &text);
                }
                ReplayEvent::Resize { cols, rows } => {
                    write_event(&mut output, interval_secs, "r", &format!("{}x{}", cols, rows));
                }
                ReplayEvent::AppSwitch { shortname } => {
                    write_event(&mut output, interval_secs, "m", &format!("app:{}", shortname));
                }
            }
        }

        output
    }
}

fn write_event(output: &mut Vec<u8>, interval_secs: f64, code: &str, data: &str) {
    let interval_secs = (interval_secs * 1000.0).round() / 1000.0;
    let event: (f64, &str, &str) = (interval_secs, code, data);
    serde_json::to_writer(&mut *output, &event).unwrap();
    output.push(b'\n');
}
