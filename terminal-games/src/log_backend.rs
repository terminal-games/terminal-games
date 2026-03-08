// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn from_u8(v: u8) -> Option<Self> {
        [Self::Trace, Self::Debug, Self::Info, Self::Warn, Self::Error].get(v as usize).copied()
    }
    pub fn as_str(self) -> &'static str {
        ["trace", "debug", "info", "warn", "error"][self as usize]
    }
}

#[derive(Clone, Debug)]
pub struct GuestLogRecord {
    pub level: LogLevel,
    pub message: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub module_path: Option<String>,
    pub attributes: Vec<(String, Value)>,
}

impl GuestLogRecord {
    pub fn new(level: LogLevel, message: impl Into<String>) -> Self {
        Self {
            level,
            message: message.into(),
            file: None,
            line: None,
            module_path: None,
            attributes: Vec::new(),
        }
    }
}

pub fn parse_guest_log_message(message: &str) -> GuestLogRecord {
    let trimmed = message.trim_end_matches(['\r', '\n']);
    let Ok(Value::Object(mut obj)) = serde_json::from_str::<Value>(trimmed) else {
        return GuestLogRecord::new(LogLevel::Info, trimmed);
    };
    let level = obj
        .remove("level")
        .or_else(|| obj.remove("lvl"))
        .as_ref()
        .and_then(|v| v.as_str())
        .and_then(|s| {
            if s.eq_ignore_ascii_case("trace") {
                Some(LogLevel::Trace)
            } else if s.eq_ignore_ascii_case("debug") {
                Some(LogLevel::Debug)
            } else if s.eq_ignore_ascii_case("info") {
                Some(LogLevel::Info)
            } else if s.eq_ignore_ascii_case("warn") || s.eq_ignore_ascii_case("warning") {
                Some(LogLevel::Warn)
            } else if s.eq_ignore_ascii_case("error") {
                Some(LogLevel::Error)
            } else {
                None
            }
        })
        .unwrap_or(LogLevel::Info);
    let message = obj
        .remove("message")
        .or_else(|| obj.remove("msg"))
        .as_ref()
        .map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))
        .unwrap_or_else(|| trimmed.to_owned());
    let file = obj.remove("file").as_ref().map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()));
    let line = obj.remove("line").as_ref().and_then(|v| match v {
        Value::Number(n) => n.as_u64().and_then(|n| u32::try_from(n).ok()),
        Value::String(s) => s.parse().ok(),
        _ => None,
    });
    let module_path = obj.remove("module_path").as_ref().map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()));
    let _ = obj.remove("target");
    GuestLogRecord {
        level,
        message,
        file,
        line,
        module_path,
        attributes: obj.into_iter().collect(),
    }
}

pub trait GuestLogBackend: Send + Sync {
    fn log(&self, shortname: &str, user_id: Option<u64>, record: &GuestLogRecord);
}

pub struct NoopLogBackend;
impl GuestLogBackend for NoopLogBackend {
    fn log(&self, _: &str, _: Option<u64>, _: &GuestLogRecord) {}
}
