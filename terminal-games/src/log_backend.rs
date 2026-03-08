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
        match v {
            0 => Some(Self::Trace),
            1 => Some(Self::Debug),
            2 => Some(Self::Info),
            3 => Some(Self::Warn),
            4 => Some(Self::Error),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
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
    let Ok(Value::Object(mut object)) = serde_json::from_str::<Value>(trimmed) else {
        return GuestLogRecord::new(LogLevel::Info, trimmed);
    };

    let level = object
        .remove("level")
        .or_else(|| object.remove("lvl"))
        .as_ref()
        .and_then(parse_level)
        .unwrap_or(LogLevel::Info);
    let message = object
        .remove("message")
        .or_else(|| object.remove("msg"))
        .as_ref()
        .map(value_to_string)
        .unwrap_or_else(|| trimmed.to_owned());
    let file = object.remove("file").as_ref().map(value_to_string);
    let line = object.remove("line").as_ref().and_then(parse_line);
    let module_path = object.remove("module_path").as_ref().map(value_to_string);
    let _ = object.remove("target");
    let attributes = object.into_iter().collect();

    GuestLogRecord {
        level,
        message,
        file,
        line,
        module_path,
        attributes,
    }
}

fn parse_level(value: &Value) -> Option<LogLevel> {
    let level = match value {
        Value::String(level) => level,
        _ => return None,
    };
    if level.eq_ignore_ascii_case("trace") {
        Some(LogLevel::Trace)
    } else if level.eq_ignore_ascii_case("debug") {
        Some(LogLevel::Debug)
    } else if level.eq_ignore_ascii_case("info") {
        Some(LogLevel::Info)
    } else if level.eq_ignore_ascii_case("warn") || level.eq_ignore_ascii_case("warning") {
        Some(LogLevel::Warn)
    } else if level.eq_ignore_ascii_case("error") {
        Some(LogLevel::Error)
    } else {
        None
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

fn parse_line(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number.as_u64().and_then(|line| u32::try_from(line).ok()),
        Value::String(line) => line.parse().ok(),
        _ => None,
    }
}

pub trait GuestLogBackend: Send + Sync {
    fn log(&self, shortname: &str, user_id: Option<u64>, record: &GuestLogRecord);
}

pub struct NoopLogBackend;

impl GuestLogBackend for NoopLogBackend {
    fn log(&self, _shortname: &str, _user_id: Option<u64>, _record: &GuestLogRecord) {}
}
