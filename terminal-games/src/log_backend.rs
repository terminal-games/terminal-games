// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
}

pub trait GuestLogBackend: Send + Sync {
    fn log(&self, shortname: &str, user_id: Option<u64>, level: LogLevel, message: &str);
}

pub struct NoopLogBackend;

impl GuestLogBackend for NoopLogBackend {
    fn log(&self, _shortname: &str, _user_id: Option<u64>, _level: LogLevel, _message: &str) {}
}
