// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

/// Asks the host to switch to another app identified by its shortname.
/// The current guest should exit after calling this function so the host can
/// start the next app.
pub fn change_app(shortname: impl AsRef<str>) -> Result<(), std::io::Error> {
    let shortname = shortname.as_ref();
    let result = unsafe { crate::internal::change_app(shortname.as_ptr(), shortname.len() as u32) };
    match result {
        CHANGE_APP_ERR_RATE_LIMITED => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "please wait 10 seconds between app changes",
            ));
        }
        r if r < 0 => {
            return Err(crate::host_call_error("terminal_games.change_app_v1", r));
        }
        _ => {}
    }

    Ok(())
}

const CHANGE_APP_ERR_RATE_LIMITED: i32 = -2;
const NEXT_APP_READY_ERR_UNKNOWN_SHORTNAME: i32 = -1;
const NEXT_APP_READY_ERR_OTHER: i32 = -2;

#[derive(Debug)]
pub enum NextAppReadyError {
    UnknownShortname,
    VersionMismatch,
    Other(i32),
}

impl std::fmt::Display for NextAppReadyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownShortname => write!(f, "unknown app shortname"),
            Self::VersionMismatch => {
                write!(
                    f,
                    "terminal-games host version mismatch for terminal_games.next_app_ready_v1"
                )
            }
            Self::Other(code) => write!(f, "next_app_ready failed with code {}", code),
        }
    }
}

impl std::error::Error for NextAppReadyError {}

/// Reports whether the next app requested via [`change_app`] is fully warmed in
/// the host's module cache and ready to switch to.
///
/// This can be called in a loop by the current guest before exiting to ensure
/// the next app will start quickly once the host performs the switch. This is
/// useful for building a loading UI
pub fn next_app_ready() -> Result<bool, NextAppReadyError> {
    let result = unsafe { crate::internal::next_app_ready() };
    match result {
        NEXT_APP_READY_ERR_UNKNOWN_SHORTNAME => Err(NextAppReadyError::UnknownShortname),
        crate::HOST_API_VERSION_MISMATCH => Err(NextAppReadyError::VersionMismatch),
        NEXT_APP_READY_ERR_OTHER => Err(NextAppReadyError::Other(result)),
        r if r < 0 => Err(NextAppReadyError::Other(r)),
        r => Ok(r > 0),
    }
}

/// Polls whether a graceful shutdown has been triggered by the host.
///
/// Returns `true` if a graceful shutdown has been initiated, `false` otherwise.
/// This can be called periodically by the guest to check if it should begin
/// shutting down gracefully (e.g., saving state, closing connections, etc.)
/// before the host forces a hard shutdown.
pub fn graceful_shutdown_poll() -> std::io::Result<bool> {
    crate::host_call_bool("terminal_games.graceful_shutdown_poll_v1", unsafe {
        crate::internal::graceful_shutdown_poll()
    })
}

/// Polls whether a newer uploaded version of the current app is available.
pub fn is_new_version_available() -> std::io::Result<bool> {
    crate::host_call_bool("terminal_games.new_version_available_poll_v1", unsafe {
        crate::internal::new_version_available_poll()
    })
}

/// Information about the current session's network connection to the host.
#[derive(Debug, Clone)]
pub struct NetworkInfo {
    /// Receive rate in bytes per second.
    pub bytes_per_sec_in: f64,
    /// Send rate in bytes per second.
    pub bytes_per_sec_out: f64,
    /// When the connection was last throttled, or `None` if never.
    pub last_throttled: Option<std::time::SystemTime>,
    /// TCP RTT latency in milliseconds, or -1 if unavailable.
    pub latency_ms: i32,
}

/// Fetches network information from the host (throughput, throttling, RTT).
///
/// Returns an error if the host call fails.
pub fn network_info() -> std::io::Result<NetworkInfo> {
    let mut bytes_per_sec_in = 0.0f64;
    let mut bytes_per_sec_out = 0.0f64;
    let mut last_throttled_ms = 0i64;
    let mut latency_ms = 0i32;
    let result = unsafe {
        crate::internal::network_info(
            &mut bytes_per_sec_in,
            &mut bytes_per_sec_out,
            &mut last_throttled_ms,
            &mut latency_ms,
        )
    };
    if result < 0 {
        return Err(crate::host_call_error(
            "terminal_games.network_info_v1",
            result,
        ));
    }
    let last_throttled = (last_throttled_ms > 0).then(|| {
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(last_throttled_ms as u64)
    });

    Ok(NetworkInfo {
        bytes_per_sec_in,
        bytes_per_sec_out,
        last_throttled,
        latency_ms,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalColorMode {
    Color16,
    Color256,
    TrueColor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalInfo {
    pub color_mode: TerminalColorMode,
    pub background_rgb: Option<(u8, u8, u8)>,
    pub dark_background: Option<bool>,
}

pub fn terminal_info() -> std::io::Result<TerminalInfo> {
    let mut color_mode = 0u8;
    let mut has_bg = 0i32;
    let mut bg_r = 0u8;
    let mut bg_g = 0u8;
    let mut bg_b = 0u8;
    let mut has_dark = 0i32;
    let mut dark = 0i32;
    let result = unsafe {
        crate::internal::terminal_info(
            &mut color_mode,
            &mut has_bg,
            &mut bg_r,
            &mut bg_g,
            &mut bg_b,
            &mut has_dark,
            &mut dark,
        )
    };
    if result < 0 {
        return Err(crate::host_call_error(
            "terminal_games.terminal_info_v1",
            result,
        ));
    }

    let color_mode = match color_mode {
        2 => TerminalColorMode::TrueColor,
        1 => TerminalColorMode::Color256,
        _ => TerminalColorMode::Color16,
    };
    let background_rgb = (has_bg > 0).then_some((bg_r, bg_g, bg_b));
    let dark_background = (has_dark > 0).then_some(dark > 0);

    Ok(TerminalInfo {
        color_mode,
        background_rgb,
        dark_background,
    })
}
