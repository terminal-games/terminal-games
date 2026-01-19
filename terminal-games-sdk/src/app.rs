// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

/// Asks the host to switch to another app identified by its shortname.
/// The current guest should exit after calling this function so the host can
/// start the next app.
pub fn change_app(shortname: impl AsRef<str>) -> Result<(), std::io::Error> {
    let shortname = shortname.as_ref();
    let result = unsafe { crate::internal::change_app(shortname.as_ptr(), shortname.len() as u32) };
    if result < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("failed to change app, got {}", result),
        ));
    }

    Ok(())
}

/// Reports whether the next app requested via [`change_app`] is fully warmed in
/// the host's module cache and ready to switch to.
///
/// This can be called in a loop by the current guest before exiting to ensure
/// the next app will start quickly once the host performs the switch. This is
/// useful for building a loading UI
pub fn next_app_ready() -> bool {
    return unsafe { crate::internal::next_app_ready() } > 0;
}

/// Polls whether a graceful shutdown has been triggered by the host.
///
/// Returns `true` if a graceful shutdown has been initiated, `false` otherwise.
/// This can be called periodically by the guest to check if it should begin
/// shutting down gracefully (e.g., saving state, closing connections, etc.)
/// before the host forces a hard shutdown.
pub fn graceful_shutdown_poll() -> bool {
    return unsafe { crate::internal::graceful_shutdown_poll() } > 0;
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
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "network_info host call failed",
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
