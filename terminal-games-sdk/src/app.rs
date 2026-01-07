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
