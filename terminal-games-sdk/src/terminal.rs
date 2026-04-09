// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::io::{self, Write};

use ratatui::{
    backend::WindowSize,
    layout::{Position, Size},
};

use crate::ansi_backend::{AnsiBackend, EmulatorBackend};
pub use crate::app::{TerminalColorMode, TerminalInfo};

pub struct TerminalReader {}

impl TerminalReader {
    pub fn read(&mut self) -> io::Result<Option<terminput::Event>> {
        let mut buf = [0u8; 4096];
        let written = unsafe {
            crate::internal::terminal_read(buf.as_mut_ptr() as *mut u8, buf.len() as u32)
        };
        if written < 0 {
            return Err(crate::host_call_error(
                "terminal_games.terminal_read_v1",
                written,
            ));
        }

        match terminput::Event::parse_from(&buf[..written as usize]) {
            Ok(Some(event)) => Ok(Some(event)),
            Ok(None) => Ok(None),
            Err(error) => Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        }
    }
}

impl Iterator for TerminalReader {
    type Item = io::Result<terminput::Event>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

fn terminal_size() -> io::Result<(u16, u16)> {
    let mut width: u16 = 0;
    let mut height: u16 = 0;
    let result = unsafe { crate::internal::terminal_size(&mut width, &mut height) };
    if result < 0 {
        return Err(crate::host_call_error(
            "terminal_games.terminal_size_v1",
            result,
        ));
    }
    Ok((width, height))
}

pub struct TerminalGamesTerminalEmulator {}

impl EmulatorBackend for TerminalGamesTerminalEmulator {
    fn get_cursor_position(&mut self) -> io::Result<Position> {
        let mut x: u16 = 0;
        let mut y: u16 = 0;
        let result = unsafe { crate::internal::terminal_cursor(&mut x, &mut y) };
        if result < 0 {
            return Err(crate::host_call_error(
                "terminal_games.terminal_cursor_v1",
                result,
            ));
        }
        Ok(Position { x, y })
    }

    fn size(&self) -> io::Result<Size> {
        let (width, height) = terminal_size()?;
        Ok(Size { width, height })
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        let (width, height) = terminal_size()?;
        Ok(WindowSize {
            columns_rows: Size { width, height },
            pixels: Size::new(0, 0),
        })
    }
}

pub struct TerminalGamesBackend {}

impl TerminalGamesBackend {
    pub fn new<W>(writer: W) -> AnsiBackend<W, TerminalGamesTerminalEmulator>
    where
        W: Write,
    {
        AnsiBackend::new(writer, TerminalGamesTerminalEmulator {})
    }
}

pub fn info() -> io::Result<TerminalInfo> {
    crate::app::terminal_info()
}
