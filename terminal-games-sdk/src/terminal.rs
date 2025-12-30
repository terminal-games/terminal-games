use std::io::{self, Write};

use ratatui::{
    backend::WindowSize,
    layout::{Position, Size},
};

use crate::ansi_backend::{AnsiBackend, EmulatorBackend};

pub struct TerminalReader {}

impl Iterator for TerminalReader {
    type Item = terminput::Event;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0u8; 64];
        let written = unsafe { crate::internal::terminal_read(buf.as_mut_ptr() as *mut u8, 64) };
        if written < 0 {
            return None;
        }

        match terminput::Event::parse_from(&buf[..written as usize]) {
            Ok(Some(event)) => return Some(event),
            Ok(None) => return None,
            Err(_) => return None,
        }
    }
}

fn terminal_size() -> (u16, u16) {
    let mut width: u16 = 0;
    let mut height: u16 = 0;
    unsafe { crate::internal::terminal_size(&mut width, &mut height) };
    (width, height)
}

pub struct TerminalGamesTerminalEmulator {}

impl EmulatorBackend for TerminalGamesTerminalEmulator {
    fn get_cursor_position(&mut self) -> io::Result<Position> {
        let mut x: u16 = 0;
        let mut y: u16 = 0;
        unsafe { crate::internal::terminal_cursor(&mut x, &mut y) };
        Ok(Position { x, y })
    }

    fn size(&self) -> io::Result<Size> {
        let (width, height) = terminal_size();
        Ok(Size { width, height })
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        let (width, height) = terminal_size();
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
