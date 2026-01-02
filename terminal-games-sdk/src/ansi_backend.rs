// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! This module provides a pure ANSI backend implementation for the [`Backend`] trait.
//! It uses standard ANSI/VT100 escape sequences to interact with the terminal.

use std::{
    fmt,
    io::{self, Write},
};

use ratatui::{
    backend::{Backend, ClearType, WindowSize},
    buffer::Cell,
    layout::{Position, Size},
    style::{Color, Modifier},
};

/// Functions in [`Backend`] which require terminal emulator API commands and
/// cannot be done purely with ANSI escapes in [`AnsiBackend`]
pub trait EmulatorBackend {
    /// See [`Backend::get_cursor_position`]
    fn get_cursor_position(&mut self) -> io::Result<Position>;
    /// See [`Backend::size`]
    fn size(&self) -> io::Result<Size>;
    /// See [`Backend::window_size`]
    fn window_size(&mut self) -> io::Result<WindowSize>;
}

/// A [`Backend`] implementation that uses pure ANSI escape sequences.
///
/// The `AnsiBackend` struct is a wrapper around a writer implementing [`Write`], which is used
/// to send ANSI escape codes to the terminal. It provides methods for drawing content,
/// manipulating the cursor, and clearing the terminal screen.
///
/// # Example
///
/// ```rust,no_run
/// use std::io::stdout;
/// use ratatui::{backend::AnsiBackend, Terminal};
///
/// let backend = AnsiBackend::new(stdout());
/// let mut terminal = Terminal::new(backend)?;
///
/// terminal.clear()?;
/// terminal.draw(|frame| {
///     // -- snip --
/// })?;
/// # std::io::Result::Ok(())
/// ```
#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct AnsiBackend<W, E>
where
    W: Write,
    E: EmulatorBackend,
{
    writer: W,
    emulator: E,
}

impl<W, E> AnsiBackend<W, E>
where
    W: Write,
    E: EmulatorBackend,
{
    /// Creates a new ANSI backend with the given writer.
    pub const fn new(writer: W, emulator: E) -> Self {
        Self { writer, emulator }
    }
}

impl<W, E> Write for AnsiBackend<W, E>
where
    W: Write,
    E: EmulatorBackend,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W, E> Backend for AnsiBackend<W, E>
where
    W: Write,
    E: EmulatorBackend,
{
    fn clear(&mut self) -> io::Result<()> {
        self.clear_region(ClearType::All)
    }

    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
        match clear_type {
            ClearType::All => write!(self.writer, "\x1b[2J")?,
            ClearType::AfterCursor => write!(self.writer, "\x1b[0J")?,
            ClearType::BeforeCursor => write!(self.writer, "\x1b[1J")?,
            ClearType::CurrentLine => write!(self.writer, "\x1b[2K")?,
            ClearType::UntilNewLine => write!(self.writer, "\x1b[K")?,
        };
        self.writer.flush()
    }

    fn append_lines(&mut self, n: u16) -> io::Result<()> {
        for _ in 0..n {
            writeln!(self.writer)?;
        }
        self.writer.flush()
    }

    fn hide_cursor(&mut self) -> io::Result<()> {
        write!(self.writer, "\x1b[?25l")?;
        self.writer.flush()
    }

    fn show_cursor(&mut self) -> io::Result<()> {
        write!(self.writer, "\x1b[?25h")?;
        self.writer.flush()
    }

    fn get_cursor_position(&mut self) -> io::Result<Position> {
        // ANSI escape sequence to query cursor position: ESC[6n
        // Terminal responds with: ESC[{row};{col}R
        // This is difficult to implement reliably without terminal-specific libraries because
        // it requires reading from stdin, which may not be available or synchronized. Also in
        // some cases (like SSH), this would take a whole network round trip but this function
        // is blocking
        self.emulator.get_cursor_position()
    }

    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        let Position { x, y } = position.into();
        write!(self.writer, "\x1b[{};{}H", y + 1, x + 1)?;
        self.writer.flush()
    }

    fn draw<'a, I>(&mut self, content: I) -> io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        use std::fmt::Write;

        let mut string = String::with_capacity(content.size_hint().0 * 3);
        let mut fg = Color::Reset;
        let mut bg = Color::Reset;
        let mut modifier = Modifier::empty();
        let mut last_pos: Option<Position> = None;

        for (x, y, cell) in content {
            // Move the cursor if the previous location was not (x - 1, y)
            if !matches!(last_pos, Some(p) if x == p.x + 1 && y == p.y) {
                write!(string, "\x1b[{};{}H", y + 1, x + 1).unwrap();
            }
            last_pos = Some(Position { x, y });

            if cell.modifier != modifier {
                write!(
                    string,
                    "{}",
                    ModifierDiff {
                        from: modifier,
                        to: cell.modifier
                    }
                )
                .unwrap();
                modifier = cell.modifier;
            }

            if cell.fg != fg {
                write!(string, "{}", Fg(cell.fg)).unwrap();
                fg = cell.fg;
            }

            if cell.bg != bg {
                write!(string, "{}", Bg(cell.bg)).unwrap();
                bg = cell.bg;
            }

            string.push_str(cell.symbol());
        }

        write!(self.writer, "{}\x1b[39m\x1b[49m\x1b[0m", string)
    }

    fn size(&self) -> io::Result<Size> {
        self.emulator.size()
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        self.emulator.window_size()
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    // #[cfg(feature = "scrolling-regions")]
    // fn scroll_region_up(&mut self, region: std::ops::Range<u16>, amount: u16) -> io::Result<()> {
    //     write!(
    //         self.writer,
    //         "\x1b[{};{}r\x1b[{}S\x1b[r",
    //         region.start.saturating_add(1),
    //         region.end,
    //         amount
    //     )?;
    //     self.writer.flush()
    // }

    // #[cfg(feature = "scrolling-regions")]
    // fn scroll_region_down(&mut self, region: std::ops::Range<u16>, amount: u16) -> io::Result<()> {
    //     write!(
    //         self.writer,
    //         "\x1b[{};{}r\x1b[{}T\x1b[r",
    //         region.start.saturating_add(1),
    //         region.end,
    //         amount
    //     )?;
    //     self.writer.flush()
    // }
}

struct Fg(Color);

struct Bg(Color);

impl fmt::Display for Fg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Color::Reset => write!(f, "\x1b[39m"),
            Color::Black => write!(f, "\x1b[30m"),
            Color::Red => write!(f, "\x1b[31m"),
            Color::Green => write!(f, "\x1b[32m"),
            Color::Yellow => write!(f, "\x1b[33m"),
            Color::Blue => write!(f, "\x1b[34m"),
            Color::Magenta => write!(f, "\x1b[35m"),
            Color::Cyan => write!(f, "\x1b[36m"),
            Color::Gray => write!(f, "\x1b[37m"),
            Color::DarkGray => write!(f, "\x1b[90m"),
            Color::LightRed => write!(f, "\x1b[91m"),
            Color::LightGreen => write!(f, "\x1b[92m"),
            Color::LightYellow => write!(f, "\x1b[93m"),
            Color::LightBlue => write!(f, "\x1b[94m"),
            Color::LightMagenta => write!(f, "\x1b[95m"),
            Color::LightCyan => write!(f, "\x1b[96m"),
            Color::White => write!(f, "\x1b[97m"),
            Color::Indexed(i) => write!(f, "\x1b[38;5;{}m", i),
            Color::Rgb(r, g, b) => write!(f, "\x1b[38;2;{};{};{}m", r, g, b),
        }
    }
}

impl fmt::Display for Bg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Color::Reset => write!(f, "\x1b[49m"),
            Color::Black => write!(f, "\x1b[40m"),
            Color::Red => write!(f, "\x1b[41m"),
            Color::Green => write!(f, "\x1b[42m"),
            Color::Yellow => write!(f, "\x1b[43m"),
            Color::Blue => write!(f, "\x1b[44m"),
            Color::Magenta => write!(f, "\x1b[45m"),
            Color::Cyan => write!(f, "\x1b[46m"),
            Color::Gray => write!(f, "\x1b[47m"),
            Color::DarkGray => write!(f, "\x1b[100m"),
            Color::LightRed => write!(f, "\x1b[101m"),
            Color::LightGreen => write!(f, "\x1b[102m"),
            Color::LightYellow => write!(f, "\x1b[103m"),
            Color::LightBlue => write!(f, "\x1b[104m"),
            Color::LightMagenta => write!(f, "\x1b[105m"),
            Color::LightCyan => write!(f, "\x1b[106m"),
            Color::White => write!(f, "\x1b[107m"),
            Color::Indexed(i) => write!(f, "\x1b[48;5;{}m", i),
            Color::Rgb(r, g, b) => write!(f, "\x1b[48;2;{};{};{}m", r, g, b),
        }
    }
}

/// The `ModifierDiff` struct calculates the difference between two `Modifier` values
/// and generates the appropriate ANSI escape sequences.
struct ModifierDiff {
    from: Modifier,
    to: Modifier,
}

impl fmt::Display for ModifierDiff {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let remove = self.from - self.to;

        if remove.contains(Modifier::REVERSED) {
            write!(f, "\x1b[27m")?;
        }
        if remove.contains(Modifier::BOLD) {
            write!(f, "\x1b[22m")?;
            if self.to.contains(Modifier::DIM) {
                write!(f, "\x1b[2m")?;
            }
        }
        if remove.contains(Modifier::ITALIC) {
            write!(f, "\x1b[23m")?;
        }
        if remove.contains(Modifier::UNDERLINED) {
            write!(f, "\x1b[24m")?;
        }
        if remove.contains(Modifier::DIM) {
            write!(f, "\x1b[22m")?;
            if self.to.contains(Modifier::BOLD) {
                write!(f, "\x1b[1m")?;
            }
        }
        if remove.contains(Modifier::CROSSED_OUT) {
            write!(f, "\x1b[29m")?;
        }
        if remove.contains(Modifier::SLOW_BLINK) || remove.contains(Modifier::RAPID_BLINK) {
            write!(f, "\x1b[25m")?;
        }

        let add = self.to - self.from;

        if add.contains(Modifier::REVERSED) {
            write!(f, "\x1b[7m")?;
        }
        if add.contains(Modifier::BOLD) {
            write!(f, "\x1b[1m")?;
        }
        if add.contains(Modifier::ITALIC) {
            write!(f, "\x1b[3m")?;
        }
        if add.contains(Modifier::UNDERLINED) {
            write!(f, "\x1b[4m")?;
        }
        if add.contains(Modifier::DIM) {
            write!(f, "\x1b[2m")?;
        }
        if add.contains(Modifier::CROSSED_OUT) {
            write!(f, "\x1b[9m")?;
        }
        if add.contains(Modifier::SLOW_BLINK) {
            write!(f, "\x1b[5m")?;
        }
        if add.contains(Modifier::RAPID_BLINK) {
            write!(f, "\x1b[6m")?;
        }

        Ok(())
    }
}
