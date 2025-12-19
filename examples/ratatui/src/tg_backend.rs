use std::io::{self, Write};

use ratatui::{
    backend::{ClearType, WindowSize},
    buffer::Cell,
    layout::{Position, Size},
    prelude::Backend,
    style::{Color, Modifier},
};

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct TerminalGamesBackend<W: Write> {
    /// The writer used to send commands to the terminal.
    writer: W,
}

impl<W> TerminalGamesBackend<W>
where
    W: Write,
{
    pub const fn new(writer: W) -> Self {
        Self { writer }
    }
}

fn move_to<W: Write>(writer: &mut W, line: u16, column: u16) -> io::Result<()> {
    write!(writer, "\x1b[{};{}H", line, column)
}

impl<W> Backend for TerminalGamesBackend<W>
where
    W: Write,
{
    fn draw<'a, I>(&mut self, content: I) -> io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        todo!()
    }

    fn hide_cursor(&mut self) -> io::Result<()> {
        todo!()
    }

    fn show_cursor(&mut self) -> io::Result<()> {
        todo!()
    }

    fn get_cursor_position(&mut self) -> io::Result<Position> {
        todo!()
    }

    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        todo!()
    }

    fn clear(&mut self) -> io::Result<()> {
        todo!()
    }

    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
        todo!()
    }

    fn append_lines(&mut self, n: u16) -> io::Result<()> {
        todo!()
    }

    fn size(&self) -> io::Result<Size> {
        todo!()
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        todo!()
    }

    fn flush(&mut self) -> io::Result<()> {
        todo!()
    }
}
