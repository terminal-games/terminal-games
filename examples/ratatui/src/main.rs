use std::{io::Write, mem::MaybeUninit};

use ratatui::{Terminal, widgets::Paragraph};

use crate::ansi_backend::AnsiBackend;

mod ansi_backend;
mod tg_backend;

mod internal {
    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        pub(crate) fn terminal_size(width_ptr: *mut u16, height_ptr: *mut u16);
        pub(crate) fn terminal_read(address_ptr: *mut u8, address_len: u32) -> i32;
    }
}

pub fn terminal_size() -> (u16, u16) {
    let mut width: u16 = 0;
    let mut height: u16 = 0;
    unsafe { internal::terminal_size(&mut width, &mut height) };
    (width, height)
}

pub fn terminal_read_process() -> std::io::Result<Option<terminput::Event>> {
    let mut buf = [MaybeUninit::<u8>::uninit(); 64];
    let written = unsafe { internal::terminal_read(buf.as_mut_ptr() as *mut u8, 64) };
    if written < 0 {
        // return Err();
        return Ok(None);
    }

    let written = written as usize;
    if written > 64 {
        // return Err(-1); // defensive check
        return Ok(None);
    }
    if written == 0 {
        return Ok(None);
    }

    // SAFETY:
    // - terminal_read initialized the first `written` bytes
    // - we only access that prefix
    let initialized_bytes =
        unsafe { core::slice::from_raw_parts(buf.as_ptr() as *const u8, written) };

    terminput::Event::parse_from(initialized_bytes)
}

fn main() -> std::io::Result<()> {
    let mut terminal = Terminal::new(AnsiBackend::new(std::io::stdout()))?;
    terminal.clear()?;
    std::io::stdout().write(b"\x1b[?1003h")?;

    let mut counter = 1;
    let mut last_event = None;
    loop {
        terminal.draw(|frame| {
            let area = frame.area();
            frame.render_widget(
                Paragraph::new(format!("Hello World! {} {:?}\n", counter, last_event)),
                area,
            );
        })?;
        if counter > 10000 {
            break;
        }
        counter += 1;
        match terminal_read_process() {
            Ok(Some(event)) => {
                if let Some(key_event) = event.as_key() {
                    match key_event {
                        terminput::key!(terminput::KeyCode::Char('q')) => break,
                        _ => {}
                    }
                }

                last_event = Some(event);
            }
            Ok(None) => {}
            Err(_) => {}
        }
    }
    std::io::stdout().write(b"\x1b[?1003l")?;
    Ok(())
}
