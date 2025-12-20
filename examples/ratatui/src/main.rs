use std::{
    cell::RefCell,
    io::Write,
    mem::MaybeUninit,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    time::Duration,
};

use http_body_util::{BodyExt, Empty};
use hyper::{Request, Version, body::Bytes};
use hyper_util::rt::TokioIo;
use ratatui::{Terminal, widgets::Paragraph};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::ansi_backend::AnsiBackend;

mod ansi_backend;
mod tg_backend;

mod internal {
    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        pub(crate) fn terminal_size(width_ptr: *mut u16, height_ptr: *mut u16);
        pub(crate) fn terminal_read(address_ptr: *mut u8, address_len: u32) -> i32;
        pub(crate) fn dial(address_ptr: *const u8, addressLen: u32, mode: u32) -> i32;
        pub(crate) fn conn_write(conn_id: i32, address_ptr: *const u8, addressLen: u32) -> i32;
        pub(crate) fn conn_read(conn_id: i32, address_ptr: *mut u8, addressLen: u32) -> i32;
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
        // return Err(-1);
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

struct TerminalReader {}

impl Iterator for TerminalReader {
    type Item = terminput::Event;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0u8; 64];
        let written = unsafe { internal::terminal_read(buf.as_mut_ptr() as *mut u8, 64) };
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

struct Conn {
    fd: i32,
    sleep: RefCell<Option<Pin<Box<tokio::time::Sleep>>>>,
}

impl Conn {
    fn dial(address: &str, tls: bool) -> std::io::Result<Self> {
        let mode = if tls { 1 } else { 0 };
        let result = unsafe { internal::dial(address.as_ptr(), address.len() as u32, mode) };
        if result < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("dial failed with error code: {}", result),
            ));
        }
        Ok(Self {
            fd: result,
            sleep: RefCell::new(None),
        })
    }
}

impl AsyncWrite for Conn {
    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let result = unsafe { internal::conn_write(self.fd, buf.as_ptr(), buf.len() as u32) };
        if result < 0 {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("conn_write failed with error code: {}", result),
            )));
        }
        Poll::Ready(Ok(result as usize))
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let result = unsafe {
            internal::conn_read(
                self.fd,
                buf.unfilled_mut().as_mut_ptr() as *mut u8,
                buf.remaining() as u32,
            )
        };

        if result < 0 {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("conn_read failed with error code: {}", result),
            )));
        }

        if result == 0 {
            // No data available so use a sleep timer to avoid busy looping
            // This registers the waker with tokio's timer system
            let mut sleep_opt = self.sleep.borrow_mut();
            let sleep = sleep_opt
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(Duration::from_millis(10))));

            if let Poll::Ready(_) = sleep.as_mut().poll(cx) {
                // Timer expired - reset it and immediately poll once to register waker
                let mut new_sleep = Box::pin(tokio::time::sleep(Duration::from_millis(10)));
                let _ = new_sleep.as_mut().poll(cx);
                *sleep_opt = Some(new_sleep);
            }

            Poll::Pending
        } else {
            // Data available, clear the sleep timer since we have data
            self.sleep.borrow_mut().take();

            let bytes_read = result as usize;
            // SAFETY: we initialized `bytes_read` bytes when writing directly
            // to the `buf.unfilled_mut()` pointer in the host
            unsafe { buf.assume_init(bytes_read) };
            buf.advance(bytes_read);
            Poll::Ready(Ok(()))
        }
    }
}

#[derive(Clone)]
// An Executor that uses the tokio runtime.
pub struct TokioExecutor;

// Implement the `hyper::rt::Executor` trait for `TokioExecutor` so that it can be used to spawn
// tasks in the hyper runtime.
// An Executor allows us to manage execution of tasks which can help us improve the efficiency and
// scalability of the server.
impl<F> hyper::rt::Executor<F> for TokioExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn(fut);
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let mut terminal = Terminal::new(AnsiBackend::new(std::io::stdout()))?;
    terminal.clear()?;
    std::io::stdout().write(b"\x1b[?1003h")?;

    let val = Arc::new(AtomicBool::new(false));

    // let (parts, body) = {
    let url = "https://example.com".parse::<hyper::Uri>().unwrap();

    let host = url.host().expect("uri has no host");
    let port = url.port_u16().unwrap_or(443);

    let address = format!("{}:{}", host, port);
    let stream = Conn::dial(&address, true)?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = match hyper::client::conn::http2::handshake::<
        TokioExecutor,
        TokioIo<Conn>,
        Empty<Bytes>,
    >(TokioExecutor, io)
    .await
    {
        Ok(result) => result,
        Err(e) => {
            eprintln!("HTTP/2 handshake failed: {:?}", e);
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!("HTTP/2 handshake failed: {}", e),
            ));
        }
    };
    let val_clone = val.clone();
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {:?}", err);
        }
        val_clone.store(true, Ordering::SeqCst);
    });

    let req = Request::builder()
        .version(Version::HTTP_2)
        .uri(url)
        .body(Empty::<Bytes>::new())
        .unwrap();

    let res = match sender.send_request(req).await {
        Ok(res) => res,
        Err(e) => {
            println!("Failed to send request: {:?}", e);
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!("Failed to send request: {}", e),
            ));
        }
    };
    let (parts, body) = res.into_parts();
    let body = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            println!("Failed to collect body: {:?}", e);
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("Failed to collect body: {}", e),
            ));
        }
    };
    //     (parts, body)
    // };

    let mut terminal_reader = TerminalReader {};

    let start = std::time::Instant::now();
    let mut frame_counter = 1;
    let mut last_event = None;
    'outer: loop {
        let mut event_counter = 0;
        for event in &mut terminal_reader {
            event_counter += 1;
            if let Some(key_event) = event.as_key() {
                match key_event {
                    terminput::key!(terminput::KeyCode::Char('q')) => break 'outer,
                    _ => {}
                }
            }
            last_event = Some(event);
        }

        terminal.draw(|frame| {
            let area = frame.area();
            frame.render_widget(
                Paragraph::new(format!(
                    "Hello World!\ncounter={}\nlast_event={:#?}\nparts={:#?}\nbody={:#?}\nval={:#?}\nfps={}\nevent_counter={}\n",
                    frame_counter,
                    last_event,
                    parts,
                    body,
                    val,
                    frame_counter as f64 / start.elapsed().as_secs_f64(),
                    event_counter,
                )),
                area,
            );
        })?;
        if frame_counter > 100000 {
            break;
        }
        frame_counter += 1;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    std::io::stdout().write(b"\x1b[?1003l")?;
    Ok(())
}
