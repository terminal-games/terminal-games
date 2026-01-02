// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{cell::RefCell, pin::Pin, task::Poll, time::Duration};

use tokio::io::{AsyncRead, AsyncWrite};

pub struct Conn {
    fd: i32,
    sleep: RefCell<Option<Pin<Box<tokio::time::Sleep>>>>,
}

impl Conn {
    pub fn dial(address: &str, tls: bool) -> std::io::Result<Self> {
        let mode = if tls { 1 } else { 0 };
        let result = unsafe { crate::internal::dial(address.as_ptr(), address.len() as u32, mode) };
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
        let result =
            unsafe { crate::internal::conn_write(self.fd, buf.as_ptr(), buf.len() as u32) };
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
            crate::internal::conn_read(
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
