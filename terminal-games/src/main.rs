// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod mesh;
mod ssh;
mod status_bar;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use wasmtime_wasi::{ResourceTable, p1::WasiP1Ctx};

use crate::{
    mesh::{EnvDiscovery, Mesh, PeerId, PeerMessageApp},
    ssh::{ModuleCache, Terminal},
};

pub enum Stream {
    Tcp(tokio::net::TcpStream),
    Tls(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
}

impl Stream {
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(stream) => {
                use tokio::io::AsyncWriteExt;
                stream.write(buf).await
            }
            Stream::Tls(stream) => {
                use tokio::io::AsyncWriteExt;
                stream.write(buf).await
            }
        }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(stream) => {
                use tokio::io::AsyncReadExt;
                stream.read(buf).await
            }
            Stream::Tls(stream) => {
                use tokio::io::AsyncReadExt;
                stream.read(buf).await
            }
        }
    }

    pub fn try_read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(stream) => stream.try_read(buf),
            Stream::Tls(stream) => {
                use std::pin::Pin;
                use std::task::{Context, Poll, Waker};
                use tokio::io::AsyncRead;

                struct NoOpWaker;
                impl std::task::Wake for NoOpWaker {
                    fn wake(self: Arc<Self>) {}
                }
                let waker = Waker::from(Arc::new(NoOpWaker));
                let mut cx = Context::from_waker(&waker);

                let mut read_buf = tokio::io::ReadBuf::new(buf);

                match Pin::new(stream).poll_read(&mut cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let filled = read_buf.filled().len();
                        if filled == 0 {
                            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
                        } else {
                            Ok(filled)
                        }
                    }
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Pending => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
                }
            }
        }
    }
}

pub struct ComponentRunStates {
    pub wasi_ctx: WasiP1Ctx,
    pub resource_table: ResourceTable,
    streams: Vec<Stream>,
    limits: MyLimiter,
    terminal: Arc<Mutex<Terminal>>,
    next_app_shortname: Arc<Mutex<Option<String>>>,
    input_receiver: tokio::sync::mpsc::Receiver<smallvec::SmallVec<[u8; 16]>>,
    module_cache: Arc<Mutex<ModuleCache>>,
    peer_rx: tokio::sync::mpsc::Receiver<PeerMessageApp>,
    peer_tx: tokio::sync::mpsc::Sender<(Vec<PeerId>, Vec<u8>)>,
    mesh: Mesh,
}

struct MyLimiter {
    total: usize,
}

impl Default for MyLimiter {
    fn default() -> Self {
        MyLimiter { total: 0 }
    }
}

impl wasmtime::ResourceLimiter for MyLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> Result<bool> {
        tracing::trace!(current, desired, maximum, "memory growing");
        self.total -= current;
        self.total += desired;
        if self.total >= 32 * 1024 * 1024 {
            tracing::trace!(total = self.total, "rejected memory grow");
            return Ok(false);
        }

        return Ok(true);
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> Result<bool> {
        tracing::trace!(current, desired, maximum, "table growing");
        return Ok(true);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_log::LogTracer::init().expect("Failed to set logger");

    let subscriber = tracing_subscriber::fmt()
        // .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let mesh = Mesh::new(Arc::new(EnvDiscovery::new()));
    mesh.start_discovery().await.unwrap();
    mesh.serve().await.unwrap();

    let mut server = ssh::AppServer::new(mesh.clone()).await?;
    server.run().await.expect("Failed running server");
    mesh.graceful_shutdown().await;

    Ok(())
}
