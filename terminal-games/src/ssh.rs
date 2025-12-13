use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rand_core::OsRng;
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::{ComponentRunStates, MyLimiter};

fn create_status_bar(width: u32, height: u32) -> Vec<u8> {
    let mut bar = Vec::new();
    bar.extend_from_slice(b"\x1b[s");

    bar.extend_from_slice(format!("\x1b[{};1H", height).as_bytes());
    bar.extend_from_slice(b"\x1b[48;5;4m");
    bar.extend_from_slice(b"\x1b[38;5;15m");

    let text = "terminal-games";
    let padding = width.saturating_sub(text.len() as u32);
    bar.extend_from_slice(text.as_bytes());
    bar.extend(std::iter::repeat(b' ').take(padding as usize));

    bar.extend_from_slice(b"\x1b[0m");
    bar.extend_from_slice(b"\x1b[u");
    bar
}

pub struct App {
    input_channel: tokio::sync::mpsc::Sender<Vec<u8>>,
    output_receiver: Option<UnboundedReceiver<Vec<u8>>>,
    dimensions: Arc<Mutex<(u32, u32)>>,
}

#[derive(Clone)]
pub(crate) struct AppServer {
    id: usize,
    engine: wasmtime::Engine,
    module: wasmtime::Module,
    linker: Arc<Mutex<wasmtime::Linker<ComponentRunStates>>>,
}

impl AppServer {
    pub async fn new() -> anyhow::Result<Self> {
        tracing::info!("Initializing runtime");

        let mut config = wasmtime::Config::new();
        config.async_support(true);
        config.epoch_interruption(true);
        let engine = wasmtime::Engine::new(&config)?;

        let engine_weak = engine.weak();
        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                if let Some(engine) = engine_weak.upgrade() {
                    engine.increment_epoch();
                } else {
                    return;
                }
            }
        });

        let mut linker: wasmtime::Linker<ComponentRunStates> = wasmtime::Linker::new(&engine);
        wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| &mut t.wasi_ctx)?;

        linker.func_wrap_async(
            "terminal_games",
            "dial",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             (address_ptr, address_len): (i32, u32)| {
                Box::new(async move {
                    let mut buf = [0u8; 64];
                    if address_len >= buf.len() as u32 {
                        anyhow::bail!("dial address too long")
                    }
                    let len = address_len as usize;
                    let offset = address_ptr as usize;

                    let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                        anyhow::bail!("failed to find host memory");
                    };

                    if let Err(_) = mem.read(&mut caller, offset, &mut buf[..len]) {
                        anyhow::bail!("failed to write to host memory");
                    }
                    let address = String::from_utf8_lossy(&buf[..len]);

                    let stream = match tokio::net::TcpStream::connect(address.as_ref()).await {
                        Ok(stream) => stream,
                        Err(_) => {
                            return Ok(-1);
                        }
                    };
                    caller.data_mut().streams.push(stream);

                    Ok((caller.data().streams.len() - 1) as i32)
                })
            },
        )?;
        linker.func_wrap_async(
            "terminal_games",
            "conn_write",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             (conn_id, address_ptr, address_len): (i32, i32, u32)| {
                Box::new(async move {
                    let mut buf = [0u8; 4 * 1024];
                    if address_len >= buf.len() as u32 {
                        anyhow::bail!("address too long")
                    }
                    let len = address_len as usize;
                    let offset = address_ptr as usize;

                    let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                        anyhow::bail!("failed to find host memory");
                    };

                    if let Err(_) = mem.read(&caller, offset, &mut buf[..len]) {
                        anyhow::bail!("failed to write to host memory");
                    }

                    let Some(stream) = caller.data_mut().streams.get_mut(conn_id as usize) else {
                        anyhow::bail!("failed to write to host memory");
                    };

                    match stream.write(&buf[..len]).await {
                        Ok(n) => Ok(n as i32),
                        Err(_) => anyhow::bail!("failed to write"),
                    }
                })
            },
        )?;

        linker.func_wrap(
            "terminal_games",
            "conn_read",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             conn_id: i32,
             ptr: i32,
             len: u32| {
                let Some(stream) = caller.data_mut().streams.get_mut(conn_id as usize) else {
                    anyhow::bail!("failed to write to host memory");
                };

                let mut buf = [0u8; 4 * 1024];
                let len = std::cmp::min(buf.len(), len as usize);
                let n = match stream.try_read(&mut buf[..len]) {
                    Ok(n) => n,
                    Err(_) => return Ok(0),
                };

                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    anyhow::bail!("failed to find host memory");
                };
                let offset = ptr as u32 as usize;
                if let Err(_) = mem.write(&mut caller, offset, &buf[..n]) {
                    anyhow::bail!("failed to write to host memory");
                }
                Ok(n as i32)
            },
        )?;

        linker.func_wrap(
            "terminal_games",
            "terminal_read",
            move |mut caller: wasmtime::Caller<'_, ComponentRunStates>, ptr: i32, _len: u32| {
                match caller.data_mut().input_channel.try_recv() {
                    Ok(buf) => {
                        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory")
                        else {
                            anyhow::bail!("failed to find host memory");
                        };
                        let offset = ptr as u32 as usize;
                        if let Err(_) = mem.write(&mut caller, offset, &buf) {
                            anyhow::bail!("failed to write to host memory");
                        }
                        Ok(buf.len() as i32)
                    }
                    Err(_) => Ok(0),
                }
            },
        )?;

        linker.func_wrap_async(
            "terminal_games",
            "terminal_size",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             (width_ptr, height_ptr): (i32, i32)| {
                Box::new(async move {
                    let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                        anyhow::bail!("failed to find host memory");
                    };
                    let (width, height) = *caller.data().dimensions.lock().await;
                    let effective_height = if height > 0 { height - 1 } else { 0 };

                    let width_offset = width_ptr as u32 as usize;
                    if let Err(_) = mem.write(&mut caller, width_offset, &width.to_le_bytes()) {
                        anyhow::bail!("failed to write to host memory");
                    }

                    let height_offset = height_ptr as u32 as usize;
                    if let Err(_) =
                        mem.write(&mut caller, height_offset, &effective_height.to_le_bytes())
                    {
                        anyhow::bail!("failed to write to host memory");
                    }

                    Ok(())
                })
            },
        )?;

        let path = "examples/kitchen-sink/main.wasm";
        tracing::info!(path, "Compiling");
        let module = wasmtime::Module::from_file(&engine, path)?;

        Ok(Self {
            id: 0,
            engine,
            module,
            linker: Arc::new(Mutex::new(linker)),
        })
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        let config = Config {
            inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
            auth_rejection_time: std::time::Duration::from_secs(3),
            auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
            keys: vec![
                russh::keys::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap(),
            ],
            nodelay: true,
            ..Default::default()
        };

        tracing::info!("Running SSH server");
        self.run_on_address(Arc::new(config), ("0.0.0.0", 2222))
            .await?;
        Ok(())
    }
}

struct MyStdoutStream {
    sender: UnboundedSender<Vec<u8>>,
}

struct AsyncStdoutWriter {
    sender: UnboundedSender<Vec<u8>>,
    buffer: Vec<u8>,
}

impl tokio::io::AsyncWrite for AsyncStdoutWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if !self.buffer.is_empty() {
            let data = std::mem::take(&mut self.buffer);
            if self.sender.send(data).is_err() {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "channel closed",
                )));
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl wasmtime_wasi::cli::StdoutStream for MyStdoutStream {
    fn async_stream(&self) -> Box<dyn tokio::io::AsyncWrite + Send + Sync> {
        Box::new(AsyncStdoutWriter {
            sender: self.sender.clone(),
            buffer: Vec::new(),
        })
    }
}

impl wasmtime_wasi::cli::IsTerminal for MyStdoutStream {
    fn is_terminal(&self) -> bool {
        true
    }
}

impl Server for AppServer {
    type Handler = App;
    fn new_client(&mut self, _: Option<std::net::SocketAddr>) -> App {
        self.id += 1;
        let id = self.id;
        tracing::info!(id, "new client");

        let (output_sender, output_receiver) = unbounded_channel::<Vec<u8>>();

        let wasi_ctx = wasmtime_wasi::WasiCtx::builder()
            .stdout(MyStdoutStream {
                sender: output_sender,
            })
            .build_p1();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let dimensions = Arc::new(Mutex::new((0, 0)));

        let state = ComponentRunStates {
            wasi_ctx: wasi_ctx,
            resource_table: wasmtime_wasi::ResourceTable::new(),
            streams: Vec::default(),
            input_channel: rx,
            limits: MyLimiter::default(),
            dimensions: dimensions.clone(),
        };

        let mut store = wasmtime::Store::new(&self.engine, state);
        store.limiter(|state| &mut state.limits);
        store.epoch_deadline_callback(|_| Ok(wasmtime::UpdateDeadline::Yield(1)));

        let linker = self.linker.clone();
        let module = self.module.clone();
        tokio::task::spawn(async move {
            let func = {
                let linker = linker.lock().await;
                let instance = linker.instantiate_async(&mut store, &module).await.unwrap();
                let func = instance
                    .get_typed_func::<(), ()>(&mut store, "_start")
                    .unwrap();
                func
            };

            match func.call_async(&mut store, ()).await {
                Ok(()) => {}
                Err(err) => if let Ok(_err) = err.downcast::<wasmtime_wasi::I32Exit>() {},
            }
        });

        App {
            input_channel: tx,
            output_receiver: Some(output_receiver),
            dimensions,
        }
    }
}

impl Handler for App {
    type Error = anyhow::Error;

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        session: &mut Session,
    ) -> Result<bool, Self::Error> {
        if let Some(mut output_receiver) = self.output_receiver.take() {
            let handle = session.handle();
            let channel_id = channel.id();
            let dimensions = self.dimensions.clone();

            tokio::spawn(async move {
                while let Some(data) = output_receiver.recv().await {
                    if handle.data(channel_id, data.into()).await.is_err() {
                        eprintln!("Failed to send output data");
                        break;
                    }

                    let (width, height) = *dimensions.lock().await;
                    if width > 0 && height > 0 {
                        let bar = create_status_bar(width, height);
                        if handle.data(channel_id, bar.into()).await.is_err() {
                            eprintln!("Failed to send status bar");
                            break;
                        }
                    }
                }
            });
        }

        Ok(true)
    }

    async fn auth_publickey(&mut self, _: &str, _: &PublicKey) -> Result<Auth, Self::Error> {
        Ok(Auth::Accept)
    }

    async fn data(
        &mut self,
        _channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.input_channel.send(data.to_vec()).await?;

        Ok(())
    }

    async fn window_change_request(
        &mut self,
        _: ChannelId,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        _: &mut Session,
    ) -> Result<(), Self::Error> {
        *self.dimensions.lock().await = (col_width, row_height);

        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        _: &str,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        _: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        *self.dimensions.lock().await = (col_width, row_height);

        session.channel_success(channel)?;

        Ok(())
    }
}
