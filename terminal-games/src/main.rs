use std::io::{Read, Write};

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use wasmtime::{Caller, Config, Engine, Extern, Linker, Module, Store};
use wasmtime_wasi::{I32Exit, ResourceTable, WasiCtx, p1::WasiP1Ctx};

pub struct ComponentRunStates {
    pub wasi_ctx: WasiP1Ctx,
    pub resource_table: ResourceTable,
    streams: Vec<tokio::net::TcpStream>,
    input_channel: std::sync::mpsc::Receiver<Vec<u8>>,
    limits: MyLimiter,
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
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .open("log.txt")
            .unwrap();
        writeln!(
            &mut log,
            "memory growing current={} desired={} maximum={:?}",
            current, desired, maximum
        )?;
        self.total -= current;
        self.total += desired;
        if self.total >= 32 * 1024 * 1024 {
            writeln!(&mut log, "rejected memory grow total={}", self.total)?;
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
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .open("log.txt")
            .unwrap();
        writeln!(
            &mut log,
            "table growing current={} desired={} maximum={:?}",
            current, desired, maximum
        )?;
        return Ok(true);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::new();
    config.async_support(true);
    config.epoch_interruption(true);
    let engine = Engine::new(&config)?;

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

    let mut linker: Linker<ComponentRunStates> = Linker::new(&engine);
    wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| &mut t.wasi_ctx)?;

    linker.func_wrap_async(
        "terminal_games",
        "dial",
        |mut caller: Caller<'_, ComponentRunStates>, (address_ptr, address_len): (i32, u32)| {
            Box::new(async move {
                let mut buf = [0u8; 64];
                if address_len >= buf.len() as u32 {
                    anyhow::bail!("dial address too long")
                }
                let len = address_len as usize;
                let offset = address_ptr as usize;

                let Some(Extern::Memory(mem)) = caller.get_export("memory") else {
                    anyhow::bail!("failed to find host memory");
                };

                if let Err(_) = mem.read(&mut caller, offset, &mut buf[..len]) {
                    anyhow::bail!("failed to write to host memory");
                }
                let address = String::from_utf8_lossy(&buf[..len]);

                let stream = match tokio::net::TcpStream::connect(address.as_ref()).await {
                    Ok(stream) => stream,
                    Err(_) => {
                        return {
                            // println!("tokio tcpstream connect {}", err);
                            Ok(-1)
                        };
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
        |mut caller: Caller<'_, ComponentRunStates>,
         (conn_id, address_ptr, address_len): (i32, i32, u32)| {
            Box::new(async move {
                let mut buf = [0u8; 4 * 1024];
                if address_len >= buf.len() as u32 {
                    anyhow::bail!("address too long")
                }
                let len = address_len as usize;
                let offset = address_ptr as usize;

                let Some(Extern::Memory(mem)) = caller.get_export("memory") else {
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
        |mut caller: Caller<'_, ComponentRunStates>, conn_id: i32, ptr: i32, len: u32| {
            let Some(stream) = caller.data_mut().streams.get_mut(conn_id as usize) else {
                anyhow::bail!("failed to write to host memory");
            };

            let mut buf = [0u8; 4 * 1024];
            let len = std::cmp::min(buf.len(), len as usize);
            let n = match stream.try_read(&mut buf[..len]) {
                Ok(n) => n,
                Err(_) => return Ok(0),
            };

            let Some(Extern::Memory(mem)) = caller.get_export("memory") else {
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
        move |mut caller: Caller<'_, ComponentRunStates>, ptr: i32, _len: u32| match caller
            .data()
            .input_channel
            .try_recv()
        {
            Ok(buf) => {
                let Some(Extern::Memory(mem)) = caller.get_export("memory") else {
                    anyhow::bail!("failed to find host memory");
                };
                let offset = ptr as u32 as usize;
                if let Err(_) = mem.write(&mut caller, offset, &buf) {
                    anyhow::bail!("failed to write to host memory");
                }
                Ok(buf.len() as i32)
            }
            Err(_) => Ok(0),
        },
    )?;

    linker.func_wrap(
        "terminal_games",
        "terminal_size",
        move |mut caller: Caller<'_, ComponentRunStates>, width_ptr: i32, height_ptr: u32| {
            let Some(Extern::Memory(mem)) = caller.get_export("memory") else {
                anyhow::bail!("failed to find host memory");
            };
            let (width, height) = crossterm::terminal::size()?;

            let width_offset = width_ptr as u32 as usize;
            if let Err(_) = mem.write(&mut caller, width_offset, &width.to_le_bytes()) {
                anyhow::bail!("failed to write to host memory");
            }

            let height_offset = height_ptr as u32 as usize;
            if let Err(_) = mem.write(&mut caller, height_offset, &height.to_le_bytes()) {
                anyhow::bail!("failed to write to host memory");
            }

            Ok(())
        },
    )?;

    let wasi_ctx = WasiCtx::builder()
        .inherit_stdout()
        .inherit_stderr()
        .build_p1();

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        loop {
            let mut buf = [0u8; 64];

            match std::io::stdin().read(&mut buf) {
                Ok(0) => continue,
                Ok(n) => {
                    tx.send(buf[..n].to_vec()).unwrap();
                }
                Err(e) => {
                    eprintln!("stdin read error: {:?}", e);
                    break;
                }
            }
        }
    });

    let state = ComponentRunStates {
        wasi_ctx: wasi_ctx,
        resource_table: ResourceTable::new(),
        streams: Vec::default(),
        input_channel: rx,
        limits: MyLimiter::default(),
    };

    let mut store = Store::new(&engine, state);
    store.limiter(|state| &mut state.limits);
    store.epoch_deadline_callback(|_| Ok(wasmtime::UpdateDeadline::Yield(1)));

    let args: Vec<String> = std::env::args().collect();
    let path = &args[1];
    _ = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open("log.txt");

    let module = Module::from_file(&engine, path)?;
    let func = linker
        .module_async(&mut store, "", &module)
        .await?
        .get_default(&mut store, "")?
        .typed::<(), ()>(&store)?;

    crossterm::terminal::enable_raw_mode()?;
    match func.call_async(&mut store, ()).await {
        Ok(()) => {}
        Err(err) => {
            if let Ok(err) = err.downcast::<I32Exit>() {
                crossterm::terminal::disable_raw_mode()?;
                std::process::exit(err.0)
            }
        }
    }
    crossterm::terminal::disable_raw_mode()?;

    Ok(())
}
