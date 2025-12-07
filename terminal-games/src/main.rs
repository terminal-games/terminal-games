use anyhow::Result;
use tokio::io::AsyncWriteExt;
use wasmtime::{Caller, Config, Engine, Extern, Linker, Module, Store};
use wasmtime_wasi::{I32Exit, ResourceTable, WasiCtx, p1::WasiP1Ctx};

pub struct ComponentRunStates {
    pub wasi_ctx: WasiP1Ctx,
    pub resource_table: ResourceTable,
    streams: Vec<tokio::net::TcpStream>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::new();
    config.async_support(true);
    // config.strategy(wasmtime::Strategy::Winch);
    let engine = Engine::new(&config)?;

    let mut linker: Linker<ComponentRunStates> = Linker::new(&engine);
    wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| &mut t.wasi_ctx)?;

    let wasi_ctx = WasiCtx::builder().inherit_stdio().build_p1();
    let state = ComponentRunStates {
        wasi_ctx: wasi_ctx,
        resource_table: ResourceTable::new(),
        streams: Vec::default(),
    };

    let mut store = Store::new(&engine, state);
    linker.func_wrap("terminal_games", "triple", |x: i32| x * 3)?;
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
                println!("dial {:?}", address);

                // let mut x = Box::pin(tokio::net::TcpStream::connect(address.as_ref()));
                // let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                let stream = match tokio::net::TcpStream::connect(address.as_ref()).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        return {
                            println!("tokio tcpstream connect {}", err);
                            Ok(-1)
                        };
                    }
                };
                caller.data_mut().streams.push(stream);

                Ok(0)
            })
        },
    )?;
    linker.func_wrap_async(
        "terminal_games",
        "dial_write",
        |mut caller: Caller<'_, ComponentRunStates>,
         (conn_id, address_ptr, address_len): (i32, i32, u32)| {
            Box::new(async move {
                println!("writing {} {}", conn_id, address_len);
                let mut buf = [0u8; 4 * 1024];
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

                let Some(stream) = caller.data_mut().streams.get_mut(0) else {
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
        "dial_read",
        |mut caller: Caller<'_, ComponentRunStates>, conn_id: i32, ptr: i32, len: u32| {
            let Some(stream) = caller.data_mut().streams.get_mut(0) else {
                anyhow::bail!("failed to write to host memory");
            };
            // let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            // let poll_result = stream.poll_read_ready(&mut cx);
            // println!("Read poll {:?}", poll_result);

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

    let module = Module::from_file(&engine, "examples/go/net/main.wasm")?;
    let func = linker
        .module_async(&mut store, "", &module)
        .await?
        .get_default(&mut store, "")?
        .typed::<(), ()>(&store)?;

    println!("calling");
    match func.call_async(&mut store, ()).await {
        Ok(()) => {}
        Err(err) => {
            if let Ok(err) = err.downcast::<I32Exit>() {
                std::process::exit(err.0)
            }
        }
    }

    Ok(())
}
