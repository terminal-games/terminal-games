use std::io::Write;

use wasmtime::component::{Component, HasData, Linker, ResourceTable, bindgen};
use wasmtime::*;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

bindgen!({
    world: "app",
    path: "../wit",
    imports: {
        default: async | trappable
    },
    exports: {
        default: async | trappable
    }
});

pub struct ComponentRunStates {
    // These two are required basically as a standard way to enable the impl of IoView and
    // WasiView.
    // impl of WasiView is required by [`wasmtime_wasi::p2::add_to_linker_sync`]
    pub wasi_ctx: WasiCtx,
    pub resource_table: ResourceTable,
    // You can add other custom host states if needed
    limits: MyLimiter,
}

impl WasiView for ComponentRunStates {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

struct MyLimiter;

impl ResourceLimiter for MyLimiter {
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

struct TerminalData;

impl HasData for TerminalData {
    type Data<'a> = &'a mut ComponentRunStates;
}

impl terminal_games::app::terminal::Host for ComponentRunStates {
    async fn size(&mut self) -> wasmtime::Result<terminal_games::app::terminal::Dimensions> {
        let (width, height) = crossterm::terminal::size()?;
        Ok(terminal_games::app::terminal::Dimensions { width, height })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    crossterm::terminal::enable_raw_mode()?;

    let mut config = Config::new();
    config.async_support(true);
    config.epoch_interruption(true);
    let engine = Engine::new(&config)?;
    let mut linker = Linker::new(&engine);

    let engine_weak = engine.weak();
    tokio::task::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if let Some(engine) = engine_weak.upgrade() {
                engine.increment_epoch();
            } else {
                return;
            }
        }
    });

    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    terminal_games::app::terminal::add_to_linker::<_, TerminalData>(&mut linker, |s| s)?;

    let wasi = WasiCtx::builder()
        .inherit_stdin()
        .inherit_stdout()
        .inherit_stderr()
        .inherit_args()
        .build();

    let args: Vec<String> = std::env::args().collect();
    let path = &args[1];
    _ = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open("log.txt");
    let component = Component::from_file(&engine, path)?;

    let state = ComponentRunStates {
        wasi_ctx: wasi,
        resource_table: ResourceTable::new(),
        limits: MyLimiter,
    };
    let mut store = Store::new(&engine, state);
    store.limiter(|state| &mut state.limits);
    store.epoch_deadline_callback(|_| Ok(UpdateDeadline::Yield(1)));

    let plugin = App::instantiate_async(&mut store, &component, &linker).await?;
    let program_result = plugin.wasi_cli_run().call_run(&mut store).await?;

    if program_result.is_err() {
        std::process::exit(1)
    }

    crossterm::terminal::disable_raw_mode()?;

    Ok(())
}
