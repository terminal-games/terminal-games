mod ssh;

use std::{io::Write, sync::Arc};

use anyhow::Result;
use tokio::sync::Mutex;
use wasmtime_wasi::{ResourceTable, p1::WasiP1Ctx};

pub struct ComponentRunStates {
    pub wasi_ctx: WasiP1Ctx,
    pub resource_table: ResourceTable,
    streams: Vec<tokio::net::TcpStream>,
    input_channel: tokio::sync::mpsc::Receiver<Vec<u8>>,
    limits: MyLimiter,
    dimensions: Arc<Mutex<(u32, u32)>>,
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
    let mut server = ssh::AppServer::new().await?;
    server.run().await.expect("Failed running server");

    Ok(())
}
