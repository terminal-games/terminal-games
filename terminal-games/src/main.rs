// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod ssh;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use wasmtime_wasi::{ResourceTable, p1::WasiP1Ctx};

use crate::ssh::ModuleCache;

pub struct ComponentRunStates {
    pub wasi_ctx: WasiP1Ctx,
    pub resource_table: ResourceTable,
    streams: Vec<tokio::net::TcpStream>,
    limits: MyLimiter,
    dimensions: Arc<Mutex<(u32, u32)>>,
    next_app_shortname: Arc<Mutex<Option<String>>>,
    input_receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    module_cache: Arc<Mutex<ModuleCache>>,
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

    let mut server = ssh::AppServer::new().await?;
    server.run().await.expect("Failed running server");

    Ok(())
}
