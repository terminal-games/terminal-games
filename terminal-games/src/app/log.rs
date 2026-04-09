use super::{AppServer, AppState};
use crate::{
    log_backend::{LogLevel, parse_guest_log_object},
    wasm_abi::HostApiRegistration,
};

inventory::submit! { HostApiRegistration::new("log", "log_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::log_v1)) }

impl AppServer {
    fn log_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        level: u32,
        ptr: i32,
        len: u32,
    ) -> wasmtime::Result<i32> {
        if len == 0 {
            return Ok(0);
        }

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("host_log: failed to find host memory");
        };

        let read_len = (len as usize).min(16384);
        let mut buf = vec![0u8; read_len];
        mem.read(&caller, ptr as u32 as usize, &mut buf)?;

        let message = String::from_utf8_lossy(&buf);
        let trimmed = message.trim_end_matches(['\r', '\n']);
        let fallback_level = LogLevel::from_u8(level as u8).unwrap_or(LogLevel::Info);
        let record = match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(serde_json::Value::Object(obj)) => parse_guest_log_object(obj, trimmed),
            _ => crate::log_backend::GuestLogRecord::new(fallback_level, trimmed),
        };

        if record.message.is_empty() && record.attributes.is_empty() {
            return Ok(0);
        }

        let state = caller.data();
        state
            .ctx
            .log_backend
            .log(&state.app.shortname, state.ctx.user_id, &record);
        Ok(0)
    }
}
