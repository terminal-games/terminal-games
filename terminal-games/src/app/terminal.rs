use super::{AppServer, AppState};
use crate::wasm_abi::HostApiRegistration;

inventory::submit! { HostApiRegistration::new("terminal_read", "terminal_read_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::terminal_read_v1)) }
inventory::submit! { HostApiRegistration::new("terminal_size", "terminal_size_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::terminal_size_v1)) }
inventory::submit! { HostApiRegistration::new("terminal_cursor", "terminal_cursor_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::terminal_cursor_v1)) }

impl AppServer {
    fn terminal_read_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        len: u32,
    ) -> wasmtime::Result<i32> {
        if caller.data().session_io.is_closed() {
            return Ok(0);
        }
        if len == 0 {
            return Ok(0);
        }
        loop {
            let buf = match caller.data_mut().input_receiver.try_recv() {
                Ok(buf) => buf,
                Err(_) => return Ok(0),
            };
            if caller
                .data()
                .status_bar_input
                .handle_terminal_input(buf.as_ref())
            {
                continue;
            }
            if buf.len() > 4096 {
                return Ok(0);
            }
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                wasmtime::bail!("terminal_read: failed to find host memory");
            };
            let max_len = std::cmp::min(len as usize, buf.len());
            let buf = buf.slice(..max_len);
            let offset = ptr as u32 as usize;
            mem.write(&mut caller, offset, buf.as_ref())?;
            return Ok(buf.len() as i32);
        }
    }

    fn terminal_size_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        width_ptr: i32,
        height_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("terminal_size: failed to find host memory");
        };
        let (width, height) = caller.data().terminal_snapshot.size();
        let effective_height = if height > 0 { height - 1 } else { 0 };

        let width_offset = width_ptr as u32 as usize;
        mem.write(&mut caller, width_offset, &width.to_le_bytes())?;

        let height_offset = height_ptr as u32 as usize;
        mem.write(&mut caller, height_offset, &effective_height.to_le_bytes())?;

        Ok(0)
    }

    fn terminal_cursor_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        x_ptr: i32,
        y_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("terminal_cursor: failed to find host memory");
        };
        let (x, y) = caller.data().terminal_snapshot.cursor_position();

        let x_offset = x_ptr as u32 as usize;
        mem.write(&mut caller, x_offset, &x.to_le_bytes())?;

        let y_offset = y_ptr as u32 as usize;
        mem.write(&mut caller, y_offset, &y.to_le_bytes())?;

        Ok(0)
    }
}
