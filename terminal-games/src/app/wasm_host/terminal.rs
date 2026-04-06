use super::super::{AppServer, AppState};
use crate::{wasm_abi, wasm_abi::HOST_API_MODULE};

impl AppServer {
    #[rustfmt::skip]
    pub(super) fn link_terminal_host_functions(
        linker: &mut wasmtime::Linker<AppState>,
    ) -> anyhow::Result<()> {
        linker.func_wrap(HOST_API_MODULE, wasm_abi::terminal::READ.current_import(), Self::terminal_read)?;
        linker.func_wrap(HOST_API_MODULE, wasm_abi::terminal::SIZE.current_import(), Self::terminal_size)?;
        linker.func_wrap(HOST_API_MODULE, wasm_abi::terminal::CURSOR.current_import(), Self::terminal_cursor)?;
        Ok(())
    }

    fn terminal_read(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        _len: u32,
    ) -> wasmtime::Result<i32> {
        if caller.data().session_io.is_closed() {
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
            let offset = ptr as u32 as usize;
            mem.write(&mut caller, offset, buf.as_ref())?;
            return Ok(buf.len() as i32);
        }
    }

    fn terminal_size(
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

    fn terminal_cursor(
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
