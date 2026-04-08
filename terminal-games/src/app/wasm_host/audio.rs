use super::super::{AppServer, AppState};
use crate::{
    audio::{CHANNELS, FRAME_SIZE, SAMPLE_RATE},
    wasm_abi::HostApiRegistration,
};

inventory::submit! { HostApiRegistration::new("audio_write", "audio_write_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::audio_write_v1)) }
inventory::submit! { HostApiRegistration::new("audio_info", "audio_info_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::audio_info_v1)) }

impl AppServer {
    fn audio_write_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        sample_count: u32,
    ) -> wasmtime::Result<i32> {
        if sample_count == 0 {
            return Ok(0);
        }
        if caller.data().session_io.is_closed() {
            return Ok(0);
        }

        let sample_count = sample_count.min(SAMPLE_RATE) as usize;
        let float_count = sample_count * CHANNELS;
        let byte_count = float_count * std::mem::size_of::<f32>();

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("audio_write: failed to find host memory");
        };

        let mut buf = vec![0u8; byte_count];
        let offset = ptr as usize;
        mem.read(&caller, offset, &mut buf)?;

        let samples: Vec<f32> = buf
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();

        let written = caller
            .data_mut()
            .audio
            .as_mut()
            .map(|mixer| mixer.write(&samples))
            .unwrap_or(0);
        Ok(written as i32)
    }

    fn audio_info_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        frame_size_ptr: i32,
        sample_rate_ptr: i32,
        pts_ptr: i32,
        buffer_available_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("audio_info: failed to find host memory");
        };

        let (pts, buffer_available) = caller
            .data_mut()
            .audio
            .as_mut()
            .map(|mixer| mixer.info())
            .unwrap_or((0, 0));

        mem.write(
            &mut caller,
            frame_size_ptr as usize,
            &(FRAME_SIZE as u32).to_le_bytes(),
        )?;

        mem.write(
            &mut caller,
            sample_rate_ptr as usize,
            &(SAMPLE_RATE).to_le_bytes(),
        )?;

        mem.write(&mut caller, pts_ptr as usize, &(pts as u64).to_le_bytes())?;

        mem.write(
            &mut caller,
            buffer_available_ptr as usize,
            &(buffer_available as u32).to_le_bytes(),
        )?;

        Ok(0)
    }
}
