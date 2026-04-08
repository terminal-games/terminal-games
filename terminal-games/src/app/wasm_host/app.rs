use std::{sync::atomic::Ordering, time::Duration, time::UNIX_EPOCH};

use super::super::{
    AppServer, AppState, CHANGE_APP_ERR_RATE_LIMITED, CHANGE_APP_RATE_LIMIT_SECS,
    NEXT_APP_READY_ERR_PREPARE_FAILED_OTHER, NEXT_APP_READY_NOT_READY, NEXT_APP_READY_READY,
    NextAppPrepareError, NextAppState,
};
use crate::wasm_abi::HostApiRegistration;

inventory::submit! { HostApiRegistration::new("change_app", "change_app_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::change_app_v1)) }
inventory::submit! { HostApiRegistration::new("next_app_ready", "next_app_ready_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::next_app_ready_v1)) }
inventory::submit! { HostApiRegistration::new("graceful_shutdown_poll", "graceful_shutdown_poll_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::graceful_shutdown_poll_v1)) }
inventory::submit! { HostApiRegistration::new("new_version_available_poll", "new_version_available_poll_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::new_version_available_poll_v1)) }
inventory::submit! { HostApiRegistration::new("network_info", "network_info_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::network_info_v1)) }
inventory::submit! { HostApiRegistration::new("terminal_info", "terminal_info_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::terminal_info_v1)) }

impl AppServer {
    fn change_app_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        len: u32,
    ) -> wasmtime::Result<i32> {
        let len = len as usize;
        if len == 0 || len > 128 {
            return Ok(-1);
        }

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("change_app: failed to find host memory");
        };

        let mut buf = vec![0u8; len];
        let offset = ptr as usize;
        mem.read(&caller, offset, &mut buf)?;

        let shortname = match String::from_utf8(buf) {
            Ok(s) => s,
            Err(_) => return Ok(-1),
        };

        let has_next_app = caller.data().has_next_app.clone();
        let should_prepare = !matches!(
            caller.data().next_app.as_ref(),
            Some(NextAppState::Pending(_))
        );
        if !should_prepare {
            return Ok(0);
        }

        if !caller
            .data()
            .change_app_limiter
            .try_acquire(Duration::from_secs(CHANGE_APP_RATE_LIMIT_SECS))
        {
            return Ok(CHANGE_APP_ERR_RATE_LIMITED);
        }

        has_next_app.store(false, Ordering::Release);
        let ctx = caller.data().ctx.clone();
        let menu_session = caller.data().menu_session.clone();
        let session_identity = caller.data().session_identity.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        caller.data_mut().next_app = Some(NextAppState::Pending(rx));
        tokio::task::spawn(async move {
            let result =
                match Self::prepare_instantiate(&ctx, &menu_session, &session_identity, shortname)
                    .await
                {
                    Ok(next_app) => {
                        has_next_app.store(true, Ordering::Release);
                        Ok(next_app)
                    }
                    Err(err) => {
                        let error = NextAppPrepareError::from_anyhow(err);
                        tracing::warn!(error = %error.message(), "change_app preload failed");
                        has_next_app.store(false, Ordering::Release);
                        Err(error)
                    }
                };
            let _ = tx.send(result);
        });

        Ok(0)
    }

    fn next_app_ready_v1(mut caller: wasmtime::Caller<'_, AppState>) -> wasmtime::Result<i32> {
        let ready = caller.data().has_next_app.load(Ordering::Acquire);
        if ready {
            return Ok(NEXT_APP_READY_READY);
        }

        let Some(next_app_state) = caller.data_mut().next_app.take() else {
            return Ok(NEXT_APP_READY_NOT_READY);
        };
        match next_app_state {
            NextAppState::Pending(mut next_app_rx) => match next_app_rx.try_recv() {
                Ok(Ok(next_app)) => {
                    caller.data_mut().next_app = Some(NextAppState::Ready(next_app));
                    Ok(NEXT_APP_READY_READY)
                }
                Ok(Err(error)) => {
                    let code = error.code.to_i32();
                    caller.data_mut().next_app = Some(NextAppState::Failed(error));
                    Ok(code)
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    caller.data_mut().next_app = Some(NextAppState::Pending(next_app_rx));
                    Ok(NEXT_APP_READY_NOT_READY)
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    Ok(NEXT_APP_READY_ERR_PREPARE_FAILED_OTHER)
                }
            },
            NextAppState::Ready(next_app) => {
                caller.data_mut().next_app = Some(NextAppState::Ready(next_app));
                Ok(NEXT_APP_READY_READY)
            }
            NextAppState::Failed(error) => {
                let code = error.code.to_i32();
                caller.data_mut().next_app = Some(NextAppState::Failed(error));
                Ok(code)
            }
        }
    }
    fn graceful_shutdown_poll_v1(caller: wasmtime::Caller<'_, AppState>) -> wasmtime::Result<i32> {
        let is_cancelled = caller.data().graceful_shutdown_token.is_cancelled();
        Ok(if is_cancelled { 1 } else { 0 })
    }

    fn new_version_available_poll_v1(
        caller: wasmtime::Caller<'_, AppState>,
    ) -> wasmtime::Result<i32> {
        let has_update = caller
            .data()
            .app
            .app_runtime_session
            .update_available()
            .load(Ordering::Relaxed);
        Ok(if has_update { 1 } else { 0 })
    }

    fn network_info_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        bytes_per_sec_in_ptr: i32,
        bytes_per_sec_out_ptr: i32,
        last_throttled_ms_ptr: i32,
        latency_ms_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("network_info: failed to find host memory");
        };

        let info = &caller.data().network_info;

        let bytes_per_sec_in = info.bytes_per_sec_in();
        let bytes_per_sec_out = info.bytes_per_sec_out();
        let last_throttled_ms = info
            .last_throttled()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let latency_ms = info.latency().map(|d| d.as_millis() as i32).unwrap_or(-1);

        mem.write(
            &mut caller,
            bytes_per_sec_in_ptr as usize,
            &bytes_per_sec_in.to_le_bytes(),
        )?;
        mem.write(
            &mut caller,
            bytes_per_sec_out_ptr as usize,
            &bytes_per_sec_out.to_le_bytes(),
        )?;
        mem.write(
            &mut caller,
            last_throttled_ms_ptr as usize,
            &last_throttled_ms.to_le_bytes(),
        )?;
        mem.write(
            &mut caller,
            latency_ms_ptr as usize,
            &latency_ms.to_le_bytes(),
        )?;

        Ok(0)
    }

    fn terminal_info_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        color_mode_ptr: i32,
        has_bg_ptr: i32,
        bg_r_ptr: i32,
        bg_g_ptr: i32,
        bg_b_ptr: i32,
        has_dark_ptr: i32,
        dark_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("terminal_info: failed to find host memory");
        };

        let profile = caller.data().terminal_profile;
        let mode = profile.color_mode as u8;
        let background_rgb = caller
            .data()
            .terminal_snapshot
            .background_rgb()
            .or(profile.background_rgb);
        let (has_bg, bg_r, bg_g, bg_b) = match background_rgb {
            Some((r, g, b)) => (1i32, r, g, b),
            None => (0i32, 0u8, 0u8, 0u8),
        };
        let dark_background = background_rgb
            .map(super::super::is_rgb_dark)
            .or(profile.dark_background);
        let (has_dark, dark) = match dark_background {
            Some(is_dark) => (1i32, if is_dark { 1i32 } else { 0i32 }),
            None => (0i32, 0i32),
        };

        mem.write(&mut caller, color_mode_ptr as usize, &[mode])?;
        mem.write(&mut caller, has_bg_ptr as usize, &has_bg.to_le_bytes())?;
        mem.write(&mut caller, bg_r_ptr as usize, &[bg_r])?;
        mem.write(&mut caller, bg_g_ptr as usize, &[bg_g])?;
        mem.write(&mut caller, bg_b_ptr as usize, &[bg_b])?;
        mem.write(&mut caller, has_dark_ptr as usize, &has_dark.to_le_bytes())?;
        mem.write(&mut caller, dark_ptr as usize, &dark.to_le_bytes())?;

        Ok(0)
    }
}
