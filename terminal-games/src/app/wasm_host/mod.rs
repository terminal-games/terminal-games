mod app;
mod audio;
mod log;
mod menu;
mod net;
mod peer;
mod terminal;

use std::collections::HashSet;

use super::{AppServer, AppState};
use crate::wasm_abi::{self, HOST_API_MODULE};

pub(super) fn link_missing_host_api_fallbacks(
    linker: &mut wasmtime::Linker<AppState>,
    module: &wasmtime::Module,
) -> anyhow::Result<()> {
    let mut defined_fallbacks = HashSet::new();
    for import in module.imports() {
        if import.module() != HOST_API_MODULE {
            continue;
        }
        if wasm_abi::ALL_FUNCTIONS
            .iter()
            .any(|api| api.current_import() == import.name())
        {
            continue;
        }
        if !defined_fallbacks.insert(import.name().to_string()) {
            continue;
        }
        let wasmtime::ExternType::Func(func_ty) = import.ty() else {
            anyhow::bail!(
                "unsupported non-function terminal_games import: {}",
                import.name()
            );
        };
        let result_tys = func_ty.results().collect::<Vec<_>>();
        linker.func_new(
            HOST_API_MODULE,
            import.name(),
            func_ty,
            move |_, _, results| {
                if result_tys.len() == 1 && result_tys[0].is_i32() {
                    results[0] = wasmtime::Val::I32(wasm_abi::HOST_API_VERSION_MISMATCH);
                    return Ok(());
                }
                for (slot, ty) in results.iter_mut().zip(result_tys.iter()) {
                    let Some(value) = wasmtime::Val::default_for_ty(ty) else {
                        return Err(wasmtime::Error::msg(
                            "no default value for host API fallback",
                        ));
                    };
                    *slot = value;
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}

pub(super) fn link(linker: &mut wasmtime::Linker<AppState>) -> anyhow::Result<()> {
    AppServer::link_app_host_functions(linker)?;
    AppServer::link_terminal_host_functions(linker)?;
    AppServer::link_net_host_functions(linker)?;
    AppServer::link_peer_host_functions(linker)?;
    AppServer::link_audio_host_functions(linker)?;
    AppServer::link_log_host_functions(linker)?;
    AppServer::link_menu_host_functions(linker)?;
    Ok(())
}
