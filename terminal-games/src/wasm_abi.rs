use std::collections::BTreeSet;

use anyhow::Context as _;

use crate::app::AppState;

pub const HOST_API_MODULE: &str = "terminal_games";
pub const HOST_API_VERSION_MISMATCH: i32 = -32_000;

type LinkFn = for<'a> fn(
    &'a mut wasmtime::Linker<AppState>,
    &'static str,
    &'static str,
) -> wasmtime::Result<&'a mut wasmtime::Linker<AppState>>;

pub struct HostApiRegistration {
    pub stable_id: &'static str,
    pub import: &'static str,
    pub version: u32,
    pub link: LinkFn,
}

inventory::collect!(HostApiRegistration);

impl HostApiRegistration {
    pub const fn new(
        stable_id: &'static str,
        import: &'static str,
        version: u32,
        link: LinkFn,
    ) -> Self {
        Self {
            stable_id,
            import,
            version,
            link,
        }
    }
}

pub(crate) fn link(linker: &mut wasmtime::Linker<AppState>) -> anyhow::Result<()> {
    for registration in inventory::iter::<HostApiRegistration> {
        (registration.link)(linker, HOST_API_MODULE, registration.import)?;
    }
    Ok(())
}

pub fn guest_host_api_inventory(wasm: &[u8]) -> anyhow::Result<Vec<String>> {
    let mut imports_out = BTreeSet::new();
    for payload in wasmparser::Parser::new(0).parse_all(wasm) {
        if let wasmparser::Payload::ImportSection(imports) =
            payload.context("failed to parse wasm imports")?
        {
            for import in imports {
                let import = import.context("failed to parse wasm import group")?;
                for import in import {
                    let (_, import) = import.context("failed to parse wasm import")?;
                    if import.module == HOST_API_MODULE {
                        imports_out.insert(import.name.to_string());
                    }
                }
            }
        }
    }
    Ok(imports_out.into_iter().collect())
}

pub fn is_host_api_import_out_of_date(import: &str) -> bool {
    let Some(registration) = registration_for_import(import) else {
        return true;
    };
    let Some(latest_registration) = latest_host_api_registration(registration.stable_id) else {
        return true;
    };
    registration.version != latest_registration.version
}

pub fn latest_host_api_import(import: &str) -> Option<String> {
    Some(
        latest_host_api_registration(registration_for_import(import)?.stable_id)?
            .import
            .to_string(),
    )
}

fn registration_for_import(import: &str) -> Option<&'static HostApiRegistration> {
    inventory::iter::<HostApiRegistration>
        .into_iter()
        .find(|api| api.import == import)
}

fn latest_host_api_registration(stable_id: &str) -> Option<&'static HostApiRegistration> {
    inventory::iter::<HostApiRegistration>
        .into_iter()
        .filter(|api| api.stable_id == stable_id)
        .max_by_key(|api| api.version)
}
