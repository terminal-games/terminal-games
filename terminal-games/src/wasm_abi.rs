use std::collections::BTreeSet;

use anyhow::Context as _;

use crate::app::AppState;

pub const HOST_API_MODULE: &str = "terminal_games";
pub const HOST_API_VERSION_MISMATCH: i32 = -32_000;

type LinkFn = for<'a> fn(
    &'a mut wasmtime::Linker<AppState>,
) -> wasmtime::Result<&'a mut wasmtime::Linker<AppState>>;

pub struct HostApiRegistration {
    pub stem: &'static str,
    pub version: u32,
    pub link: LinkFn,
}

inventory::collect!(HostApiRegistration);

impl HostApiRegistration {
    pub const fn new(stem: &'static str, version: u32, link: LinkFn) -> Self {
        Self {
            stem,
            version,
            link,
        }
    }
}

pub(crate) fn link(linker: &mut wasmtime::Linker<AppState>) -> anyhow::Result<()> {
    for registration in inventory::iter::<HostApiRegistration> {
        (registration.link)(linker)?;
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
    let Some((stem, version)) = parse_import(import) else {
        return true;
    };
    let current_version = inventory::iter::<HostApiRegistration>
        .into_iter()
        .filter(|api| api.stem == stem)
        .map(|api| api.version)
        .max();
    current_version.is_none_or(|current| version != current)
}

fn parse_import(import: &str) -> Option<(&str, u32)> {
    let (name, version) = import.rsplit_once("_v")?;
    Some((name, version.parse().ok()?))
}
