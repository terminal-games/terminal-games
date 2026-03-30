// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{LazyLock, OnceLock},
};

use boon::{Compiler, Draft, SchemaIndex, Schemas};

const APP_MANIFEST_SCHEMA_ID: &str = "urn:terminal-games:manifest-schema:v1";

static APP_MANIFEST_SCHEMA_VALUE: LazyLock<serde_json::Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../terminal-games.schema.json"))
        .expect("manifest schema must be valid JSON")
});

static COMPILED_APP_MANIFEST_SCHEMA: OnceLock<Result<CompiledManifestSchema, String>> =
    OnceLock::new();

struct CompiledManifestSchema {
    schemas: Schemas,
    schema_index: SchemaIndex,
}

pub fn validate_manifest_json(bytes: &[u8]) -> Result<(), String> {
    let instance =
        serde_json::from_slice::<serde_json::Value>(bytes).map_err(|error| error.to_string())?;
    validate_manifest_json_value(&instance)
}

pub fn validate_manifest_file(path: impl AsRef<Path>) -> Result<(), String> {
    let path = path.as_ref();
    let bytes =
        fs::read(path).map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    validate_manifest_json(&bytes)
}

pub fn setup_build() -> Result<(), String> {
    let cargo_manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").map_err(|error| error.to_string())?);
    let out_dir = PathBuf::from(env::var("OUT_DIR").map_err(|error| error.to_string())?);
    let manifest_path = cargo_manifest_dir.join("terminal-games.json");
    println!("cargo:rerun-if-changed={}", manifest_path.display());
    prepare_embedded_manifest(&manifest_path, &out_dir)
        .map_err(|error| format!("invalid {}: {error}", manifest_path.display()))?;
    Ok(())
}

fn prepare_embedded_manifest(
    manifest_path: impl AsRef<Path>,
    out_dir: impl AsRef<Path>,
) -> Result<PathBuf, String> {
    let manifest_path = manifest_path.as_ref();
    let out_dir = out_dir.as_ref();
    let bytes = fs::read(manifest_path)
        .map_err(|error| format!("failed to read {}: {error}", manifest_path.display()))?;
    let processed = process_manifest_json(&bytes)?;
    let out_path = out_dir.join("terminal-games.embed.json");
    fs::write(&out_path, processed)
        .map_err(|error| format!("failed to write {}: {error}", out_path.display()))?;
    Ok(out_path)
}

fn validate_manifest_json_value(instance: &serde_json::Value) -> Result<(), String> {
    let compiled = compiled_manifest_schema()?;
    compiled
        .schemas
        .validate(instance, compiled.schema_index)
        .map_err(|error| format!("{error:#}"))
}

fn process_manifest_json(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut instance =
        serde_json::from_slice::<serde_json::Value>(bytes).map_err(|error| error.to_string())?;
    
    if let Some(value) = instance.pointer_mut("/details/version") {
        if value.as_str() == Some("$CARGO_PKG_VERSION") {
            *value = serde_json::Value::String(std::env::var("CARGO_PKG_VERSION").unwrap());
        }
    }

    validate_manifest_json_value(&instance)?;
    serde_json::to_vec(&instance).map_err(|error| error.to_string())
}

fn compiled_manifest_schema() -> Result<&'static CompiledManifestSchema, String> {
    COMPILED_APP_MANIFEST_SCHEMA
        .get_or_init(|| {
            let mut schemas = Schemas::new();
            let mut compiler = Compiler::new();
            compiler.set_default_draft(Draft::V2020_12);
            compiler
                .add_resource(APP_MANIFEST_SCHEMA_ID, APP_MANIFEST_SCHEMA_VALUE.clone())
                .map_err(|error| format!("{error:#}"))?;
            let schema_index = compiler
                .compile(APP_MANIFEST_SCHEMA_ID, &mut schemas)
                .map_err(|error| format!("{error:#}"))?;
            Ok(CompiledManifestSchema {
                schemas,
                schema_index,
            })
        })
        .as_ref()
        .map_err(Clone::clone)
}
