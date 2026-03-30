// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::fs;

use anyhow::{Context, Result};
use terminal_games::manifest::{extract_manifest_from_wasm, validate_manifest};

use super::AppManifestArgs;

pub(super) async fn run(args: AppManifestArgs) -> Result<()> {
    let wasm = fs::read(&args.path_to_wasm_file)
        .with_context(|| format!("failed to read {}", args.path_to_wasm_file.display()))?;
    let manifest = extract_manifest_from_wasm(&wasm)?
        .ok_or_else(|| anyhow::anyhow!("missing embedded terminal-games manifest"))?;
    validate_manifest(&manifest)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}
