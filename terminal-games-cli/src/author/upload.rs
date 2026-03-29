// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::fs;

use anyhow::{Context, Result};
use terminal_games::{
    control::UploadGameResponse,
    manifest::{extract_manifest_from_wasm, validate_manifest},
};

use super::{AuthorUploadArgs, load_upload_envs};
use crate::control_client::{AuthorClient, load_author_claims_for_target};

pub(super) async fn run(args: AuthorUploadArgs) -> Result<()> {
    let wasm = fs::read(&args.path_to_wasm_file)
        .with_context(|| format!("failed to read {}", args.path_to_wasm_file.display()))?;
    let manifest = extract_manifest_from_wasm(&wasm)?
        .ok_or_else(|| anyhow::anyhow!("missing embedded terminal-games manifest"))?;
    validate_manifest(&manifest)?;
    let target_shortname = args.shortname.as_deref().unwrap_or(&manifest.shortname);
    let claims = load_author_claims_for_target(target_shortname, args.url.as_deref())?;
    anyhow::ensure!(
        manifest.shortname == target_shortname,
        "manifest shortname '{}' does not match target '{}'",
        manifest.shortname,
        target_shortname
    );

    let client = AuthorClient::from_claims(claims)?;
    let envs = load_upload_envs(&args.env, args.env_file.as_deref())?;
    let response: UploadGameResponse = client
        .rpc()
        .await?
        .upload(
            terminal_games::control::rpc_context(),
            terminal_games::control::UploadGameRequest { wasm, envs },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    println!(
        "Uploaded '{}' build {} (game id {})",
        response.shortname, response.build_id, response.game_id,
    );
    Ok(())
}
