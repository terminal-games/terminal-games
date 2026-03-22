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
use crate::config::parse_author_ref;
use crate::control_client::{AuthorClient, load_author_claims_for_ref};

pub(super) async fn run(args: AuthorUploadArgs) -> Result<()> {
    let wasm = fs::read(&args.path_to_wasm_file)
        .with_context(|| format!("failed to read {}", args.path_to_wasm_file.display()))?;
    let manifest = extract_manifest_from_wasm(&wasm)?
        .ok_or_else(|| anyhow::anyhow!("missing embedded terminal-games manifest"))?;
    validate_manifest(&manifest)?;
    let claims = load_author_claims_for_ref(&args.author_ref)?;
    let (_, ref_shortname) = parse_author_ref(&args.author_ref)?;
    anyhow::ensure!(
        manifest.shortname == ref_shortname,
        "manifest shortname '{}' does not match target '{}'",
        manifest.shortname,
        args.author_ref
    );

    let client = AuthorClient::from_claims(claims)?;
    let envs = load_upload_envs(&args.env, args.env_file.as_ref())?;
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
        "Uploaded '{}' version {} (game id {})",
        response.shortname, response.version, response.game_id,
    );
    Ok(())
}
