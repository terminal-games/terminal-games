// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{RotateAuthorTokenRequest, RotateAuthorTokenResponse};

use super::super::{AdminAuthorRotateTokenArgs, load_api};

pub(super) async fn run(args: AdminAuthorRotateTokenArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let response: RotateAuthorTokenResponse = api
        .rpc()
        .await?
        .author_rotate_token(
            terminal_games::control::rpc_context(),
            RotateAuthorTokenRequest {
                author_id: args.author_id,
                base_url: api.profile.url.clone(),
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    println!("Author ID: {}", response.author.author_id);
    println!("Shortname: {}", response.author.shortname);
    println!("Token: {}", response.token);
    Ok(())
}
