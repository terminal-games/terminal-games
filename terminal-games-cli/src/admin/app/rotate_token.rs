// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{RotateAppTokenRequest, RotateAppTokenResponse};

use super::super::{AdminAppRotateTokenArgs, load_api};

pub(super) async fn run(args: AdminAppRotateTokenArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let response: RotateAppTokenResponse = api
        .rpc()
        .await?
        .app_rotate_token(
            terminal_games::control::rpc_context(),
            RotateAppTokenRequest {
                app_id: args.app_id,
                base_url: api.profile.url.clone(),
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    println!("App ID: {}", response.app.app_id);
    println!("Shortname: {}", response.app.shortname);
    println!("Token: {}", response.token);
    Ok(())
}
