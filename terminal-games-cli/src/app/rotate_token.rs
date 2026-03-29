// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{AppTokenClaims, RotateAppTokenResponse};

use super::AppRotateTokenArgs;
use crate::config::save_app_token;
use crate::control_client::AppClient;

pub(super) async fn run(args: AppRotateTokenArgs, profile: Option<String>) -> Result<()> {
    let client = AppClient::from_target(&args.shortname, profile.as_deref())?;
    let response: RotateAppTokenResponse = client
        .rpc()
        .await?
        .rotate_token(terminal_games::control::rpc_context())
        .await?
        .map_err(anyhow::Error::msg)?;
    let claims = AppTokenClaims::decode(&response.token)?;
    save_app_token(&claims)?;
    println!("Rotated token for '{}'", args.shortname);
    println!("Token: {}", response.token);
    Ok(())
}
