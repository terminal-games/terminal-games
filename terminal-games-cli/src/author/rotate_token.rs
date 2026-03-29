// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{AuthorTokenClaims, RotateAuthorTokenResponse};

use super::AuthorRotateTokenArgs;
use crate::config::save_author_token;
use crate::control_client::AuthorClient;

pub(super) async fn run(args: AuthorRotateTokenArgs) -> Result<()> {
    let client = AuthorClient::from_target(&args.shortname, args.url.as_deref())?;
    let response: RotateAuthorTokenResponse = client
        .rpc()
        .await?
        .rotate_token(terminal_games::control::rpc_context())
        .await?
        .map_err(anyhow::Error::msg)?;
    let claims = AuthorTokenClaims::decode(&response.token)?;
    save_author_token(&claims)?;
    println!("Rotated token for '{}'", args.shortname);
    println!("Token: {}", response.token);
    Ok(())
}
