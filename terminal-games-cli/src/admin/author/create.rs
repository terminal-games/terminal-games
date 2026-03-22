// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{CreateAuthorRequest, CreateAuthorResponse};

use super::super::{AdminAuthorCreateArgs, load_api};

pub(super) async fn run(args: AdminAuthorCreateArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let response: CreateAuthorResponse = api
        .rpc()
        .await?
        .author_create(
            terminal_games::control::rpc_context(),
            CreateAuthorRequest {
                shortname: args.shortname,
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
