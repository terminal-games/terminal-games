// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::AuthorEnvDeleteRequest;

use super::super::AuthorEnvDeleteArgs;
use crate::control_client::AuthorClient;

pub(super) async fn run(args: AuthorEnvDeleteArgs) -> Result<()> {
    let client = AuthorClient::from_target(&args.shortname, args.url.as_deref())?;
    client
        .rpc()
        .await?
        .env_delete(
            terminal_games::control::rpc_context(),
            AuthorEnvDeleteRequest { name: args.name },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    println!("Deleted env var for '{}'", args.shortname);
    Ok(())
}
