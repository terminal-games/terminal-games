// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{AuthorEnvSetRequest, AuthorEnvVar};

use super::super::AuthorEnvSetArgs;
use crate::control_client::AuthorClient;

pub(super) async fn run(args: AuthorEnvSetArgs) -> Result<()> {
    let client = AuthorClient::from_target(&args.shortname, args.url.as_deref())?;
    client
        .rpc()
        .await?
        .env_set(
            terminal_games::control::rpc_context(),
            AuthorEnvSetRequest {
                envs: vec![AuthorEnvVar {
                    name: args.name,
                    value: args.value,
                }],
                replace: false,
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    println!("Updated env var for '{}'", args.shortname);
    Ok(())
}
