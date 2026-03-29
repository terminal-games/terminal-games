// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::AppEnvDeleteRequest;

use super::super::AppEnvDeleteArgs;
use crate::control_client::AppClient;

pub(super) async fn run(args: AppEnvDeleteArgs, profile: Option<String>) -> Result<()> {
    let client = AppClient::from_target(&args.shortname, profile.as_deref())?;
    client
        .rpc()
        .await?
        .env_delete(
            terminal_games::control::rpc_context(),
            AppEnvDeleteRequest { name: args.name },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    println!("Deleted env var for '{}'", args.shortname);
    Ok(())
}
