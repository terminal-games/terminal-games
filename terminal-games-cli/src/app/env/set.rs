// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{AppEnvSetRequest, AppEnvVar};

use super::super::AppEnvSetArgs;
use crate::control_client::AppClient;

pub(super) async fn run(args: AppEnvSetArgs, profile: Option<String>) -> Result<()> {
    let client = AppClient::from_target(&args.shortname, profile.as_deref())?;
    client
        .rpc()
        .await?
        .env_set(
            terminal_games::control::rpc_context(),
            AppEnvSetRequest {
                envs: vec![AppEnvVar {
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
