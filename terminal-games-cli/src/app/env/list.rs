// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::AppEnvListResponse;

use super::super::AppEnvListArgs;
use crate::config::print_table;
use crate::control_client::AppClient;

pub(super) async fn run(args: AppEnvListArgs, profile: Option<String>) -> Result<()> {
    let client = AppClient::from_target(&args.shortname, profile.as_deref())?;
    let response: AppEnvListResponse = client
        .rpc()
        .await?
        .env_list(terminal_games::control::rpc_context())
        .await?
        .map_err(anyhow::Error::msg)?;
    let rows = response
        .envs
        .into_iter()
        .map(|env| vec![env.name, env.value])
        .collect::<Vec<_>>();
    print_table(&["Name", "Value"], &rows);
    Ok(())
}
