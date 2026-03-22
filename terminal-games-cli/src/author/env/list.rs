// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::AuthorEnvListResponse;

use super::super::AuthorEnvListArgs;
use crate::config::print_table;
use crate::control_client::AuthorClient;

pub(super) async fn run(args: AuthorEnvListArgs) -> Result<()> {
    let client = AuthorClient::from_ref(&args.author_ref)?;
    let response: AuthorEnvListResponse = client
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
