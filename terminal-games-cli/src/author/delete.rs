// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use dialoguer::Confirm;
use terminal_games::control::DeleteShortnameResponse;

use super::AuthorDeleteArgs;
use crate::control_client::AuthorClient;

pub(super) async fn run(args: AuthorDeleteArgs) -> Result<()> {
    if !args.force
        && !Confirm::new()
            .with_prompt(format!(
                "Delete '{}' permanently? This cannot be undone.",
                args.shortname
            ))
            .default(false)
            .interact()?
    {
        return Ok(());
    }

    let client = AuthorClient::from_target(&args.shortname, args.url.as_deref())?;
    let response: DeleteShortnameResponse = client
        .rpc()
        .await?
        .delete_shortname(
            terminal_games::control::rpc_context(),
            terminal_games::control::DeleteShortnameRequest {
                shortname: args.shortname.clone(),
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow::anyhow!("unknown shortname '{}'", args.shortname))?;
    println!("Deleted '{}'", response.shortname);
    Ok(())
}
