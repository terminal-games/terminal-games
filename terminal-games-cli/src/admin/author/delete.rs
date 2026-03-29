// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use dialoguer::Confirm;
use terminal_games::control::DeleteAuthorRequest;

use super::super::{AdminAuthorDeleteArgs, load_api, parse_author_delete_ref};

pub(super) async fn run(args: AdminAuthorDeleteArgs, profile: Option<String>) -> Result<()> {
    let (author_id, shortname) = parse_author_delete_ref(&args.author)?;
    if !args.force
        && !Confirm::new()
            .with_prompt(format!(
                "Delete author {}:{} permanently? This cannot be undone.",
                author_id, shortname
            ))
            .default(false)
            .interact()?
    {
        return Ok(());
    }
    let api = load_api(profile.as_deref())?;
    api.rpc()
        .await?
        .author_delete(
            terminal_games::control::rpc_context(),
            DeleteAuthorRequest { author_id },
        )
        .await?
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow::anyhow!("unknown author {}", author_id))?;
    println!("Deleted author {}:{}", author_id, shortname);
    Ok(())
}
