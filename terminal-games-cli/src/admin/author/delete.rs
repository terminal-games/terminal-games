// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use dialoguer::Confirm;
use terminal_games::control::{CacheInvalidateRequest, DeleteAuthorRequest};

use super::super::{AdminAuthorDeleteArgs, load_api};

pub(super) async fn run(args: AdminAuthorDeleteArgs, profile: Option<String>) -> Result<()> {
    if !args.force
        && !Confirm::new()
            .with_prompt(format!(
                "Delete author {} permanently? This cannot be undone.",
                args.author_id
            ))
            .default(false)
            .interact()?
    {
        return Ok(());
    }
    let api = load_api(profile.as_deref())?;
    let deleted = api
        .rpc()
        .await?
        .author_delete(
            terminal_games::control::rpc_context(),
            DeleteAuthorRequest {
                author_id: args.author_id,
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow::anyhow!("unknown author {}", args.author_id))?;
    let invalidate_body = CacheInvalidateRequest {
        shortname: deleted.shortname,
    };
    api.fanout(|rpc| {
        let invalidate_body = invalidate_body.clone();
        async move {
            rpc.cache_invalidate(terminal_games::control::rpc_context(), invalidate_body)
                .await?
                .map_err(anyhow::Error::msg)
        }
    })
    .await?;
    println!("Deleted author {}", args.author_id);
    Ok(())
}
