// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use dialoguer::Confirm;
use terminal_games::control::DeleteAppRequest;

use super::super::{AdminAppDeleteArgs, load_api, parse_app_delete_ref};

pub(super) async fn run(args: AdminAppDeleteArgs, profile: Option<String>) -> Result<()> {
    let (app_id, shortname) = parse_app_delete_ref(&args.app)?;
    if !args.force
        && !Confirm::new()
            .with_prompt(format!(
                "Delete app {}:{} permanently? This cannot be undone.",
                app_id, shortname
            ))
            .default(false)
            .interact()?
    {
        return Ok(());
    }
    let api = load_api(profile.as_deref())?;
    api.rpc()
        .await?
        .app_delete(
            terminal_games::control::rpc_context(),
            DeleteAppRequest { app_id },
        )
        .await?
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow::anyhow!("unknown app {}", app_id))?;
    println!("Deleted app {}:{}", app_id, shortname);
    Ok(())
}
