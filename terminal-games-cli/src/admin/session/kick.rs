// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::KickSessionRequest;

use super::super::{AdminSessionKickArgs, load_api, parse_session_ref};

pub(super) async fn run(args: AdminSessionKickArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let (region, local_id) = parse_session_ref(&args.session_id)?;
    let base_url = api.region_url(&region).await?;
    api.rpc_at(&base_url)
        .await?
        .session_kick(
            terminal_games::control::rpc_context(),
            KickSessionRequest {
                local_session_id: local_id,
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    println!("Kicked session {}", args.session_id);
    Ok(())
}
