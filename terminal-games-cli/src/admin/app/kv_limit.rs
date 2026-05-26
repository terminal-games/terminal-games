// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{AppSummary, SetAppKvStorageLimitRequest};

use super::super::{AdminAppKvLimitArgs, load_api};
use crate::config::format_storage_usage;

pub(super) async fn run(args: AdminAppKvLimitArgs, profile: Option<String>) -> Result<()> {
    let limit_bytes = match (args.default, args.limit_bytes) {
        (true, _) => None,
        (false, Some(limit_bytes)) => Some(limit_bytes),
        (false, None) => anyhow::bail!("limit_bytes is required unless --default is set"),
    };
    let api = load_api(profile.as_deref())?;
    let response: AppSummary = api
        .rpc()
        .await?
        .app_set_kv_storage_limit(
            terminal_games::control::rpc_context(),
            SetAppKvStorageLimitRequest {
                app_id: args.app_id,
                limit_bytes,
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    println!("App ID: {}", response.app_id);
    println!("Shortname: {}", response.shortname);
    println!(
        "KV storage: {}",
        format_storage_usage(
            response.kv_storage_bytes,
            response.kv_storage_limit_bytes,
            response.kv_storage_limit_override_bytes.is_some(),
        )
    );
    Ok(())
}
