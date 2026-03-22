// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{
    BanEntry, BanIpAddRequest, BanIpRemoveRequest, parse_optional_expiry,
};

use super::{
    AdminBanIpAddArgs, AdminBanIpCommand, AdminBanIpRemoveArgs, format_optional_unix, load_api,
};
use crate::config::print_table;

pub(super) async fn run(command: AdminBanIpCommand, profile: Option<String>) -> Result<()> {
    match command {
        AdminBanIpCommand::Add(args) => add(args, profile).await,
        AdminBanIpCommand::List => list(profile).await,
        AdminBanIpCommand::Remove(args) => remove(args, profile).await,
    }
}

async fn add(args: AdminBanIpAddArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let expires_at = parse_optional_expiry(
        args.expiry.duration.as_deref(),
        args.expiry.expires_at.as_deref(),
    )?;
    let request = BanIpAddRequest {
        ip: args.ip,
        reason: args.reason,
        duration: args.expiry.duration,
        expires_at: args.expiry.expires_at,
    };
    api.rpc()
        .await?
        .ban_ip_add(terminal_games::control::rpc_context(), request.clone())
        .await?
        .map_err(anyhow::Error::msg)?;
    let fanout_api = api.clone();
    let region_count = api
        .fanout(|base_url, _| {
            let request = terminal_games::control::BanIpRequest {
                ip: request.ip.clone(),
                reason: request.reason.clone(),
                expires_at,
            };
            let fanout_api = fanout_api.clone();
            async move {
                fanout_api
                    .rpc_at(&base_url)
                    .await?
                    .apply_ban(terminal_games::control::rpc_context(), request)
                    .await?
                    .map_err(anyhow::Error::msg)
            }
        })
        .await?;
    println!("Applied ban across {} regions", region_count);
    Ok(())
}

async fn list(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let bans: Vec<BanEntry> = api
        .rpc()
        .await?
        .ban_ip_list(terminal_games::control::rpc_context())
        .await?
        .map_err(anyhow::Error::msg)?;
    let rows = bans
        .into_iter()
        .map(|ban| {
            vec![
                ban.ip,
                if ban.reason.trim().is_empty() {
                    "-".to_string()
                } else {
                    ban.reason
                },
                format_optional_unix(ban.expires_at),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&["IP", "Reason", "Expires"], &rows);
    Ok(())
}

async fn remove(args: AdminBanIpRemoveArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let request = BanIpRemoveRequest { ip: args.ip };
    api.rpc()
        .await?
        .ban_ip_remove(terminal_games::control::rpc_context(), request.clone())
        .await?
        .map_err(anyhow::Error::msg)?;
    let fanout_api = api.clone();
    let region_count = api
        .fanout(|base_url, _| {
            let request = request.clone();
            let fanout_api = fanout_api.clone();
            async move {
                fanout_api
                    .rpc_at(&base_url)
                    .await?
                    .apply_ban_remove(terminal_games::control::rpc_context(), request)
                    .await?
                    .map_err(anyhow::Error::msg)
            }
        })
        .await?;
    println!("Removed ban across {} regions", region_count);
    Ok(())
}
