// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{BroadcastLevel, BroadcastRequest};

use super::{AdminBroadcastArgs, BroadcastLevelArg, load_api, parse_nodes_arg};

pub(super) async fn run(args: AdminBroadcastArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let request = BroadcastRequest {
        level: match args.level {
            BroadcastLevelArg::Info => BroadcastLevel::Info,
            BroadcastLevelArg::Warning => BroadcastLevel::Warning,
            BroadcastLevelArg::Error => BroadcastLevel::Error,
        },
        nodes: parse_nodes_arg(args.nodes.as_deref()),
        message: args.message,
        duration: args.duration,
    };
    api.fanout(|rpc| {
        let request = request.clone();
        async move {
            rpc.broadcast(terminal_games::control::rpc_context(), request)
                .await?
                .map_err(anyhow::Error::msg)
        }
    })
    .await?;
    println!("Broadcast applied");
    Ok(())
}
