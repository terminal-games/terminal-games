// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::ClusterKickedIpListRequest;

use super::{AdminClusterIpKicksArgs, load_api};
use crate::config::print_table;

pub(super) async fn run(args: AdminClusterIpKicksArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let response = api
        .cluster_kicked_ip_list(ClusterKickedIpListRequest {
            page: args.page,
            page_size: args.page_size,
            exclude_banned: args.exclude_banned,
        })
        .await?;
    let rows = response
        .entries
        .into_iter()
        .map(|entry| {
            vec![
                entry.ip,
                entry.count.to_string(),
                if entry.is_banned { "yes" } else { "no" }.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&["IP", "Count", "Banned"], &rows);
    if response.has_more {
        println!();
        println!(
            "More results available. Next page: --page {}",
            args.page.saturating_add(1)
        );
    }
    Ok(())
}
