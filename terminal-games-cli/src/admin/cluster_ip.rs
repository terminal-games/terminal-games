// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::ClusterKickedIpEntry;

use super::load_api;
use crate::config::print_table;

pub(super) async fn run(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let ips: Vec<ClusterKickedIpEntry> = api.cluster_kicked_ip_list().await?;
    let rows = ips
        .into_iter()
        .map(|entry| vec![entry.ip, entry.count.to_string()])
        .collect::<Vec<_>>();
    print_table(&["IP", "Count"], &rows);
    Ok(())
}
