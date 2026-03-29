// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;

use super::load_api;
use crate::config::{format_bytes_per_second, print_table};

pub(super) async fn run(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let mut rows = api
        .region_statuses()
        .await?
        .into_iter()
        .map(|status| {
            vec![
                status.region_id,
                status.current_sessions.to_string(),
                status.max_capacity.to_string(),
                format!("{:.1}%", status.cpu_usage_percent),
                format!(
                    "{}/{} MiB",
                    status.memory_used_bytes / 1024 / 1024,
                    status.memory_total_bytes / 1024 / 1024
                ),
                format_bytes_per_second(status.bandwidth_bytes_per_second),
            ]
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left[0].cmp(&right[0]));
    print_table(
        &[
            "Region",
            "Sessions",
            "Capacity",
            "CPU",
            "Memory",
            "Bandwidth",
        ],
        &rows,
    );
    Ok(())
}
