// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::ShutdownPhase;

use super::{AdminNodesCommand, load_api};
use crate::config::{format_bytes_per_second, print_table};

use super::drain;

pub(super) async fn run(command: AdminNodesCommand, profile: Option<String>) -> Result<()> {
    match command {
        AdminNodesCommand::List => list(profile).await,
        AdminNodesCommand::Drain(command) => drain::run(command, profile).await,
    }
}

async fn list(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let mut rows = api
        .node_statuses()
        .await?
        .into_iter()
        .map(|status| {
            let state_label = drain_state_label(&status);
            vec![
                status.node_id,
                state_label,
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
            "Node",
            "State",
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

fn drain_state_label(status: &terminal_games::control::NodeRuntimeStatus) -> String {
    match status.shutdown.phase {
        ShutdownPhase::Running => "running".to_string(),
        ShutdownPhase::Draining if status.current_sessions == 0 => "drained".to_string(),
        ShutdownPhase::Draining => "draining".to_string(),
        ShutdownPhase::ShuttingDown => "shutting_down".to_string(),
    }
}
