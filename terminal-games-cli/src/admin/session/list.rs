// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;

use super::super::load_api;
use crate::config::{format_duration, print_table};

pub(super) async fn run(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let mut sessions = api.all_sessions().await?;
    sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    let rows = sessions
        .into_iter()
        .map(|session| {
            vec![
                session.session_id,
                session
                    .user_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                session.node_id,
                session.transport,
                session.shortname,
                format_duration(session.duration_seconds),
                session.username,
                session.ip_address,
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "Session",
            "User ID",
            "Node",
            "Transport",
            "Shortname",
            "Duration",
            "Username",
            "IP",
        ],
        &rows,
    );
    Ok(())
}
