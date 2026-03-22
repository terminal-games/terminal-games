// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};

use crate::config::{format_seconds, load_author_tokens_for_listing, print_table};
use crate::control_client::AuthorClient;

pub(super) async fn run() -> Result<()> {
    let mut rows = Vec::new();
    for entry in load_author_tokens_for_listing(None)? {
        let client = AuthorClient::from_claims(entry.claims.clone())?;
        let response = client
            .rpc()
            .await?
            .self_info(terminal_games::control::rpc_context())
            .await?
            .map_err(anyhow::Error::msg)
            .with_context(|| {
                format!(
                    "failed to fetch author info for '{}' on {}",
                    entry.claims.shortname, entry.claims.url
                )
            })?;
        rows.push(vec![
            entry.profile,
            if response.author_name.trim().is_empty() {
                "-".to_string()
            } else {
                response.author_name
            },
            response.shortname,
            response.server,
            format_seconds(response.playtime_seconds),
        ]);
    }
    rows.sort();
    print_table(
        &["Profile", "Author", "Shortname", "Server", "Playtime"],
        &rows,
    );
    Ok(())
}
