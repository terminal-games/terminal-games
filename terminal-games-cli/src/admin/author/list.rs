// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::AuthorSummary;

use super::super::load_api;
use crate::config::{format_seconds, print_table};

pub(super) async fn run(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let authors: Vec<AuthorSummary> = api
        .rpc()
        .await?
        .author_list(terminal_games::control::rpc_context())
        .await?
        .map_err(anyhow::Error::msg)?;
    let rows = authors
        .into_iter()
        .map(|author| {
            vec![
                author.author_id.to_string(),
                if author.author_name.trim().is_empty() {
                    "-".to_string()
                } else {
                    author.author_name
                },
                author.shortname,
                format_seconds(author.playtime_seconds),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&["Author ID", "Author", "Shortname", "Playtime"], &rows);
    Ok(())
}
