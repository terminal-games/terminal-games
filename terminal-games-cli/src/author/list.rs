// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;

use crate::config::{format_seconds, load_author_tokens_for_listing, print_table};
use crate::control_client::AuthorClient;

pub(super) async fn run() -> Result<()> {
    let mut rows = Vec::new();
    let mut warnings = Vec::new();
    for entry in load_author_tokens_for_listing(None)? {
        let client = AuthorClient::from_claims(entry.claims.clone())?;
        let response = match fetch_author_info(&client).await {
            Ok(response) => response,
            Err(error) => {
                warnings.push(format_author_list_warning(
                    &entry.profile,
                    &entry.claims.shortname,
                    &entry.claims.url,
                    &error,
                ));
                continue;
            }
        };
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
    if !warnings.is_empty() {
        eprintln!("");
    }
    for warning in warnings {
        eprintln!("{warning}");
    }
    Ok(())
}

async fn fetch_author_info(
    client: &AuthorClient,
) -> Result<terminal_games::control::AuthorSelfResponse> {
    client
        .rpc()
        .await?
        .self_info(terminal_games::control::rpc_context())
        .await?
        .map_err(anyhow::Error::msg)
}

fn format_author_list_warning(
    profile: &str,
    shortname: &str,
    url: &str,
    error: &anyhow::Error,
) -> String {
    if is_unauthorized_error(error) {
        return format!(
            "Skipping revoked author token '{}:{}' on {} ({})",
            profile,
            shortname,
            url,
            error.root_cause()
        );
    }
    format!(
        "Skipping author '{}:{}' on {} ({})",
        profile,
        shortname,
        url,
        error.root_cause()
    )
}

fn is_unauthorized_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains("401 Unauthorized"))
}
