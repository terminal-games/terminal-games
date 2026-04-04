// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use anyhow::Result;

use crate::config::StoredAppTokenEntry;
use crate::config::{format_seconds, load_app_tokens_for_listing, print_table};
use crate::control_client::AppClient;

pub(super) async fn run() -> Result<()> {
    let mut rows = Vec::new();
    let mut warnings = Vec::new();
    let mut entries_by_url = BTreeMap::<String, Vec<StoredAppTokenEntry>>::new();
    for entry in load_app_tokens_for_listing(None)? {
        entries_by_url
            .entry(entry.claims.url.clone())
            .or_default()
            .push(entry);
    }
    for (url, entries) in entries_by_url {
        let response = match fetch_author_infos_for_server(&entries).await {
            Ok(response) => response,
            Err(error) => {
                for entry in entries {
                    warnings.push(format_author_list_warning(
                        &entry.claims.shortname,
                        &entry.claims.url,
                        &error,
                    ));
                }
                continue;
            }
        };
        for response in response.apps {
            rows.push(vec![
                if response.author_name.trim().is_empty() {
                    "-".to_string()
                } else {
                    response.author_name
                },
                response.shortname,
                url.clone(),
                response.server,
                format_seconds(response.playtime_seconds),
            ]);
        }
        for shortname in response.invalid_shortnames {
            warnings.push(format!(
                "Skipping revoked app token '{}' on {} (401 Unauthorized)",
                shortname, url
            ));
        }
    }
    rows.sort();
    print_table(
        &["App", "Shortname", "Profile", "Server", "Playtime"],
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

async fn fetch_author_infos_for_server(
    entries: &[StoredAppTokenEntry],
) -> Result<terminal_games::control::AppSelfInfoResponse> {
    let request = terminal_games::control::AppSelfInfoRequest {
        tokens: entries.iter().map(|entry| entry.claims.clone()).collect(),
    };
    let mut last_unauthorized = None;
    for entry in entries {
        let client = AppClient::from_claims(entry.claims.clone())?;
        match client.rpc().await {
            Ok(rpc) => {
                return rpc
                    .self_info(terminal_games::control::rpc_context(), request.clone())
                    .await?
                    .map_err(anyhow::Error::msg);
            }
            Err(error) if is_unauthorized_error(&error) => {
                last_unauthorized = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_unauthorized.unwrap_or_else(|| anyhow::anyhow!("no configured app tokens")))
}

fn format_author_list_warning(shortname: &str, url: &str, error: &anyhow::Error) -> String {
    if is_unauthorized_error(error) {
        return format!(
            "Skipping revoked app token '{}' on {} ({})",
            shortname,
            url,
            error.root_cause()
        );
    }
    format!(
        "Skipping app '{}' on {} ({})",
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
