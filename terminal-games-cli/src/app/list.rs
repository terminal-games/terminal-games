// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use anyhow::Result;
use terminal_games::control::{
    AppSelfInfoRequest, AppSelfInfoResponse, AppSelfInfoTokenStatus, AppTokenClaims,
};

use crate::config::{format_imports, format_seconds, load_app_tokens_for_listing, print_table};
use crate::control_client::{connect_app_rpc_fallback, is_unauthorized_error};

type TokenGroups = BTreeMap<String, Vec<AppTokenClaims>>;

pub(super) async fn run() -> Result<()> {
    let mut rows = Vec::new();
    let mut warnings = Vec::new();
    let mut groups_by_url = BTreeMap::<String, TokenGroups>::new();
    for entry in load_app_tokens_for_listing(None)? {
        let claims = entry.claims;
        groups_by_url
            .entry(claims.url.clone())
            .or_default()
            .entry(claims.shortname.clone())
            .or_default()
            .push(claims);
    }
    for (url, groups) in groups_by_url {
        match fetch_author_infos_for_server(&groups).await {
            Ok(response) => {
                let token_statuses = response
                    .token_statuses
                    .into_iter()
                    .map(|status| (status.shortname.clone(), status))
                    .collect::<BTreeMap<_, _>>();
                rows.extend(response.apps.into_iter().map(|app| {
                    vec![
                        if app.author_name.trim().is_empty() {
                            "-".to_string()
                        } else {
                            app.author_name
                        },
                        app.shortname,
                        url.clone(),
                        app.server,
                        format_seconds(app.playtime_seconds),
                        if app.stale { "yes" } else { "no" }.to_string(),
                        format_imports(&app.imports, &app.stale_imports),
                    ]
                }));
                warnings.extend(collect_group_warnings(&url, &groups, &token_statuses));
            }
            Err(error) => warnings.extend(groups.iter().map(|(shortname, claims)| {
                format_group_fetch_warning(shortname, &url, claims.len(), &error)
            })),
        }
    }
    rows.sort();
    warnings.sort();
    print_table(
        &[
            "App",
            "Shortname",
            "Profile",
            "Server",
            "Playtime",
            "Stale",
            "APIs",
        ],
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

async fn fetch_author_infos_for_server(groups: &TokenGroups) -> Result<AppSelfInfoResponse> {
    let claims = groups
        .values()
        .flat_map(|claims| claims.iter().cloned())
        .collect::<Vec<_>>();
    connect_app_rpc_fallback(&claims)
        .await?
        .self_info(
            terminal_games::control::rpc_context(),
            AppSelfInfoRequest { tokens: claims },
        )
        .await?
        .map_err(anyhow::Error::msg)
}

fn collect_group_warnings(
    url: &str,
    groups: &TokenGroups,
    token_statuses: &BTreeMap<String, AppSelfInfoTokenStatus>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    for (shortname, claims) in groups {
        let fallback_status;
        let status = if let Some(status) = token_statuses.get(shortname) {
            status
        } else {
            fallback_status = AppSelfInfoTokenStatus {
                shortname: shortname.clone(),
                valid_tokens: 0,
                invalid_tokens: claims.len() as u32,
            };
            &fallback_status
        };
        let warning = format_group_warning(shortname, url, claims.len(), status);
        if let Some(warning) = warning {
            warnings.push(warning);
        }
    }
    warnings
}

fn format_group_warning(
    shortname: &str,
    url: &str,
    token_count: usize,
    status: &AppSelfInfoTokenStatus,
) -> Option<String> {
    if status.invalid_tokens == 0 {
        return None;
    }
    if token_count == 1 {
        return Some(format!(
            "Skipping invalid app token '{}' on {}",
            shortname, url
        ));
    }
    let details = format!(
        "{} invalid token{}",
        status.invalid_tokens,
        if status.invalid_tokens == 1 { "" } else { "s" }
    );
    Some(if status.valid_tokens > 0 {
        format!(
            "Ignoring duplicate app tokens for '{}' on {} ({})",
            shortname, url, details
        )
    } else {
        format!("Skipping '{}' on {} ({})", shortname, url, details)
    })
}

fn format_group_fetch_warning(
    shortname: &str,
    url: &str,
    token_count: usize,
    error: &anyhow::Error,
) -> String {
    if token_count == 1 && is_unauthorized_error(error) {
        format!(
            "Skipping invalid app token '{}' on {} ({})",
            shortname,
            url,
            error.root_cause()
        )
    } else {
        format!(
            "Skipping app '{}' on {} ({})",
            shortname,
            url,
            error.root_cause()
        )
    }
}
