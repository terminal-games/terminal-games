// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::AppSummary;

use super::super::load_api;
use crate::config::{format_seconds, print_table};

pub(super) async fn run(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let app_tokens: Vec<AppSummary> = api.app_list().await?;
    let rows = app_tokens
        .into_iter()
        .map(|app| {
            vec![
                app.app_id.to_string(),
                if app.author_name.trim().is_empty() {
                    "-".to_string()
                } else {
                    app.author_name
                },
                app.shortname,
                format_seconds(app.playtime_seconds),
                if app.stale { "yes" } else { "no" }.to_string(),
                format_imports(&app.imports, &app.stale_imports),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &["App ID", "App", "Shortname", "Playtime", "Stale", "APIs"],
        &rows,
    );
    Ok(())
}

fn format_imports(imports: &[String], stale_imports: &[String]) -> String {
    if imports.is_empty() {
        return "-".to_string();
    }
    let stale_imports = stale_imports
        .iter()
        .collect::<std::collections::HashSet<_>>();
    imports
        .iter()
        .map(|import| {
            if stale_imports.contains(import) {
                format!("{import} [old]")
            } else {
                import.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}
