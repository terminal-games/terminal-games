// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use dialoguer::{Confirm, Input, Password};

use super::AdminAuthArgs;
use crate::config::{
    AdminProfile, CliConfig, default_url_value, load_cli_config, normalize_base_url,
    read_secret_stdin, save_admin_profile, save_cli_config,
};

pub(super) async fn run(args: AdminAuthArgs, url_override: Option<String>) -> Result<()> {
    let current_config = load_cli_config()?;
    let url = match (args.url.as_deref(), url_override.as_deref()) {
        (Some(url), Some(profile)) => {
            let url = normalize_base_url(url)?;
            let profile = normalize_base_url(profile)?;
            anyhow::ensure!(
                url == profile,
                "admin auth received both --url and --profile with different values"
            );
            url
        }
        (Some(url), None) => normalize_base_url(url)?,
        (None, Some(url)) => normalize_base_url(url)?,
        (None, None) => normalize_base_url(
            &Input::<String>::new()
                .with_prompt("Server URL")
                .default("https://terminalgames.net".to_string())
                .interact_text()?,
        )?,
    };
    let password = if let Some(password) = args.password {
        password
    } else if args.password_stdin {
        read_secret_stdin()?
    } else {
        Password::new()
            .with_prompt("Shared admin secret")
            .interact()?
    };

    save_admin_profile(AdminProfile {
        url: url.clone(),
        password,
    })?;

    if default_url_value()?.as_deref() == Some(url.as_str())
        || Confirm::new()
            .with_prompt(format!("Set '{}' as the default server?", url))
            .default(current_config.default_url.is_none())
            .interact()?
    {
        save_cli_config(&CliConfig {
            default_url: Some(url.clone()),
            app_env_secret_key: current_config.app_env_secret_key.clone(),
        })?;
    }

    println!("Saved admin auth for {}", url);
    Ok(())
}
