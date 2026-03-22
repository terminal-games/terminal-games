// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use dialoguer::{Confirm, Input, Password};

use super::AdminAuthArgs;
use crate::config::{
    AdminProfile, CliConfig, load_cli_config, normalize_base_url, read_secret_stdin,
    save_admin_profile, save_cli_config,
};

pub(super) async fn run(args: AdminAuthArgs, profile_override: Option<String>) -> Result<()> {
    let current_config = load_cli_config()?;
    let url = match args.url {
        Some(url) => normalize_base_url(&url)?,
        None => normalize_base_url(
            &Input::<String>::new()
                .with_prompt("Server URL")
                .default("terminalgames.net".to_string())
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

    let profile_name = profile_override.unwrap_or_else(|| "default".to_string());
    save_admin_profile(
        &profile_name,
        AdminProfile {
            url: url.clone(),
            password,
        },
    )?;

    if profile_name == current_config.default_profile
        || Confirm::new()
            .with_prompt(format!(
                "Set '{}' as the default admin profile?",
                profile_name
            ))
            .default(profile_name == "default")
            .interact()?
    {
        save_cli_config(&CliConfig {
            default_profile: profile_name.clone(),
        })?;
    }

    println!("Saved admin profile '{}' for {}", profile_name, url);
    Ok(())
}
