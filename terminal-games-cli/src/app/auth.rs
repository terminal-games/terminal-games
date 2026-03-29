// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use dialoguer::Password;
use terminal_games::control::AppTokenClaims;

use super::AppAuthArgs;
use crate::config::normalize_base_url;
use crate::{config::read_secret_stdin, config::save_app_token};

pub(super) async fn run(args: AppAuthArgs) -> Result<()> {
    let token = if let Some(token) = args.token {
        token
    } else if args.token_stdin {
        read_secret_stdin()?
    } else {
        Password::new().with_prompt("App token").interact()?
    };
    let claims = AppTokenClaims::decode(token.trim())?;
    let mut normalized = claims.clone();
    normalized.url = normalize_base_url(&claims.url)?;
    save_app_token(&normalized)?;
    println!(
        "Saved app token for '{}' on {}",
        normalized.shortname, normalized.url
    );
    Ok(())
}
