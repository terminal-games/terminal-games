// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod create;
mod delete;
mod list;
mod rotate_token;

use anyhow::Result;

use super::AdminAppCommand;

pub(super) async fn run(command: AdminAppCommand, profile: Option<String>) -> Result<()> {
    match command {
        AdminAppCommand::Create(args) => create::run(args, profile).await,
        AdminAppCommand::List => list::run(profile).await,
        AdminAppCommand::RotateToken(args) => rotate_token::run(args, profile).await,
        AdminAppCommand::Delete(args) => delete::run(args, profile).await,
    }
}
