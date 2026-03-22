// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod kick;
mod list;
mod spy;

use anyhow::Result;

use super::AdminSessionCommand;

pub(super) async fn run(command: AdminSessionCommand, profile: Option<String>) -> Result<()> {
    match command {
        AdminSessionCommand::List => list::run(profile).await,
        AdminSessionCommand::Kick(args) => kick::run(args, profile).await,
        AdminSessionCommand::Spy(args) => spy::session_spy(args, profile).await,
    }
}
