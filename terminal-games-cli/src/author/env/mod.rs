// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod delete;
mod list;
mod set;

use anyhow::Result;

use super::AuthorEnvCommand;

pub(super) async fn run(command: AuthorEnvCommand) -> Result<()> {
    match command {
        AuthorEnvCommand::List(args) => list::run(args).await,
        AuthorEnvCommand::Set(args) => set::run(args).await,
        AuthorEnvCommand::Delete(args) => delete::run(args).await,
    }
}
