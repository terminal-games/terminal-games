// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod delete;
mod list;
mod set;

use anyhow::Result;

use super::AppEnvCommand;

pub(super) async fn run(command: AppEnvCommand, profile: Option<String>) -> Result<()> {
    match command {
        AppEnvCommand::List(args) => list::run(args, profile).await,
        AppEnvCommand::Set(args) => set::run(args, profile).await,
        AppEnvCommand::Delete(args) => delete::run(args, profile).await,
    }
}
