// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod admin;
mod audio;
mod author;
mod config;
mod control_client;
mod run;

use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand, error::ErrorKind};
use clap_complete::CompleteEnv;
use rustls::crypto::aws_lc_rs;

#[derive(Parser)]
#[command(
    about = "Terminal Games CLI",
    arg_required_else_help = true,
    subcommand_negates_reqs = true,
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    run: run::RunArgs,
}

#[derive(Subcommand)]
enum Command {
    /// Administer running terminal games servers.
    Admin(admin::AdminCli),
    /// Upload and manage games as an author.
    Author(author::AuthorCli),
}

fn main() -> Result<()> {
    let _ = aws_lc_rs::default_provider().install_default();
    CompleteEnv::with_factory(Cli::command).complete();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(command) = cli.command {
        return match command {
            Command::Admin(cli) => admin::run(cli).await,
            Command::Author(cli) => author::run(cli).await,
        };
    }
    if cli.run.wasm_file.is_none() {
        Cli::command()
            .error(ErrorKind::MissingRequiredArgument, "<WASM_FILE>")
            .exit();
    }
    run::run(cli.run).await
}
