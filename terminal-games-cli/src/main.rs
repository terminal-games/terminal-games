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
use clap::{Arg, ArgAction, Command as ClapCommand, CommandFactory, Parser, Subcommand, error::ErrorKind};
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
    #[command(hide = true, name = "readme-help")]
    ReadmeHelp,
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
            Command::ReadmeHelp => {
                print_readme_help();
                Ok(())
            }
        };
    }
    if cli.run.wasm_file.is_none() {
        Cli::command()
            .error(ErrorKind::MissingRequiredArgument, "<WASM_FILE>")
            .exit();
    }
    run::run(cli.run).await
}

fn print_readme_help() {
    let mut rows = Vec::new();
    collect_readme_help("", &Cli::command(), &mut rows);
    let width = rows.iter().map(|(left, _)| left.len()).max().unwrap_or(0);
    for (left, right) in rows {
        if right.is_empty() {
            println!("{left}");
        } else {
            println!("{left:<width$}  {right}");
        }
    }
}

fn collect_readme_help(prefix: &str, command: &ClapCommand, rows: &mut Vec<(String, String)>) {
    if !prefix.is_empty() && command.get_subcommands().next().is_none() {
        rows.push(format_command_line(prefix, command));
    }
    for subcommand in command.get_subcommands() {
        if subcommand.is_hide_set() {
            continue;
        }
        let child_prefix = if prefix.is_empty() {
            subcommand.get_name().to_string()
        } else {
            format!("{prefix} {}", subcommand.get_name())
        };
        collect_readme_help(&child_prefix, subcommand, rows);
    }
}

fn should_skip_arg(arg: &Arg, depth: usize) -> bool {
    arg.is_hide_set()
        || matches!(
            arg.get_action(),
            ArgAction::Help | ArgAction::HelpShort | ArgAction::HelpLong | ArgAction::Version
        )
        || (arg.is_global_set() && depth > 1)
}

fn format_command_line(prefix: &str, command: &ClapCommand) -> (String, String) {
    let depth = prefix.split_whitespace().filter(|part| !part.is_empty()).count();
    let usage = command
        .get_arguments()
        .filter(|arg| !should_skip_arg(arg, depth))
        .filter_map(format_arg_usage)
        .collect::<Vec<_>>()
        .join(" ");
    let left = if usage.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix} {usage}")
    };
    let description = command
        .get_about()
        .or_else(|| command.get_long_about())
        .map(|about| about.to_string())
        .unwrap_or_default()
        .trim()
        .to_string();
    let aliases = command.get_visible_aliases().collect::<Vec<_>>();
    let aliases = if aliases.is_empty() {
        String::new()
    } else {
        format!(" [aliases: {}]", aliases.join(", "))
    };
    let right = if description.is_empty() {
        aliases
    } else {
        format!("{description}{aliases}")
    };
    (left, right)
}

fn format_arg_usage(arg: &Arg) -> Option<String> {
    if arg.is_positional() {
        return Some(format!(
            "<{}>",
            arg.get_id().as_str().to_ascii_uppercase().replace('-', "_")
        ));
    }
    let Some(name) = format_flag_name(arg) else {
        return None;
    };
    let mut usage = name;
    if takes_value(arg) {
        let value_names = arg
            .get_value_names()
            .map(|names| names.iter().map(ToString::to_string).collect::<Vec<_>>())
            .unwrap_or_else(|| vec![arg.get_id().as_str().to_ascii_uppercase().replace('-', "_")]);
        for name in value_names {
            usage.push(' ');
            usage.push('<');
            usage.push_str(&name);
            usage.push('>');
        }
    }
    if arg.is_required_set() {
        Some(usage)
    } else {
        Some(format!("[{usage}]"))
    }
}

fn takes_value(arg: &Arg) -> bool {
    matches!(arg.get_action(), ArgAction::Set | ArgAction::Append)
}

fn format_flag_name(arg: &Arg) -> Option<String> {
    arg.get_long()
        .map(|long| format!("--{long}"))
        .or_else(|| arg.get_short().map(|short| format!("-{short}")))
}
