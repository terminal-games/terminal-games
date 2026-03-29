// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use anyhow::Result;
use terminal_games::control::{
    TickerAddRequest, TickerEntry, TickerRemoveRequest, TickerReorderRequest,
};

use super::{
    AdminTickerAddArgs, AdminTickerCommand, AdminTickerRemoveArgs, AdminTickerReorderArgs,
    format_optional_unix, load_api, refresh_status_bar_state,
};
use crate::config::print_table;

pub(super) async fn run(command: AdminTickerCommand, profile: Option<String>) -> Result<()> {
    match command {
        AdminTickerCommand::List => list(profile).await,
        AdminTickerCommand::Add(args) => add(args, profile).await,
        AdminTickerCommand::Reorder(args) => reorder(args, profile).await,
        AdminTickerCommand::Remove(args) => remove(args, profile).await,
    }
}

async fn list(profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    let tickers: Vec<TickerEntry> = api.ticker_list().await?;
    let rows = tickers
        .into_iter()
        .map(|ticker| {
            vec![
                ticker.sort_order.to_string(),
                ticker.ticker_id.to_string(),
                ticker.content,
                format_optional_unix(ticker.expires_at),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&["Order", "Ticker ID", "Content", "Expires"], &rows);
    Ok(())
}

async fn add(args: AdminTickerAddArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    api.rpc()
        .await?
        .ticker_add(
            terminal_games::control::rpc_context(),
            TickerAddRequest {
                content: args.content,
                duration: args.expiry.duration,
                expires_at: args.expiry.expires_at,
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    refresh_status_bar_state(&api).await?;
    println!("Added ticker");
    Ok(())
}

async fn reorder(args: AdminTickerReorderArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    api.rpc()
        .await?
        .ticker_reorder(
            terminal_games::control::rpc_context(),
            TickerReorderRequest {
                ticker_ids: args.ticker_ids.clone(),
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    refresh_status_bar_state(&api).await?;
    println!(
        "Reordered tickers: {}",
        args.ticker_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(())
}

async fn remove(args: AdminTickerRemoveArgs, profile: Option<String>) -> Result<()> {
    let api = load_api(profile.as_deref())?;
    api.rpc()
        .await?
        .ticker_remove(
            terminal_games::control::rpc_context(),
            TickerRemoveRequest {
                ticker_id: args.ticker_id,
            },
        )
        .await?
        .map_err(anyhow::Error::msg)?;
    refresh_status_bar_state(&api).await?;
    println!("Removed ticker {}", args.ticker_id);
    Ok(())
}
