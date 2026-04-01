// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use anyhow::{Context, Result, bail};
use libsql::Value;
use terminal_games::control::{ClusterKickedIpEntry, ClusterKickedIpListResponse};

use crate::admission::decode_cidr_blob;

pub fn normalize(ip: IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(addr) => addr.octets().to_vec(),
        IpAddr::V6(addr) => addr.octets()[..8].to_vec(),
    }
}

pub fn display(ip: &[u8]) -> Result<String> {
    match ip.len() {
        4 => Ok(Ipv4Addr::from(<[u8; 4]>::try_from(ip).unwrap()).to_string()),
        8 => {
            let mut octets = [0_u8; 16];
            octets[..8].copy_from_slice(ip);
            Ok(format!("{}/64", Ipv6Addr::from(octets)))
        }
        len => bail!("invalid cluster-kicked ip blob length: {len}"),
    }
}

pub async fn increment(db: &libsql::Connection, ip: &[u8]) -> Result<()> {
    db.execute(
        "INSERT INTO cluster_kicked_ips (ip, count)
         VALUES (?1, 1)
         ON CONFLICT(ip) DO UPDATE
         SET count = cluster_kicked_ips.count + 1",
        libsql::params!(ip),
    )
    .await
    .context("failed to upsert cluster-kicked ip count")?;
    Ok(())
}

pub async fn load_entries_page(
    db: &libsql::Connection,
    limit: i64,
    offset: i64,
) -> Result<Vec<ClusterKickedIpEntry>> {
    let mut rows = db
        .query(
            "SELECT ip, count
             FROM cluster_kicked_ips
             WHERE count > 0
             ORDER BY count DESC, ip ASC
             LIMIT ?1 OFFSET ?2",
            libsql::params!(limit, offset),
        )
        .await
        .context("failed to load cluster-kicked ip counts")?;

    let mut entries = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .context("failed to read cluster-kicked ip row")?
    {
        let ip = match row
            .get_value(0)
            .context("missing cluster-kicked ip value")?
        {
            Value::Blob(bytes) => display(&bytes)?,
            Value::Text(text) => display_legacy_text(&text)?,
            other => bail!("unexpected cluster-kicked ip value type: {other:?}"),
        };
        entries.push(ClusterKickedIpEntry {
            ip,
            count: row
                .get::<u64>(1)
                .context("missing cluster-kicked ip count")?,
            is_banned: false,
        });
    }
    Ok(entries)
}

pub async fn load_visible_page(
    db: &libsql::Connection,
    offset: usize,
    limit: usize,
    exclude_banned: bool,
) -> Result<ClusterKickedIpListResponse> {
    const PAGE_SIZE: usize = 64;

    let active_bans = load_active_ban_cidrs(db).await?;
    let mut entries = Vec::with_capacity(limit);
    let mut matched_offset = 0_usize;
    let mut sql_offset = 0_i64;
    let mut has_more = false;

    loop {
        let page = load_entries_page(db, PAGE_SIZE as i64, sql_offset).await?;
        if page.is_empty() {
            break;
        }
        let page_len = page.len();
        for mut entry in page {
            entry.is_banned = cluster_kicked_ip_is_banned(&entry.ip, &active_bans);
            if exclude_banned && entry.is_banned {
                continue;
            }
            if matched_offset < offset {
                matched_offset += 1;
                continue;
            }
            if entries.len() < limit {
                entries.push(entry);
            } else {
                has_more = true;
                break;
            }
        }
        if has_more || page_len < PAGE_SIZE {
            break;
        }
        sql_offset += PAGE_SIZE as i64;
    }

    Ok(ClusterKickedIpListResponse { entries, has_more })
}

fn display_legacy_text(text: &str) -> Result<String> {
    if let Ok(addr) = text.parse::<Ipv4Addr>() {
        return Ok(addr.to_string());
    }
    if let Some(prefix) = text.strip_suffix("/64") {
        let addr = prefix
            .parse::<Ipv6Addr>()
            .with_context(|| format!("invalid legacy ipv6 cluster-kicked ip: {text}"))?;
        let masked = Ipv6Addr::from(u128::from(addr) & (!0_u128 << 64));
        return Ok(format!("{masked}/64"));
    }
    bail!("invalid legacy cluster-kicked ip value: {text}")
}

async fn load_active_ban_cidrs(db: &libsql::Connection) -> Result<Vec<ipnet::IpNet>> {
    let mut rows = db
        .query(
            "SELECT cidr, expires_at
             FROM ip_bans
             ORDER BY inserted_at DESC, cidr ASC",
            (),
        )
        .await
        .context("failed to load active ip bans")?;
    let now = current_unix_seconds();
    let mut bans = Vec::new();
    while let Some(row) = rows.next().await.context("failed to read ip ban row")? {
        let expires_at = row.get::<Option<i64>>(1).context("missing ip ban expiry")?;
        if expires_at.is_some_and(|expires_at| expires_at <= now) {
            continue;
        }
        bans.push(
            decode_cidr_blob(&row.get::<Vec<u8>>(0).context("missing ip ban cidr")?)
                .map_err(anyhow::Error::msg)?,
        );
    }
    Ok(bans)
}

fn cluster_kicked_ip_is_banned(ip: &str, bans: &[ipnet::IpNet]) -> bool {
    if let Ok(addr) = ip.parse::<IpAddr>() {
        return bans.iter().any(|ban| ban.contains(&addr));
    }

    let Ok(prefix) = ip.parse::<ipnet::IpNet>() else {
        return false;
    };
    bans.iter()
        .any(|ban| ban.contains(&prefix) || prefix.contains(ban))
}

fn current_unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
