// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use anyhow::{Context, Result, bail};
use terminal_games::control::{ClusterKickedIpEntry, ClusterKickedIpListResponse};

use crate::admission::decode_cidr_blob;

const DEFAULT_RETENTION_DAYS: u64 = 30;
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClusterKickedIpReview {
    pub(crate) entries: Vec<ClusterKickedIpReviewEntry>,
    pub(crate) retention_days: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClusterKickedIpReviewEntry {
    pub(crate) total_count: u64,
    pub(crate) incremented_by: u64,
}

pub(crate) fn empty_review() -> ClusterKickedIpReview {
    ClusterKickedIpReview {
        entries: Vec::new(),
        retention_days: retention_days(),
    }
}

pub(crate) fn retention_days() -> u64 {
    std::env::var("CLUSTER_KICKED_IP_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|days| *days > 0)
        .unwrap_or(DEFAULT_RETENTION_DAYS)
}

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

pub async fn increment_for_enforcement(
    db: &libsql::Connection,
    ips: impl IntoIterator<Item = IpAddr>,
) -> Result<ClusterKickedIpReview> {
    let mut increments = HashMap::<Vec<u8>, u64>::new();
    for ip in ips {
        *increments.entry(normalize(ip)).or_default() += 1;
    }
    if increments.is_empty() {
        return Ok(empty_review());
    }

    let retention_days = retention_days();
    let bucket_start = current_bucket_start();
    let cutoff_bucket_start = retained_bucket_cutoff(bucket_start, retention_days);
    purge_expired_before(db, cutoff_bucket_start).await?;

    let mut entries = Vec::with_capacity(increments.len());

    for (ip, incremented_by) in increments {
        let display_ip = display(&ip)?;
        db.execute(
            "INSERT INTO cluster_kicked_ip_buckets (ip, bucket_start, count)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(ip, bucket_start) DO UPDATE
             SET count = cluster_kicked_ip_buckets.count + excluded.count",
            libsql::params!(ip.clone(), bucket_start, incremented_by as i64),
        )
        .await
        .with_context(|| format!("failed to upsert cluster-kicked ip count for {display_ip}"))?;

        let mut rows = db
            .query(
                "SELECT COALESCE(SUM(count), 0)
                 FROM cluster_kicked_ip_buckets
                 WHERE ip = ?1
                   AND bucket_start >= ?2",
                libsql::params!(ip, cutoff_bucket_start),
            )
            .await
            .with_context(|| {
                format!("failed to load updated cluster-kicked ip count for {display_ip}")
            })?;
        let row = rows
            .next()
            .await
            .with_context(|| {
                format!("failed to read updated cluster-kicked ip row for {display_ip}")
            })?
            .with_context(|| format!("missing updated cluster-kicked ip row for {display_ip}"))?;
        let total_count = row
            .get::<u64>(0)
            .context("missing updated cluster-kicked ip count")?;
        entries.push(ClusterKickedIpReviewEntry {
            total_count,
            incremented_by,
        });
    }

    entries.sort_by(|left, right| {
        right
            .total_count
            .cmp(&left.total_count)
            .then_with(|| right.incremented_by.cmp(&left.incremented_by))
    });
    Ok(ClusterKickedIpReview {
        entries,
        retention_days,
    })
}

pub async fn load_entries_page(
    db: &libsql::Connection,
    limit: i64,
    offset: i64,
) -> Result<Vec<ClusterKickedIpEntry>> {
    let cutoff_bucket_start = retained_bucket_cutoff(current_bucket_start(), retention_days());
    let mut rows = db
        .query(
            "SELECT ip, SUM(count) AS total_count
             FROM cluster_kicked_ip_buckets
             WHERE bucket_start >= ?1
             GROUP BY ip
             HAVING SUM(count) > 0
             ORDER BY total_count DESC, ip ASC
             LIMIT ?2 OFFSET ?3",
            libsql::params!(cutoff_bucket_start, limit, offset),
        )
        .await
        .context("failed to load cluster-kicked ip counts")?;

    let mut entries = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .context("failed to read cluster-kicked ip row")?
    {
        let ip = display(
            &row.get::<Vec<u8>>(0)
                .context("missing cluster-kicked ip value")?,
        )?;
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

    purge_expired(db).await?;
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

pub(crate) async fn purge_expired(db: &libsql::Connection) -> Result<()> {
    let cutoff_bucket_start = retained_bucket_cutoff(current_bucket_start(), retention_days());
    purge_expired_before(db, cutoff_bucket_start).await
}

async fn purge_expired_before(db: &libsql::Connection, cutoff_bucket_start: i64) -> Result<()> {
    db.execute(
        "DELETE FROM cluster_kicked_ip_buckets
         WHERE bucket_start < ?1",
        libsql::params!(cutoff_bucket_start),
    )
    .await
    .context("failed to purge expired cluster-kicked ip buckets")?;
    Ok(())
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

fn current_bucket_start() -> i64 {
    let now = current_unix_seconds();
    now - now.rem_euclid(SECONDS_PER_DAY)
}

fn retained_bucket_cutoff(current_bucket_start: i64, retention_days: u64) -> i64 {
    current_bucket_start - (retention_days.saturating_sub(1) as i64).saturating_mul(SECONDS_PER_DAY)
}
