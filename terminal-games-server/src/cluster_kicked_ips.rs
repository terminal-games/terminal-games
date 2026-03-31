// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use anyhow::{Context, Result, bail};
use libsql::Value;
use terminal_games::control::ClusterKickedIpEntry;

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

pub async fn load_entries(db: &libsql::Connection) -> Result<Vec<ClusterKickedIpEntry>> {
    let mut rows = db
        .query(
            "SELECT ip, count
             FROM cluster_kicked_ips
             WHERE count > 0
             ORDER BY count DESC, ip ASC",
            (),
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
        });
    }
    Ok(entries)
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
