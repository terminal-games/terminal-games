use std::collections::BTreeSet;

use anyhow::Context as _;

pub const HOST_API_MODULE: &str = "terminal_games";
pub const HOST_API_VERSION_MISMATCH: i32 = -32_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HostSubsystem {
    App,
    Terminal,
    Net,
    Peer,
    Audio,
    Log,
    Menu,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostApi {
    pub subsystem: HostSubsystem,
    pub stem: &'static str,
    current_import: &'static str,
    current_version: u32,
}

impl HostApi {
    pub const fn new(
        subsystem: HostSubsystem,
        stem: &'static str,
        current_import: &'static str,
        current_version: u32,
    ) -> Self {
        Self {
            subsystem,
            stem,
            current_import,
            current_version,
        }
    }

    pub const fn current_import(self) -> &'static str {
        self.current_import
    }

    pub const fn current_version(self) -> u32 {
        self.current_version
    }
}

#[rustfmt::skip]
pub mod app {
    use super::*;

    pub const CHANGE_APP: HostApi = HostApi::new(HostSubsystem::App, "change_app", "change_app_v1", 1);
    pub const NEXT_APP_READY: HostApi = HostApi::new(HostSubsystem::App, "next_app_ready", "next_app_ready_v1", 1);
    pub const GRACEFUL_SHUTDOWN_POLL: HostApi = HostApi::new(HostSubsystem::App, "graceful_shutdown_poll", "graceful_shutdown_poll_v1", 1);
    pub const NEW_VERSION_AVAILABLE_POLL: HostApi = HostApi::new(HostSubsystem::App, "new_version_available_poll", "new_version_available_poll_v1", 1);
    pub const NETWORK_INFO: HostApi = HostApi::new(HostSubsystem::App, "network_info", "network_info_v1", 1);
    pub const TERMINAL_INFO: HostApi = HostApi::new(HostSubsystem::App, "terminal_info", "terminal_info_v1", 1);

    pub const FUNCTIONS: &[HostApi] = &[
        CHANGE_APP,
        NEXT_APP_READY,
        GRACEFUL_SHUTDOWN_POLL,
        NEW_VERSION_AVAILABLE_POLL,
        NETWORK_INFO,
        TERMINAL_INFO,
    ];
}

#[rustfmt::skip]
pub mod terminal {
    use super::*;

    pub const READ: HostApi = HostApi::new(HostSubsystem::Terminal, "terminal_read", "terminal_read_v1", 1);
    pub const SIZE: HostApi = HostApi::new(HostSubsystem::Terminal, "terminal_size", "terminal_size_v1", 1);
    pub const CURSOR: HostApi = HostApi::new(HostSubsystem::Terminal, "terminal_cursor", "terminal_cursor_v1", 1);

    pub const FUNCTIONS: &[HostApi] = &[READ, SIZE, CURSOR];
}

#[rustfmt::skip]
pub mod net {
    use super::*;

    pub const DIAL: HostApi = HostApi::new(HostSubsystem::Net, "dial", "dial_v1", 1);
    pub const POLL_DIAL: HostApi = HostApi::new(HostSubsystem::Net, "poll_dial", "poll_dial_v1", 1);
    pub const CONN_CLOSE: HostApi = HostApi::new(HostSubsystem::Net, "conn_close", "conn_close_v1", 1);
    pub const CONN_WRITE: HostApi = HostApi::new(HostSubsystem::Net, "conn_write", "conn_write_v1", 1);
    pub const CONN_READ: HostApi = HostApi::new(HostSubsystem::Net, "conn_read", "conn_read_v1", 1);

    pub const FUNCTIONS: &[HostApi] = &[DIAL, POLL_DIAL, CONN_CLOSE, CONN_WRITE, CONN_READ];
}

#[rustfmt::skip]
pub mod peer {
    use super::*;

    pub const SEND: HostApi = HostApi::new(HostSubsystem::Peer, "peer_send", "peer_send_v1", 1);
    pub const RECV: HostApi = HostApi::new(HostSubsystem::Peer, "peer_recv", "peer_recv_v1", 1);
    pub const NODE_LATENCY: HostApi = HostApi::new(HostSubsystem::Peer, "node_latency", "node_latency_v1", 1);
    pub const LIST: HostApi = HostApi::new(HostSubsystem::Peer, "peer_list", "peer_list_v1", 1);

    pub const FUNCTIONS: &[HostApi] = &[SEND, RECV, NODE_LATENCY, LIST];
}

#[rustfmt::skip]
pub mod audio {
    use super::*;

    pub const WRITE: HostApi = HostApi::new(HostSubsystem::Audio, "audio_write", "audio_write_v1", 1);
    pub const INFO: HostApi = HostApi::new(HostSubsystem::Audio, "audio_info", "audio_info_v1", 1);

    pub const FUNCTIONS: &[HostApi] = &[WRITE, INFO];
}

#[rustfmt::skip]
pub mod log {
    use super::*;

    pub const LOG: HostApi = HostApi::new(HostSubsystem::Log, "log", "log_v1", 1);

    pub const FUNCTIONS: &[HostApi] = &[LOG];
}

#[rustfmt::skip]
pub mod menu {
    use super::*;

    pub const REQUEST: HostApi = HostApi::new(HostSubsystem::Menu, "menu_request", "menu_request_v1", 1);
    pub const POLL: HostApi = HostApi::new(HostSubsystem::Menu, "menu_poll", "menu_poll_v1", 1);

    pub const FUNCTIONS: &[HostApi] = &[REQUEST, POLL];
}

pub const ALL_FUNCTIONS: &[HostApi] = &[
    app::CHANGE_APP,
    app::NEXT_APP_READY,
    app::GRACEFUL_SHUTDOWN_POLL,
    app::NEW_VERSION_AVAILABLE_POLL,
    app::NETWORK_INFO,
    app::TERMINAL_INFO,
    terminal::READ,
    terminal::SIZE,
    terminal::CURSOR,
    net::DIAL,
    net::POLL_DIAL,
    net::CONN_CLOSE,
    net::CONN_WRITE,
    net::CONN_READ,
    peer::SEND,
    peer::RECV,
    peer::NODE_LATENCY,
    peer::LIST,
    audio::WRITE,
    audio::INFO,
    log::LOG,
    menu::REQUEST,
    menu::POLL,
];

pub fn guest_host_api_inventory(wasm: &[u8]) -> anyhow::Result<Vec<String>> {
    let mut imports_out = BTreeSet::new();
    for payload in wasmparser::Parser::new(0).parse_all(wasm) {
        if let wasmparser::Payload::ImportSection(imports) =
            payload.context("failed to parse wasm imports")?
        {
            for import in imports {
                let import = import.context("failed to parse wasm import group")?;
                for import in import {
                    let (_, import) = import.context("failed to parse wasm import")?;
                    if import.module == HOST_API_MODULE {
                        imports_out.insert(import.name.to_string());
                    }
                }
            }
        }
    }
    Ok(imports_out.into_iter().collect())
}

pub fn is_host_api_import_out_of_date(import: &str) -> bool {
    if ALL_FUNCTIONS
        .iter()
        .any(|api| api.current_import() == import)
    {
        return false;
    }
    let Some((stem, version)) = parse_import(import) else {
        return true;
    };
    ALL_FUNCTIONS
        .iter()
        .find(|api| api.stem == stem)
        .is_none_or(|api| version != api.current_version())
}

fn parse_import(import: &str) -> Option<(&str, u32)> {
    let (name, version) = import.rsplit_once("_v")?;
    Some((name, version.parse().ok()?))
}
