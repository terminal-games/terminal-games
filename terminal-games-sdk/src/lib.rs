// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

pub mod ansi_backend;
pub mod app;
pub mod audio;
pub mod log;
pub mod peer;
pub mod terminal;

#[cfg(feature = "network")]
pub mod network;

pub use terminal_games_manifest as manifest;
pub use terminput;

const HOST_API_VERSION_MISMATCH: i32 = -32_000;

fn version_mismatch_error(api: &'static str) -> std::io::Error {
    std::io::Error::other(format!("terminal-games host version mismatch for {api}"))
}

fn host_call_error(api: &'static str, code: i32) -> std::io::Error {
    if code == HOST_API_VERSION_MISMATCH {
        version_mismatch_error(api)
    } else {
        std::io::Error::other(format!("{api} failed with code {code}"))
    }
}

fn host_call_bool(api: &'static str, code: i32) -> std::io::Result<bool> {
    if code < 0 {
        Err(host_call_error(api, code))
    } else {
        Ok(code > 0)
    }
}

#[macro_export]
macro_rules! embed_manifest {
    () => {
        #[used]
        static TERMINAL_GAMES_MANIFEST: &[u8] =
            include_bytes!(concat!(env!("OUT_DIR"), "/terminal-games.embed.json"));
    };
}

mod internal {
    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        #[link_name = "terminal_size_v1"]
        pub(crate) fn terminal_size(width_ptr: *mut u16, height_ptr: *mut u16) -> i32;
        #[link_name = "terminal_cursor_v1"]
        pub(crate) fn terminal_cursor(x_ptr: *mut u16, y_ptr: *mut u16) -> i32;
        #[link_name = "terminal_read_v1"]
        pub(crate) fn terminal_read(address_ptr: *mut u8, address_len: u32) -> i32;
    }

    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        #[link_name = "change_app_v1"]
        pub(crate) fn change_app(address_ptr: *const u8, address_len: u32) -> i32;
        #[link_name = "next_app_ready_v1"]
        pub(crate) fn next_app_ready() -> i32;
        #[link_name = "graceful_shutdown_poll_v1"]
        pub(crate) fn graceful_shutdown_poll() -> i32;
        #[link_name = "new_version_available_poll_v1"]
        pub(crate) fn new_version_available_poll() -> i32;
        #[link_name = "network_info_v1"]
        pub(crate) fn network_info(
            bytes_per_sec_in_ptr: *mut f64,
            bytes_per_sec_out_ptr: *mut f64,
            last_throttled_ms_ptr: *mut i64,
            latency_ms_ptr: *mut i32,
        ) -> i32;
        #[link_name = "terminal_info_v1"]
        pub(crate) fn terminal_info(
            color_mode_ptr: *mut u8,
            has_bg_ptr: *mut i32,
            bg_r_ptr: *mut u8,
            bg_g_ptr: *mut u8,
            bg_b_ptr: *mut u8,
            has_dark_ptr: *mut i32,
            dark_ptr: *mut i32,
        ) -> i32;
    }

    #[cfg(feature = "network")]
    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        #[link_name = "dial_v1"]
        pub(crate) fn dial(address_ptr: *const u8, address_len: u32, mode: u32) -> i32;
        #[link_name = "poll_dial_v1"]
        pub(crate) fn poll_dial(
            dial_id: i32,
            local_addr_ptr: *mut u8,
            local_addr_len_ptr: *mut u32,
            remote_addr_ptr: *mut u8,
            remote_addr_len_ptr: *mut u32,
        ) -> i32;
        #[link_name = "conn_close_v1"]
        pub(crate) fn conn_close(conn_id: i32) -> i32;
        #[link_name = "conn_write_v1"]
        pub(crate) fn conn_write(conn_id: i32, data_ptr: *const u8, data_len: u32) -> i32;
        #[link_name = "conn_read_v1"]
        pub(crate) fn conn_read(conn_id: i32, buf_ptr: *mut u8, buf_len: u32) -> i32;
    }

    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        #[link_name = "peer_send_v1"]
        pub(crate) fn peer_send(
            peer_ids_ptr: *const u8,
            peer_ids_count: u32,
            data_ptr: *const u8,
            data_len: u32,
        ) -> i32;
        #[link_name = "peer_recv_v1"]
        pub(crate) fn peer_recv(
            from_peer_ptr: *mut u8,
            data_ptr: *mut u8,
            data_max_len: u32,
        ) -> i32;
        #[link_name = "peer_list_v1"]
        pub(crate) fn peer_list(
            peer_ids_ptr: *mut u8,
            length: u32,
            total_count_ptr: *mut u32,
        ) -> i32;
        #[link_name = "node_latency_v1"]
        pub(crate) fn node_latency(node_ptr: *const u8) -> i32;
    }

    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        #[link_name = "audio_write_v1"]
        pub(crate) fn audio_write(ptr: *const f32, sample_count: u32) -> i32;
        #[link_name = "audio_info_v1"]
        pub(crate) fn audio_info(
            frame_size_ptr: *mut u32,
            sample_rate_ptr: *mut u32,
            pts_ptr: *mut u64,
            buffer_available_ptr: *mut u32,
        ) -> i32;
    }

    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        #[link_name = "log_v1"]
        pub(crate) fn log(level: u32, msg_ptr: *const u8, msg_len: u32) -> i32;
    }
}
