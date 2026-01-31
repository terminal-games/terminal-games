// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

pub mod ansi_backend;
pub mod app;
pub mod audio;
pub mod peer;
pub mod terminal;

#[cfg(feature = "network")]
pub mod network;

pub use terminput;

mod internal {
    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        pub(crate) fn terminal_size(width_ptr: *mut u16, height_ptr: *mut u16);
        pub(crate) fn terminal_cursor(x_ptr: *mut u16, y_ptr: *mut u16);
        pub(crate) fn terminal_read(address_ptr: *mut u8, address_len: u32) -> i32;

        pub(crate) fn change_app(address_ptr: *const u8, address_len: u32) -> i32;
        pub(crate) fn next_app_ready() -> i32;
        pub(crate) fn graceful_shutdown_poll() -> i32;

        #[cfg(feature = "network")]
        pub(crate) fn dial(address_ptr: *const u8, address_len: u32, mode: u32) -> i32;
        #[cfg(feature = "network")]
        pub(crate) fn poll_dial(
            dial_id: i32,
            local_addr_ptr: *mut u8,
            local_addr_len_ptr: *mut u32,
            remote_addr_ptr: *mut u8,
            remote_addr_len_ptr: *mut u32,
        ) -> i32;
        #[cfg(feature = "network")]
        pub(crate) fn conn_close(conn_id: i32) -> i32;
        #[cfg(feature = "network")]
        pub(crate) fn conn_write(conn_id: i32, data_ptr: *const u8, data_len: u32) -> i32;
        #[cfg(feature = "network")]
        pub(crate) fn conn_read(conn_id: i32, buf_ptr: *mut u8, buf_len: u32) -> i32;

        pub(crate) fn peer_send(
            peer_ids_ptr: *const u8,
            peer_ids_count: u32,
            data_ptr: *const u8,
            data_len: u32,
        ) -> i32;
        pub(crate) fn peer_recv(
            from_peer_ptr: *mut u8,
            data_ptr: *mut u8,
            data_max_len: u32,
        ) -> i32;
        pub(crate) fn peer_list(
            peer_ids_ptr: *mut u8,
            length: u32,
            total_count_ptr: *mut u32,
        ) -> i32;
        pub(crate) fn region_latency(region_ptr: *const u8) -> i32;
        pub(crate) fn network_info(
            bytes_per_sec_in_ptr: *mut f64,
            bytes_per_sec_out_ptr: *mut f64,
            last_throttled_ms_ptr: *mut i64,
            latency_ms_ptr: *mut i32,
        ) -> i32;

        pub(crate) fn audio_write(ptr: *const f32, sample_count: u32) -> i32;
        pub(crate) fn audio_info(
            frame_size_ptr: *mut u32,
            sample_rate_ptr: *mut u32,
            pts_ptr: *mut u64,
            buffer_available_ptr: *mut u32,
        ) -> i32;
    }
}
