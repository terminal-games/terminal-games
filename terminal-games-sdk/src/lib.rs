pub mod ansi_backend;
pub mod terminal;

#[cfg(feature = "network")]
pub mod network;

pub use terminput;

mod internal {
    #[link(wasm_import_module = "terminal_games")]
    unsafe extern "C" {
        pub(crate) fn terminal_size(width_ptr: *mut u16, height_ptr: *mut u16);
        pub(crate) fn terminal_read(address_ptr: *mut u8, address_len: u32) -> i32;

        #[cfg(feature = "network")]
        pub(crate) fn dial(address_ptr: *const u8, address_len: u32, mode: u32) -> i32;
        #[cfg(feature = "network")]
        pub(crate) fn conn_write(conn_id: i32, address_ptr: *const u8, address_len: u32) -> i32;
        #[cfg(feature = "network")]
        pub(crate) fn conn_read(conn_id: i32, address_ptr: *mut u8, address_len: u32) -> i32;
    }
}
