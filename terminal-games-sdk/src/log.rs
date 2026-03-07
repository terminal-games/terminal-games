// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::OnceLock;

fn forward(level: ::log::Level, message: &str) {
    let len = message.len().min(4096) as u32;
    if len == 0 {
        return;
    }
    let level = match level {
        ::log::Level::Trace => 0,
        ::log::Level::Debug => 1,
        ::log::Level::Info => 2,
        ::log::Level::Warn => 3,
        ::log::Level::Error => 4,
    };
    let _ = unsafe { crate::internal::log(level, message.as_ptr(), len) };
}

#[derive(Clone, Debug)]
struct FilterConfig {
    default_level: ::log::LevelFilter,
    target_levels: Box<[(String, ::log::LevelFilter)]>,
}

impl FilterConfig {
    fn level_for(&self, target: &str) -> ::log::LevelFilter {
        self.target_levels
            .iter()
            .filter(|(prefix, _)| target.starts_with(prefix))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, level)| *level)
            .unwrap_or(self.default_level)
    }
}

struct HostLogger;
static FILTER_CONFIG: OnceLock<FilterConfig> = OnceLock::new();

impl ::log::Log for HostLogger {
    fn enabled(&self, metadata: &::log::Metadata<'_>) -> bool {
        metadata.level() <= filter_config().level_for(metadata.target())
    }

    fn log(&self, record: &::log::Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        forward(record.level(), &record.args().to_string());
    }

    fn flush(&self) {}
}

static LOGGER: HostLogger = HostLogger;

fn filter_config() -> &'static FilterConfig {
    FILTER_CONFIG.get_or_init(|| FilterConfig {
        default_level: ::log::LevelFilter::Trace,
        target_levels: Box::new([]),
    })
}

pub fn try_init() -> Result<(), ::log::SetLoggerError> {
    try_init_with(::log::LevelFilter::Trace, &[])
}

pub fn init() {
    let _ = try_init();
}

fn try_init_targets(
    default_level: ::log::LevelFilter,
    target_levels: Vec<(String, ::log::LevelFilter)>,
) -> Result<(), ::log::SetLoggerError> {
    let _ = FILTER_CONFIG.set(FilterConfig {
        default_level,
        target_levels: target_levels.into_boxed_slice(),
    });
    ::log::set_logger(&LOGGER)?;
    ::log::set_max_level(::log::LevelFilter::Trace);
    Ok(())
}

pub fn try_init_with(
    default_level: ::log::LevelFilter,
    target_levels: &[(&'static str, ::log::LevelFilter)],
) -> Result<(), ::log::SetLoggerError> {
    try_init_targets(
        default_level,
        target_levels
            .iter()
            .map(|(target, level)| ((*target).to_owned(), *level))
            .collect(),
    )
}

pub fn init_with(
    default_level: ::log::LevelFilter,
    target_levels: &[(&'static str, ::log::LevelFilter)],
) {
    let _ = try_init_with(default_level, target_levels);
}

#[macro_export]
macro_rules! init_current_crate {
    () => {
        $crate::log::init_with(
            ::log::LevelFilter::Off,
            &[(module_path!(), ::log::LevelFilter::Trace)],
        )
    };
}

pub use init_current_crate;
