// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::OnceLock;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

fn level_str(level: &tracing::Level) -> &'static str {
    match *level {
        tracing::Level::TRACE => "trace",
        tracing::Level::DEBUG => "debug",
        tracing::Level::INFO => "info",
        tracing::Level::WARN => "warn",
        tracing::Level::ERROR => "error",
    }
}

fn level_to_host(level: &tracing::Level) -> u32 {
    match *level {
        tracing::Level::TRACE => 0,
        tracing::Level::DEBUG => 1,
        tracing::Level::INFO => 2,
        tracing::Level::WARN => 3,
        tracing::Level::ERROR => 4,
    }
}

#[derive(Default)]
struct JsonVisitor {
    message: Option<String>,
    fields: serde_json::Map<String, serde_json::Value>,
}

impl JsonVisitor {
    fn finish(mut self) -> serde_json::Map<String, serde_json::Value> {
        if let Some(msg) = self.message {
            self.fields
                .insert("message".to_owned(), serde_json::Value::String(msg));
        }
        self.fields
    }
}

impl tracing::field::Visit for JsonVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_owned(),
            serde_json::Value::String(format!("{:?}", value)),
        );
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        let v = serde_json::Value::String(value.to_owned());
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        } else {
            self.fields.insert(field.name().to_owned(), v);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(
            field.name().to_owned(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(
            field.name().to_owned(),
            serde_json::Value::Number(serde_json::Number::from(value)),
        );
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name().to_owned(), serde_json::Value::Bool(value));
    }
}

struct HostLayer;

impl<S> Layer<S> for HostLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "level".to_owned(),
            serde_json::Value::String(level_str(event.metadata().level()).to_owned()),
        );
        obj.insert(
            "target".to_owned(),
            serde_json::Value::String(event.metadata().target().to_owned()),
        );
        if let Some(module_path) = event.metadata().module_path() {
            obj.insert(
                "module_path".to_owned(),
                serde_json::Value::String(module_path.to_owned()),
            );
        }
        if let Some(file) = event.metadata().file() {
            obj.insert(
                "file".to_owned(),
                serde_json::Value::String(file.to_owned()),
            );
        }
        if let Some(line) = event.metadata().line() {
            obj.insert("line".to_owned(), serde_json::Value::Number(line.into()));
        }

        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        for (k, v) in visitor.finish() {
            obj.insert(k, v);
        }

        if obj
            .get("message")
            .and_then(|m| m.as_str())
            .map_or(true, |s| s.is_empty())
        {
            return;
        }

        let Ok(message) = serde_json::to_string(&obj) else {
            return;
        };

        let bytes = message.as_bytes();
        if bytes.is_empty() {
            return;
        }

        let len = bytes.len().min(4096);
        let _ = unsafe {
            crate::internal::log(
                level_to_host(event.metadata().level()),
                bytes.as_ptr(),
                len as u32,
            )
        };
    }
}

static INIT: OnceLock<()> = OnceLock::new();

fn do_init(filter: tracing_subscriber::filter::Targets) {
    let _ = INIT.get_or_init(|| {
        tracing_subscriber::registry()
            .with(filter)
            .with(HostLayer)
            .try_init()
            .expect("tracing init");
    });
}

pub fn init_with_filter(filter: tracing_subscriber::filter::Targets) {
    do_init(filter);
}

pub fn try_init() -> Result<(), tracing_subscriber::util::TryInitError> {
    try_init_with(tracing::Level::TRACE, &[])
}

pub fn init() {
    let _ = try_init();
}

pub fn try_init_with(
    default_level: tracing::Level,
    target_levels: &[(&'static str, tracing::Level)],
) -> Result<(), tracing_subscriber::util::TryInitError> {
    let mut targets = tracing_subscriber::filter::Targets::new().with_default(
        tracing_subscriber::filter::LevelFilter::from_level(default_level),
    );
    for (target, level) in target_levels {
        targets = targets.with_target(
            *target,
            tracing_subscriber::filter::LevelFilter::from_level(*level),
        );
    }
    do_init(targets);
    Ok(())
}

pub fn init_with(default_level: tracing::Level, target_levels: &[(&'static str, tracing::Level)]) {
    let _ = try_init_with(default_level, target_levels);
}

pub fn init_current_crate_with_module(module_path: &'static str) {
    let filter = tracing_subscriber::filter::Targets::new()
        .with_default(tracing_subscriber::filter::LevelFilter::OFF)
        .with_target(module_path, tracing_subscriber::filter::LevelFilter::TRACE);
    init_with_filter(filter);
}

#[macro_export]
macro_rules! init_current_crate {
    () => {
        $crate::log::init_current_crate_with_module(module_path!())
    };
}

pub use init_current_crate;
