// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::OnceLock;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

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

impl tracing::field::Visit for JsonVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let s = format!("{:?}", value);
        if field.name() == "message" {
            self.message = Some(s.trim_matches('"').to_string());
        } else {
            self.fields.insert(field.name().to_owned(), serde_json::Value::String(s));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        } else {
            self.fields.insert(field.name().to_owned(), serde_json::Value::String(value.to_owned()));
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(field.name().to_owned(), serde_json::Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(field.name().to_owned(), serde_json::Value::Number(value.into()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields.insert(field.name().to_owned(), serde_json::Value::Bool(value));
    }
}

struct HostLayer;

impl<S> Layer<S> for HostLayer
where
    S: tracing::Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut obj = serde_json::Map::new();
        let lvl = event.metadata().level();
        obj.insert("level".into(), serde_json::Value::String(match *lvl {
            tracing::Level::TRACE => "trace",
            tracing::Level::DEBUG => "debug",
            tracing::Level::INFO => "info",
            tracing::Level::WARN => "warn",
            tracing::Level::ERROR => "error",
        }.to_string()));
        obj.insert("target".into(), serde_json::Value::String(event.metadata().target().to_string()));
        if let Some(m) = event.metadata().module_path() {
            obj.insert("module_path".into(), serde_json::Value::String(m.to_string()));
        }
        if let Some(f) = event.metadata().file() {
            obj.insert("file".into(), serde_json::Value::String(f.to_string()));
        }
        if let Some(l) = event.metadata().line() {
            obj.insert("line".into(), serde_json::Value::Number(l.into()));
        }
        let mut v = JsonVisitor::default();
        event.record(&mut v);
        if let Some(msg) = v.message {
            obj.insert("message".into(), serde_json::Value::String(msg));
        }
        for (k, val) in v.fields {
            obj.insert(k, val);
        }
        if obj.get("message").and_then(|m| m.as_str()).map_or(true, |s| s.is_empty()) {
            return;
        }
        let Ok(msg) = serde_json::to_string(&obj) else { return };
        let bytes = msg.as_bytes();
        if !bytes.is_empty() {
            let _ = unsafe {
                crate::internal::log(level_to_host(event.metadata().level()), bytes.as_ptr(), bytes.len().min(4096) as u32)
            };
        }
    }
}

static INIT: OnceLock<()> = OnceLock::new();

pub fn init_current_crate_with_module(module_path: &'static str) {
    let _ = INIT.get_or_init(|| {
        tracing_subscriber::registry()
            .with(tracing_subscriber::filter::Targets::new()
                .with_default(tracing_subscriber::filter::LevelFilter::OFF)
                .with_target(module_path, tracing_subscriber::filter::LevelFilter::TRACE))
            .with(HostLayer)
            .try_init()
            .expect("tracing init");
    });
}

#[macro_export]
macro_rules! init_current_crate {
    () => { $crate::log::init_current_crate_with_module(module_path!()) };
}
pub use init_current_crate;
