// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::mesh::{
    AppRuntimeUpdateKind, AppRuntimeUpdateMessage, BuildId, ContentHash, hash_bytes,
};

#[derive(Clone, Default)]
pub struct AppRuntimeRegistry {
    inner: Arc<Mutex<RegistryState>>,
}

pub struct AppRuntimeSession {
    update_available: Arc<AtomicBool>,
    registry: Weak<Mutex<RegistryState>>,
    wasm_hash: ContentHash,
}

#[derive(Default)]
struct RegistryState {
    apps: HashMap<u64, AppRuntime>,
    modules: HashMap<ContentHash, wasmtime::Module>,
    active_sessions: HashMap<ContentHash, usize>,
}

struct AppRuntime {
    build_id: BuildId,
    subscribers: Vec<Weak<AtomicBool>>,
    latest_updated_at_ns: i64,
}

impl AppRuntimeRegistry {
    pub fn subscribe(
        &self,
        app_id: u64,
        build_id: BuildId,
        updated_at_ns: i64,
    ) -> AppRuntimeSession {
        let mut state = self.inner.lock().expect("app registry poisoned");
        let app = state
            .apps
            .entry(app_id)
            .or_insert_with(|| AppRuntime::new(build_id, updated_at_ns));
        app.publish(build_id, updated_at_ns);
        let update_available = Arc::new(AtomicBool::new(false));
        app.subscribe(&update_available);
        *state.active_sessions.entry(build_id.wasm_hash).or_default() += 1;
        AppRuntimeSession {
            update_available,
            registry: Arc::downgrade(&self.inner),
            wasm_hash: build_id.wasm_hash,
        }
    }

    pub fn load_or_compile_module(
        &self,
        wasm_bytes: &[u8],
        engine: &wasmtime::Engine,
    ) -> anyhow::Result<wasmtime::Module> {
        let module_key = hash_bytes(wasm_bytes);

        {
            let state = self.inner.lock().expect("app registry poisoned");
            if let Some(module) = state.modules.get(&module_key) {
                return Ok(module.clone());
            }
        }

        let module = wasmtime::Module::from_binary(engine, wasm_bytes)?;

        let mut state = self.inner.lock().expect("app registry poisoned");
        Ok(state.modules.entry(module_key).or_insert(module).clone())
    }

    pub fn apply_update(&self, update: AppRuntimeUpdateMessage) -> bool {
        let mut state = self.inner.lock().expect("app registry poisoned");
        let changed = match update.kind {
            AppRuntimeUpdateKind::Published => {
                let app = state
                    .apps
                    .entry(update.app_id)
                    .or_insert_with(|| AppRuntime::new(update.build_id, update.updated_at_ns));
                app.publish(update.build_id, update.updated_at_ns)
            }
            AppRuntimeUpdateKind::Deleted => state.apps.remove(&update.app_id).is_some(),
        };
        if changed {
            state.prune_modules();
        }
        changed
    }

    pub fn sync_snapshot(&self, snapshot: Vec<AppRuntimeUpdateMessage>) {
        let snapshot_ids = snapshot
            .iter()
            .map(|update| update.app_id)
            .collect::<HashSet<_>>();
        let mut state = self.inner.lock().expect("app registry poisoned");
        state.apps.retain(|app_id, _| snapshot_ids.contains(app_id));
        for update in snapshot {
            if update.kind == AppRuntimeUpdateKind::Deleted {
                state.apps.remove(&update.app_id);
                continue;
            }
            let app = state
                .apps
                .entry(update.app_id)
                .or_insert_with(|| AppRuntime::new(update.build_id, update.updated_at_ns));
            app.publish(update.build_id, update.updated_at_ns);
        }
        state.prune_modules();
    }
}

impl AppRuntimeSession {
    pub fn update_available(&self) -> &Arc<AtomicBool> {
        &self.update_available
    }
}

impl Drop for AppRuntimeSession {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut state = registry.lock().expect("app registry poisoned");
        match state.active_sessions.get_mut(&self.wasm_hash) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                state.active_sessions.remove(&self.wasm_hash);
            }
            None => {}
        }
        state.prune_modules();
    }
}

impl RegistryState {
    fn prune_modules(&mut self) {
        let live_wasm_hashes = self
            .apps
            .values()
            .map(|app| app.build_id.wasm_hash)
            .chain(self.active_sessions.keys().copied())
            .collect::<HashSet<_>>();
        self.modules
            .retain(|module_hash, _| live_wasm_hashes.contains(module_hash));
    }
}

impl AppRuntime {
    fn new(build_id: BuildId, latest_updated_at_ns: i64) -> Self {
        Self {
            build_id,
            subscribers: Vec::new(),
            latest_updated_at_ns,
        }
    }

    fn subscribe(&mut self, update_available: &Arc<AtomicBool>) {
        self.subscribers.push(Arc::downgrade(update_available));
    }

    fn publish(&mut self, build_id: BuildId, updated_at_ns: i64) -> bool {
        if updated_at_ns < self.latest_updated_at_ns {
            return false;
        }
        self.latest_updated_at_ns = updated_at_ns;
        let changed = self.build_id != build_id;
        self.build_id = build_id;
        if changed {
            self.subscribers.retain(|subscriber| {
                let Some(subscriber) = subscriber.upgrade() else {
                    return false;
                };
                subscriber.store(true, Ordering::Relaxed);
                true
            });
        }
        changed
    }
}
