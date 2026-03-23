// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use sha2::{Digest, Sha256};

use crate::mesh::{GameRuntimeUpdateKind, GameRuntimeUpdateMessage};

#[derive(Clone, Default)]
pub struct GameRuntimeRegistry {
    inner: Arc<Mutex<RegistryState>>,
}

#[derive(Default)]
struct RegistryState {
    games: HashMap<u64, GameRuntime>,
}

struct GameRuntime {
    latest_version: Arc<AtomicU64>,
    module: Option<CachedModule>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct WasmModuleKey([u8; 32]);

struct CachedModule {
    key: WasmModuleKey,
    module: wasmtime::Module,
}

impl GameRuntimeRegistry {
    pub fn subscribe(&self, game_id: u64, current_version: u64) -> Arc<AtomicU64> {
        let mut state = self.inner.lock().expect("game registry poisoned");
        let game = state
            .games
            .entry(game_id)
            .or_insert_with(|| GameRuntime::new(current_version));
        game.publish_version(current_version);
        game.latest_version.clone()
    }

    pub fn load_or_compile_module(
        &self,
        game_id: u64,
        wasm_bytes: &[u8],
        engine: &wasmtime::Engine,
    ) -> anyhow::Result<wasmtime::Module> {
        let module_key = WasmModuleKey::from_wasm(wasm_bytes);

        {
            let mut state = self.inner.lock().expect("game registry poisoned");
            let game = state.games.entry(game_id).or_insert_with(|| GameRuntime::new(0));
            if let Some(module) = game.cached_module(module_key) {
                return Ok(module);
            }
        }

        let module = wasmtime::Module::from_binary(engine, wasm_bytes)?;

        let mut state = self.inner.lock().expect("game registry poisoned");
        let game = state.games.entry(game_id).or_insert_with(|| GameRuntime::new(0));
        if let Some(cached) = game.cached_module(module_key) {
            return Ok(cached);
        }
        game.module = Some(CachedModule::new(module_key, module.clone()));
        Ok(module)
    }

    pub fn apply_update(&self, update: GameRuntimeUpdateMessage) -> bool {
        let mut state = self.inner.lock().expect("game registry poisoned");
        match update.kind {
            GameRuntimeUpdateKind::Published => {
                let game = state
                    .games
                    .entry(update.game_id)
                    .or_insert_with(|| GameRuntime::new(update.version));
                game.publish_version(update.version)
            }
            GameRuntimeUpdateKind::Deleted => state.games.remove(&update.game_id).is_some(),
        }
    }

    pub fn sync_snapshot(&self, snapshot: Vec<GameRuntimeUpdateMessage>) {
        let snapshot_ids = snapshot.iter().map(|update| update.game_id).collect::<HashSet<_>>();
        let mut state = self.inner.lock().expect("game registry poisoned");
        state.games.retain(|game_id, _| snapshot_ids.contains(game_id));
        for update in snapshot {
            if update.kind == GameRuntimeUpdateKind::Deleted {
                state.games.remove(&update.game_id);
                continue;
            }
            let game = state
                .games
                .entry(update.game_id)
                .or_insert_with(|| GameRuntime::new(update.version));
            game.publish_version(update.version);
        }
    }

    #[cfg(test)]
    fn module_cache_len(&self) -> usize {
        self.inner
            .lock()
            .expect("game registry poisoned")
            .games
            .values()
            .filter(|game| game.module.is_some())
            .count()
    }

    #[cfg(test)]
    fn has_game_state(&self, game_id: u64) -> bool {
        self.inner
            .lock()
            .expect("game registry poisoned")
            .games
            .contains_key(&game_id)
    }
}

impl GameRuntime {
    fn new(initial_version: u64) -> Self {
        Self {
            latest_version: Arc::new(AtomicU64::new(initial_version)),
            module: None,
        }
    }

    fn publish_version(&self, version: u64) -> bool {
        let current = self.latest_version.load(Ordering::Acquire);
        if version <= current {
            return false;
        }
        self.latest_version.store(version, Ordering::Release);
        true
    }

    fn cached_module(&self, module_key: WasmModuleKey) -> Option<wasmtime::Module> {
        self.module
            .as_ref()
            .filter(|cached| cached.key == module_key)
            .map(|cached| cached.module.clone())
    }
}

impl WasmModuleKey {
    fn from_wasm(wasm_bytes: &[u8]) -> Self {
        Self(Sha256::digest(wasm_bytes).into())
    }
}

impl CachedModule {
    fn new(key: WasmModuleKey, module: wasmtime::Module) -> Self {
        Self { key, module }
    }
}
