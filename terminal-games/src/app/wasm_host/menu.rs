use super::super::{
    AppServer, AppState, GameDetailsOut, MAX_MENU_REQUESTS, MENU_POLL_ERR_BUFFER_TOO_SMALL,
    MENU_POLL_ERR_INVALID_REQUEST_ID, MENU_POLL_ERR_REQUEST_FAILED, MENU_POLL_PENDING,
    MENU_REQ_ERR_INVALID_INPUT, MENU_REQ_ERR_NOT_MENU_APP, MENU_REQ_ERR_TOO_MANY_REQUESTS,
    MENU_REQ_GAMES_LIST, MENU_REQ_PROFILE_GET, MENU_REQ_PROFILE_SET, MENU_REQ_REPLAY_DELETE,
    MENU_REQ_REPLAYS_LIST, MenuRequestJob, MenuRequestKind, MenuRequestState, MenuSessionState,
    MenuUpdate, PendingMenuRequest,
};
use crate::wasm_abi::HostApiRegistration;

inventory::submit! { HostApiRegistration::new("menu_request", "menu_request_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::menu_request_v1)) }
inventory::submit! { HostApiRegistration::new("menu_poll", "menu_poll_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::menu_poll_v1)) }

impl AppServer {
    fn menu_request_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        typ: i32,
        ptr1: i32,
        len1: u32,
        ptr2: i32,
        len2: u32,
        extra: i64,
    ) -> wasmtime::Result<i32> {
        if caller.data().app.shortname != "menu" {
            return Ok(MENU_REQ_ERR_NOT_MENU_APP);
        }
        let request_id = Self::insert_menu_request_pending(caller.data_mut());
        if request_id < 0 {
            return Ok(request_id);
        }
        let request_id_u = request_id as usize;

        let request = match typ {
            MENU_REQ_GAMES_LIST => MenuRequestJob {
                request_id: request_id_u,
                kind: MenuRequestKind::AppsList {
                    current_shortname: caller.data().app.shortname.clone(),
                },
            },
            MENU_REQ_PROFILE_GET => MenuRequestJob {
                request_id: request_id_u,
                kind: MenuRequestKind::ProfileGet,
            },
            MENU_REQ_PROFILE_SET => {
                if len1 > 1024 || len2 > 1024 {
                    caller.data_mut().menu_requests[request_id_u] = None;
                    return Ok(MENU_REQ_ERR_INVALID_INPUT);
                }
                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    wasmtime::bail!("menu_request: failed to find host memory");
                };
                let mut username_bytes = vec![0u8; len1 as usize];
                let mut locale_bytes = vec![0u8; len2 as usize];
                mem.read(&caller, ptr1 as usize, &mut username_bytes)?;
                mem.read(&caller, ptr2 as usize, &mut locale_bytes)?;
                let Ok(username) = String::from_utf8(username_bytes) else {
                    caller.data_mut().menu_requests[request_id_u] = None;
                    return Ok(MENU_REQ_ERR_INVALID_INPUT);
                };
                let Ok(locale) = String::from_utf8(locale_bytes) else {
                    caller.data_mut().menu_requests[request_id_u] = None;
                    return Ok(MENU_REQ_ERR_INVALID_INPUT);
                };
                MenuRequestJob {
                    request_id: request_id_u,
                    kind: MenuRequestKind::ProfileSet { username, locale },
                }
            }
            MENU_REQ_REPLAYS_LIST => {
                if len1 > 64 {
                    caller.data_mut().menu_requests[request_id_u] = None;
                    return Ok(MENU_REQ_ERR_INVALID_INPUT);
                }
                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    wasmtime::bail!("menu_request: failed to find host memory");
                };
                let mut locale_bytes = vec![0u8; len1 as usize];
                mem.read(&caller, ptr1 as usize, &mut locale_bytes)?;
                let Ok(locale) = String::from_utf8(locale_bytes) else {
                    caller.data_mut().menu_requests[request_id_u] = None;
                    return Ok(MENU_REQ_ERR_INVALID_INPUT);
                };
                MenuRequestJob {
                    request_id: request_id_u,
                    kind: MenuRequestKind::ReplaysList { locale },
                }
            }
            MENU_REQ_REPLAY_DELETE => MenuRequestJob {
                request_id: request_id_u,
                kind: MenuRequestKind::ReplayDelete { created_at: extra },
            },
            _ => {
                caller.data_mut().menu_requests[request_id_u] = None;
                return Ok(MENU_REQ_ERR_INVALID_INPUT);
            }
        };

        if caller.data().menu_request_tx.try_send(request).is_err() {
            caller.data_mut().menu_requests[request_id_u] = None;
            return Ok(MENU_REQ_ERR_TOO_MANY_REQUESTS);
        }

        Ok(request_id)
    }

    fn menu_poll_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        request_id: i32,
        data_ptr: i32,
        data_max_len: u32,
        data_len_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let request_id = request_id as usize;
        {
            let state = caller.data();
            if request_id >= state.menu_requests.len() || state.menu_requests[request_id].is_none()
            {
                return Ok(MENU_POLL_ERR_INVALID_REQUEST_ID);
            }
        }

        while let Ok((id, r)) = caller.data_mut().menu_completed_rx.try_recv() {
            caller.data_mut().completed_menu_results.insert(id, r);
        }
        let completed_result = caller.data_mut().completed_menu_results.remove(&request_id);
        if let Some(result) = completed_result {
            caller.data_mut().menu_requests[request_id] = Some(PendingMenuRequest {
                state: MenuRequestState::Complete(result.map(Vec::into_boxed_slice)),
            });
        }
        let Some(mut request) = caller.data_mut().menu_requests[request_id].take() else {
            return Ok(MENU_POLL_ERR_INVALID_REQUEST_ID);
        };
        if let MenuRequestState::Pending = request.state {
            caller.data_mut().menu_requests[request_id] = Some(request);
            return Ok(MENU_POLL_PENDING);
        }

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("menu_poll: failed to find host memory");
        };

        match request.state {
            MenuRequestState::Complete(Ok(data)) => {
                let needed = data.len() as u32;
                mem.write(&mut caller, data_len_ptr as usize, &needed.to_le_bytes())?;
                if needed > data_max_len {
                    request.state = MenuRequestState::Complete(Ok(data));
                    caller.data_mut().menu_requests[request_id] = Some(request);
                    return Ok(MENU_POLL_ERR_BUFFER_TOO_SMALL);
                }
                if needed > 0 {
                    mem.write(&mut caller, data_ptr as usize, &data)?;
                }
                Ok(1)
            }
            MenuRequestState::Complete(Err(_)) => Ok(MENU_POLL_ERR_REQUEST_FAILED),
            MenuRequestState::Pending => unreachable!(),
        }
    }

    fn insert_menu_request_pending(state: &mut AppState) -> i32 {
        let request = PendingMenuRequest {
            state: MenuRequestState::Pending,
        };
        for (i, slot) in state.menu_requests.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(request);
                return i as i32;
            }
        }
        if state.menu_requests.len() >= MAX_MENU_REQUESTS {
            return MENU_REQ_ERR_TOO_MANY_REQUESTS;
        }
        let request_id = state.menu_requests.len() as i32;
        state.menu_requests.push(Some(request));
        request_id
    }

    pub(in crate::app) async fn load_menu_games(
        db: libsql::Connection,
        current_shortname: String,
    ) -> Result<Vec<u8>, String> {
        #[derive(serde::Serialize)]
        struct AppOut {
            id: u64,
            shortname: String,
            details: serde_json::Value,
        }

        #[derive(serde::Serialize)]
        struct AppsOut {
            apps: Vec<AppOut>,
        }

        let mut rows = db
            .query(
                "SELECT id, shortname, details FROM apps WHERE shortname != ?1 ORDER BY id",
                libsql::params!(current_shortname),
            )
            .await
            .map_err(|e| e.to_string())?;

        let mut apps = Vec::<AppOut>::new();
        while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
            let id = row.get::<u64>(0).map_err(|e| e.to_string())?;
            let shortname = row.get::<String>(1).map_err(|e| e.to_string())?;
            let details_json = row.get::<String>(2).map_err(|e| e.to_string())?;
            let details = serde_json::from_str::<serde_json::Value>(&details_json)
                .unwrap_or_else(|_| serde_json::json!({}));
            let app = AppOut {
                id,
                shortname,
                details,
            };
            apps.push(app);
        }

        serde_json::to_vec(&AppsOut { apps }).map_err(|e| e.to_string())
    }

    pub(in crate::app) async fn load_menu_profile(
        db: libsql::Connection,
        user_id: Option<u64>,
        menu_session: MenuSessionState,
        menu_username: String,
    ) -> (Result<Vec<u8>, String>, MenuUpdate) {
        #[derive(serde::Serialize)]
        struct ProfileOut {
            username: String,
            locale: String,
        }

        let mut profile = ProfileOut {
            username: menu_username,
            locale: menu_session.locale.clone(),
        };

        let mut update = MenuUpdate::default();
        let mut had_row = false;
        if let Some(user_id) = user_id {
            let rows = db
                .query(
                    "SELECT username, locale FROM users WHERE id = ?1 LIMIT 1",
                    libsql::params!(user_id),
                )
                .await
                .map_err(|e| e.to_string());
            let Ok(mut rows) = rows else {
                return (Err(rows.unwrap_err()), update);
            };
            if let Ok(Some(row)) = rows.next().await {
                had_row = true;
                profile.username = row
                    .get::<String>(0)
                    .unwrap_or_else(|_| std::mem::take(&mut profile.username));
                profile.locale = row
                    .get::<String>(1)
                    .unwrap_or_else(|_| std::mem::take(&mut profile.locale));
            }
        }

        let result = serde_json::to_vec(&profile).map_err(|e| e.to_string());
        if had_row {
            update.username = Some(std::mem::take(&mut profile.username));
            update.locale = Some(std::mem::take(&mut profile.locale));
        }
        (result, update)
    }

    pub(in crate::app) async fn save_menu_profile(
        db: libsql::Connection,
        user_id: Option<u64>,
        _menu_session: MenuSessionState,
        username: String,
        locale: String,
    ) -> (Result<Vec<u8>, String>, MenuUpdate) {
        let mut update = MenuUpdate::default();
        if let Some(user_id) = user_id {
            let result = db
                .execute(
                    "UPDATE users SET username = ?1, locale = ?2 WHERE id = ?3",
                    libsql::params!(username.as_str(), locale.as_str(), user_id),
                )
                .await
                .map_err(|e| e.to_string());
            if let Err(e) = result {
                return (Err(e), update);
            }
        }
        update.username = Some(username);
        update.locale = Some(locale);
        (Ok(Vec::new()), update)
    }

    pub(in crate::app) async fn load_menu_replays(
        db: libsql::Connection,
        user_id: Option<u64>,
        mut menu_session: MenuSessionState,
        locale: String,
    ) -> (Result<Vec<u8>, String>, MenuUpdate) {
        #[derive(serde::Serialize, Clone)]
        struct ReplayOut {
            created_at: i64,
            asciinema_url: String,
            app_title: String,
        }

        #[derive(serde::Serialize)]
        struct ReplaysOut {
            replays: Vec<ReplayOut>,
        }

        let mut replays = Vec::<ReplayOut>::new();

        if let Some(user_id) = user_id {
            let mut rows = match db
                .query(
                    "SELECT r.created_at, r.asciinema_url, g.shortname, g.details FROM replays r LEFT JOIN apps g ON r.app_id = g.id WHERE r.user_id = ?1 ORDER BY r.created_at DESC",
                    libsql::params!(user_id),
                )
                .await
                .map_err(|e| e.to_string())
            {
                Ok(r) => r,
                Err(e) => return (Err(e), MenuUpdate::default()),
            };
            loop {
                let row_opt = match rows.next().await.map_err(|e| e.to_string()) {
                    Ok(opt) => opt,
                    Err(e) => return (Err(e), MenuUpdate::default()),
                };
                let Some(row) = row_opt else { break };
                let shortname = row
                    .get::<String>(2)
                    .unwrap_or_else(|_| String::from("unknown"));
                let details = row.get::<String>(3).unwrap_or_default();
                let created_at = match row.get::<i64>(0).map_err(|e| e.to_string()) {
                    Ok(v) => v,
                    Err(e) => return (Err(e), MenuUpdate::default()),
                };
                let asciinema_url = match row.get::<String>(1).map_err(|e| e.to_string()) {
                    Ok(v) => v,
                    Err(e) => return (Err(e), MenuUpdate::default()),
                };
                replays.push(ReplayOut {
                    created_at,
                    asciinema_url,
                    app_title: Self::app_title_from_details(&shortname, &details, &locale),
                });
            }
        } else {
            let session_replays = std::mem::take(&mut menu_session.replays);
            for replay in session_replays.into_iter().rev() {
                let mut shortname = String::new();
                let mut details = String::new();
                if let Ok(mut rows) = db
                    .query(
                        "SELECT shortname, details FROM apps WHERE id = ?1 LIMIT 1",
                        libsql::params!(replay.app_id),
                    )
                    .await
                    && let Ok(Some(row)) = rows.next().await
                {
                    shortname = row.get::<String>(0).unwrap_or_default();
                    details = row.get::<String>(1).unwrap_or_default();
                }
                replays.push(ReplayOut {
                    created_at: replay.created_at,
                    asciinema_url: replay.asciinema_url,
                    app_title: Self::app_title_from_details(&shortname, &details, &locale),
                });
            }
        }

        let result = serde_json::to_vec(&ReplaysOut { replays }).map_err(|e| e.to_string());
        (result, MenuUpdate::default())
    }

    fn app_title_from_details(shortname: &str, details_json: &str, locale: &str) -> String {
        serde_json::from_str::<GameDetailsOut>(details_json)
            .map(|details| details.name.resolve(locale, shortname))
            .unwrap_or_else(|_| shortname.to_string())
    }

    pub(in crate::app) async fn delete_menu_replay(
        db: libsql::Connection,
        user_id: Option<u64>,
        mut menu_session: MenuSessionState,
        created_at: i64,
    ) -> (Result<Vec<u8>, String>, MenuUpdate) {
        let mut update = MenuUpdate::default();
        if let Some(user_id) = user_id {
            let result = db
                .execute(
                    "DELETE FROM replays WHERE user_id = ?1 AND created_at = ?2",
                    libsql::params!(user_id, created_at),
                )
                .await
                .map_err(|e| e.to_string());
            if let Err(e) = result {
                return (Err(e), update);
            }
        } else {
            menu_session.replays.retain(|r| r.created_at != created_at);
            update.replays = Some(std::mem::take(&mut menu_session.replays));
        }
        (Ok(Vec::new()), update)
    }
}
