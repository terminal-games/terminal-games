use super::{
    AppServer, AppState, KV_POLL_ERR_BUFFER_TOO_SMALL, KV_POLL_ERR_INVALID_REQUEST_ID,
    KV_POLL_ERR_REQUEST_FAILED, KV_POLL_PENDING, KV_REQ_ERR_INVALID_INPUT,
    KV_REQ_ERR_TOO_MANY_REQUESTS, KvPendingResult, KvRequestState, MAX_KV_REQUEST_BYTES,
    MAX_KV_REQUESTS, PendingKvRequest,
};
use crate::kv::{KvCheckFailedReason, KvCommand, KvError, KvListPage};
use crate::wasm_abi::HostApiRegistration;
use bytes::Bytes;

const KV_CMD_SET: u32 = 1;
const KV_CMD_DELETE: u32 = 2;
const KV_CMD_CHECK_VALUE: u32 = 3;
const KV_CMD_CHECK_EXISTS: u32 = 4;
const KV_CMD_CHECK_MISSING: u32 = 5;
const KV_CMD_SIZE: usize = 20;
const KV_LIST_REQUEST_HEADER_SIZE: usize = 16;
const KV_LIST_RESPONSE_HEADER_SIZE: usize = 8;
const KV_OPTIONAL_KEY_MISSING: u32 = u32::MAX;
const KV_ERROR_UNAVAILABLE: u32 = 1;
const KV_ERROR_CHECK_FAILED: u32 = 2;
const KV_ERROR_QUOTA_EXCEEDED: u32 = 3;
const KV_CHECK_FAILED_KEY_MISSING: u32 = 1;
const KV_CHECK_FAILED_KEY_EXISTS: u32 = 2;
const KV_CHECK_FAILED_VALUE_MISMATCH: u32 = 3;

inventory::submit! { HostApiRegistration::new("kv_get", "kv_get_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::kv_get_v1)) }
inventory::submit! { HostApiRegistration::new("kv_get_poll", "kv_get_poll_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::kv_get_poll_v1)) }
inventory::submit! { HostApiRegistration::new("kv_exec", "kv_exec_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::kv_exec_v1)) }
inventory::submit! { HostApiRegistration::new("kv_exec_poll", "kv_exec_poll_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::kv_exec_poll_v1)) }
inventory::submit! { HostApiRegistration::new("kv_list", "kv_list_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::kv_list_v1)) }
inventory::submit! { HostApiRegistration::new("kv_list_poll", "kv_list_poll_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::kv_list_poll_v1)) }
inventory::submit! { HostApiRegistration::new("kv_storage_used", "kv_storage_used_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::kv_storage_used_v1)) }
inventory::submit! { HostApiRegistration::new("kv_storage_used_poll", "kv_storage_used_poll_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::kv_storage_used_poll_v1)) }

impl AppServer {
    fn kv_get_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        key_ptr: i32,
        key_len: u32,
    ) -> wasmtime::Result<i32> {
        let key = match Self::read_guest_bytes(&mut caller, key_ptr, key_len) {
            Ok(key) => key,
            Err(code) => return Ok(code),
        };
        let kv_backend = caller.data().ctx.kv_backend.clone();
        let app_id = caller.data().app.app_id.app_id;
        Ok(Self::spawn_kv_get_request(caller.data_mut(), async move {
            kv_backend
                .get(app_id, key)
                .await
                .map_err(|error| sanitize_kv_error("get", app_id, error))
        }))
    }

    fn kv_get_poll_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        request_id: i32,
        value_ptr: i32,
        value_max_len: u32,
        value_len_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let polled =
            match Self::poll_kv_request(caller.data_mut(), request_id, |result| match result {
                KvPendingResult::Get(value) => Ok(value),
                other => Err(other),
            }) {
                Ok(polled) => polled,
                Err(code) => return Ok(code),
            };
        match polled {
            PolledKvRequest::Pending => Ok(KV_POLL_PENDING),
            PolledKvRequest::WrongType => Ok(KV_POLL_ERR_REQUEST_FAILED),
            PolledKvRequest::Ready(value) => match value {
                Some(value) => {
                    let mut memory = Self::memory(&mut caller, "kv_get_poll")?;
                    if let Some(code) = Self::write_guest_bytes(
                        &mut caller,
                        &mut memory,
                        value_ptr,
                        value_max_len,
                        value_len_ptr,
                        &value,
                    )? {
                        Self::store_kv_result(
                            caller.data_mut(),
                            request_id,
                            Ok(KvPendingResult::Get(Some(value))),
                        );
                        return Ok(code);
                    }
                    Ok(1)
                }
                None => Ok(0),
            },
            PolledKvRequest::Failed(error) => {
                let mut memory = Self::memory(&mut caller, "kv_get_poll")?;
                if let Some(code) = Self::write_guest_bytes(
                    &mut caller,
                    &mut memory,
                    value_ptr,
                    value_max_len,
                    value_len_ptr,
                    &encode_kv_error(&error),
                )? {
                    Self::store_kv_result(caller.data_mut(), request_id, Err(error));
                    return Ok(code);
                }
                Ok(KV_POLL_ERR_REQUEST_FAILED)
            }
        }
    }

    fn kv_exec_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        commands_ptr: i32,
        commands_len: u32,
    ) -> wasmtime::Result<i32> {
        let commands = match Self::read_guest_exec_commands(&mut caller, commands_ptr, commands_len)
        {
            Ok(commands) => commands,
            Err(code) => return Ok(code),
        };
        let kv_backend = caller.data().ctx.kv_backend.clone();
        let app_id = caller.data().app.app_id.app_id;
        Ok(Self::spawn_kv_exec_request(caller.data_mut(), async move {
            kv_backend
                .exec(app_id, commands)
                .await
                .map_err(|error| sanitize_kv_error("exec", app_id, error))
        }))
    }

    fn kv_exec_poll_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        request_id: i32,
        error_ptr: i32,
        error_max_len: u32,
        error_len_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let polled =
            match Self::poll_kv_request(caller.data_mut(), request_id, |result| match result {
                KvPendingResult::Exec => Ok(()),
                other => Err(other),
            }) {
                Ok(polled) => polled,
                Err(code) => return Ok(code),
            };
        match polled {
            PolledKvRequest::Pending => Ok(KV_POLL_PENDING),
            PolledKvRequest::WrongType => Ok(KV_POLL_ERR_REQUEST_FAILED),
            PolledKvRequest::Ready(()) => Ok(1),
            PolledKvRequest::Failed(error) => {
                let mut memory = Self::memory(&mut caller, "kv_exec_poll")?;
                if let Some(code) = Self::write_guest_bytes(
                    &mut caller,
                    &mut memory,
                    error_ptr,
                    error_max_len,
                    error_len_ptr,
                    &encode_kv_error(&error),
                )? {
                    Self::store_kv_result(caller.data_mut(), request_id, Err(error));
                    return Ok(code);
                }
                Ok(KV_POLL_ERR_REQUEST_FAILED)
            }
        }
    }

    fn kv_list_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        request_ptr: i32,
        request_len: u32,
    ) -> wasmtime::Result<i32> {
        let request = match Self::read_guest_list_request(&mut caller, request_ptr, request_len) {
            Ok(request) => request,
            Err(code) => return Ok(code),
        };
        let kv_backend = caller.data().ctx.kv_backend.clone();
        let app_id = caller.data().app.app_id.app_id;
        Ok(Self::spawn_kv_list_request(caller.data_mut(), async move {
            let page = kv_backend
                .list_page(
                    app_id,
                    request.prefix,
                    request.start,
                    request.end,
                    request.after,
                )
                .await
                .map_err(|error| sanitize_kv_error("list", app_id, error))?;
            encode_list_page(&page).map_err(|error| {
                tracing::error!(app_id, error = %error, "failed to encode kv list page");
                KvError::Unavailable
            })
        }))
    }

    fn kv_list_poll_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        request_id: i32,
        data_ptr: i32,
        data_max_len: u32,
        data_len_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let polled =
            match Self::poll_kv_request(caller.data_mut(), request_id, |result| match result {
                KvPendingResult::List(value) => Ok(value),
                other => Err(other),
            }) {
                Ok(polled) => polled,
                Err(code) => return Ok(code),
            };
        match polled {
            PolledKvRequest::Pending => Ok(KV_POLL_PENDING),
            PolledKvRequest::WrongType => Ok(KV_POLL_ERR_REQUEST_FAILED),
            PolledKvRequest::Ready(value) => {
                let mut memory = Self::memory(&mut caller, "kv_list_poll")?;
                if let Some(code) = Self::write_guest_bytes(
                    &mut caller,
                    &mut memory,
                    data_ptr,
                    data_max_len,
                    data_len_ptr,
                    &value,
                )? {
                    Self::store_kv_result(
                        caller.data_mut(),
                        request_id,
                        Ok(KvPendingResult::List(value)),
                    );
                    return Ok(code);
                }
                Ok(1)
            }
            PolledKvRequest::Failed(error) => {
                let mut memory = Self::memory(&mut caller, "kv_list_poll")?;
                if let Some(code) = Self::write_guest_bytes(
                    &mut caller,
                    &mut memory,
                    data_ptr,
                    data_max_len,
                    data_len_ptr,
                    &encode_kv_error(&error),
                )? {
                    Self::store_kv_result(caller.data_mut(), request_id, Err(error));
                    return Ok(code);
                }
                Ok(KV_POLL_ERR_REQUEST_FAILED)
            }
        }
    }

    fn kv_storage_used_v1(mut caller: wasmtime::Caller<'_, AppState>) -> wasmtime::Result<i32> {
        let kv_backend = caller.data().ctx.kv_backend.clone();
        let app_id = caller.data().app.app_id.app_id;
        Ok(Self::spawn_kv_request(
            caller.data_mut(),
            async move {
                kv_backend
                    .storage_used(app_id)
                    .await
                    .map_err(|error| sanitize_kv_error("storage_used", app_id, error))
            },
            KvPendingResult::StorageUsed,
        ))
    }

    fn kv_storage_used_poll_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        request_id: i32,
        data_ptr: i32,
        data_max_len: u32,
        data_len_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let polled =
            match Self::poll_kv_request(caller.data_mut(), request_id, |result| match result {
                KvPendingResult::StorageUsed(value) => Ok(value),
                other => Err(other),
            }) {
                Ok(polled) => polled,
                Err(code) => return Ok(code),
            };
        match polled {
            PolledKvRequest::Pending => Ok(KV_POLL_PENDING),
            PolledKvRequest::WrongType => Ok(KV_POLL_ERR_REQUEST_FAILED),
            PolledKvRequest::Ready(value) => {
                let mut memory = Self::memory(&mut caller, "kv_storage_used_poll")?;
                let bytes = value.to_le_bytes();
                if let Some(code) = Self::write_guest_bytes(
                    &mut caller,
                    &mut memory,
                    data_ptr,
                    data_max_len,
                    data_len_ptr,
                    &bytes,
                )? {
                    Self::store_kv_result(
                        caller.data_mut(),
                        request_id,
                        Ok(KvPendingResult::StorageUsed(value)),
                    );
                    return Ok(code);
                }
                Ok(1)
            }
            PolledKvRequest::Failed(error) => {
                let mut memory = Self::memory(&mut caller, "kv_storage_used_poll")?;
                if let Some(code) = Self::write_guest_bytes(
                    &mut caller,
                    &mut memory,
                    data_ptr,
                    data_max_len,
                    data_len_ptr,
                    &encode_kv_error(&error),
                )? {
                    Self::store_kv_result(caller.data_mut(), request_id, Err(error));
                    return Ok(code);
                }
                Ok(KV_POLL_ERR_REQUEST_FAILED)
            }
        }
    }

    fn read_guest_exec_commands(
        caller: &mut wasmtime::Caller<'_, AppState>,
        ptr: i32,
        len: u32,
    ) -> Result<Vec<KvCommand>, i32> {
        let bytes = Self::read_guest_bytes(caller, ptr, len)?;
        if bytes.len() % KV_CMD_SIZE != 0 {
            return Err(KV_REQ_ERR_INVALID_INPUT);
        }

        let mut commands = Vec::with_capacity(bytes.len() / KV_CMD_SIZE);
        let mut write_count = 0_usize;
        for chunk in bytes.chunks_exact(KV_CMD_SIZE) {
            let tag = Self::read_cmd_u32(chunk, 0)?;
            let key_ptr = Self::read_cmd_u32(chunk, 4)? as i32;
            let key_len = Self::read_cmd_u32(chunk, 8)?;
            let value_ptr = Self::read_cmd_u32(chunk, 12)? as i32;
            let value_len = Self::read_cmd_u32(chunk, 16)?;

            let key = Self::read_guest_bytes(caller, key_ptr, key_len)?;
            let value = match tag {
                KV_CMD_SET | KV_CMD_CHECK_VALUE => {
                    Self::read_guest_bytes(caller, value_ptr, value_len)?
                }
                _ if value_len == 0 => Bytes::new(),
                _ => return Err(KV_REQ_ERR_INVALID_INPUT),
            };

            let command = match tag {
                KV_CMD_SET => {
                    write_count += 1;
                    KvCommand::Set { key, value }
                }
                KV_CMD_DELETE => {
                    write_count += 1;
                    KvCommand::Delete { key }
                }
                KV_CMD_CHECK_VALUE => KvCommand::CheckValue { key, value },
                KV_CMD_CHECK_EXISTS => KvCommand::CheckExists { key },
                KV_CMD_CHECK_MISSING => KvCommand::CheckMissing { key },
                _ => return Err(KV_REQ_ERR_INVALID_INPUT),
            };
            if write_count > 1 {
                return Err(KV_REQ_ERR_INVALID_INPUT);
            }
            commands.push(command);
        }
        Ok(commands)
    }

    fn read_cmd_u32(chunk: &[u8], offset: usize) -> Result<u32, i32> {
        let end = offset.checked_add(4).ok_or(KV_REQ_ERR_INVALID_INPUT)?;
        let bytes = chunk.get(offset..end).ok_or(KV_REQ_ERR_INVALID_INPUT)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_guest_list_request(
        caller: &mut wasmtime::Caller<'_, AppState>,
        ptr: i32,
        len: u32,
    ) -> Result<ListRequest, i32> {
        let bytes = Self::read_guest_bytes(caller, ptr, len)?;
        decode_list_request(bytes).map_err(|_| KV_REQ_ERR_INVALID_INPUT)
    }

    fn spawn_kv_get_request<F>(state: &mut AppState, future: F) -> i32
    where
        F: std::future::Future<Output = Result<Option<Bytes>, KvError>> + Send + 'static,
    {
        Self::spawn_kv_request(state, future, KvPendingResult::Get)
    }

    fn spawn_kv_exec_request<F>(state: &mut AppState, future: F) -> i32
    where
        F: std::future::Future<Output = Result<(), KvError>> + Send + 'static,
    {
        Self::spawn_kv_request(state, future, |()| KvPendingResult::Exec)
    }

    fn spawn_kv_list_request<F>(state: &mut AppState, future: F) -> i32
    where
        F: std::future::Future<Output = Result<Bytes, KvError>> + Send + 'static,
    {
        Self::spawn_kv_request(state, future, KvPendingResult::List)
    }

    fn spawn_kv_request<F, T, Map>(state: &mut AppState, future: F, map: Map) -> i32
    where
        F: std::future::Future<Output = Result<T, KvError>> + Send + 'static,
        Map: FnOnce(T) -> KvPendingResult + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let request_id = Self::insert_kv_request_pending(state, rx);
        if request_id < 0 {
            return request_id;
        }
        tokio::spawn(async move {
            let _ = tx.send(future.await.map(map));
        });
        request_id
    }

    fn read_guest_bytes(
        caller: &mut wasmtime::Caller<'_, AppState>,
        ptr: i32,
        len: u32,
    ) -> Result<Bytes, i32> {
        let len = len as usize;
        if len > MAX_KV_REQUEST_BYTES {
            return Err(KV_REQ_ERR_INVALID_INPUT);
        }
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            return Err(KV_REQ_ERR_INVALID_INPUT);
        };
        let mut bytes = vec![0u8; len];
        mem.read(caller, ptr as usize, &mut bytes)
            .map_err(|_| KV_REQ_ERR_INVALID_INPUT)?;
        Ok(Bytes::from(bytes))
    }

    fn memory(
        caller: &mut wasmtime::Caller<'_, AppState>,
        api: &str,
    ) -> wasmtime::Result<wasmtime::Memory> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("{api}: failed to find host memory");
        };
        Ok(mem)
    }

    fn write_guest_bytes(
        caller: &mut wasmtime::Caller<'_, AppState>,
        memory: &mut wasmtime::Memory,
        data_ptr: i32,
        data_max_len: u32,
        data_len_ptr: i32,
        data: &[u8],
    ) -> wasmtime::Result<Option<i32>> {
        let needed = data.len() as u32;
        Self::write_guest_u32(caller, memory, data_len_ptr, needed)?;
        if needed > data_max_len {
            return Ok(Some(KV_POLL_ERR_BUFFER_TOO_SMALL));
        }
        if needed > 0 {
            memory.write(caller, data_ptr as usize, data)?;
        }
        Ok(None)
    }

    fn write_guest_u32(
        caller: &mut wasmtime::Caller<'_, AppState>,
        memory: &mut wasmtime::Memory,
        ptr: i32,
        value: u32,
    ) -> wasmtime::Result<()> {
        Ok(memory.write(caller, ptr as usize, &value.to_le_bytes())?)
    }

    fn advance_kv_request(request: &mut PendingKvRequest) {
        if let KvRequestState::Pending(receiver) = &mut request.state {
            match receiver.try_recv() {
                Ok(result) => {
                    request.state = KvRequestState::Complete(result);
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    request.state = KvRequestState::Complete(Err(KvError::Unavailable));
                }
            }
        }
    }

    fn take_kv_request(state: &mut AppState, request_id: i32) -> Option<PendingKvRequest> {
        let request_id = usize::try_from(request_id).ok()?;
        if request_id >= state.kv_requests.len() {
            return None;
        }
        state.kv_requests[request_id].take()
    }

    fn poll_kv_request<T>(
        state: &mut AppState,
        request_id: i32,
        take_value: impl FnOnce(KvPendingResult) -> Result<T, KvPendingResult>,
    ) -> Result<PolledKvRequest<T>, i32> {
        let Some(mut request) = Self::take_kv_request(state, request_id) else {
            return Err(KV_POLL_ERR_INVALID_REQUEST_ID);
        };
        Self::advance_kv_request(&mut request);

        match request.state {
            KvRequestState::Pending(receiver) => {
                request.state = KvRequestState::Pending(receiver);
                Self::put_kv_request(state, request_id, request);
                Ok(PolledKvRequest::Pending)
            }
            KvRequestState::Complete(Ok(result)) => match take_value(result) {
                Ok(value) => Ok(PolledKvRequest::Ready(value)),
                Err(result) => {
                    Self::store_kv_result(state, request_id, Ok(result));
                    Ok(PolledKvRequest::WrongType)
                }
            },
            KvRequestState::Complete(Err(error)) => Ok(PolledKvRequest::Failed(error)),
        }
    }

    fn store_kv_result(
        state: &mut AppState,
        request_id: i32,
        result: Result<KvPendingResult, KvError>,
    ) {
        Self::put_kv_request(
            state,
            request_id,
            PendingKvRequest {
                state: KvRequestState::Complete(result),
            },
        );
    }

    fn put_kv_request(state: &mut AppState, request_id: i32, request: PendingKvRequest) {
        if let Ok(request_id) = usize::try_from(request_id) {
            if let Some(slot) = state.kv_requests.get_mut(request_id) {
                *slot = Some(request);
            }
        }
    }

    fn insert_kv_request_pending(
        state: &mut AppState,
        receiver: tokio::sync::oneshot::Receiver<Result<KvPendingResult, KvError>>,
    ) -> i32 {
        let pending = PendingKvRequest {
            state: KvRequestState::Pending(receiver),
        };
        for (i, slot) in state.kv_requests.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(pending);
                return i as i32;
            }
        }
        if state.kv_requests.len() >= MAX_KV_REQUESTS {
            return KV_REQ_ERR_TOO_MANY_REQUESTS;
        }
        let request_id = state.kv_requests.len() as i32;
        state.kv_requests.push(Some(pending));
        request_id
    }
}

enum PolledKvRequest<T> {
    Pending,
    Ready(T),
    Failed(KvError),
    WrongType,
}

fn sanitize_kv_error(op: &'static str, app_id: u64, error: KvError) -> KvError {
    tracing::error!(app_id, op, error = %error, "kv request failed");
    match error {
        KvError::CheckFailed(_) | KvError::QuotaExceeded { .. } | KvError::Unavailable => error,
        KvError::Internal(_) => KvError::Unavailable,
    }
}

fn encode_kv_error(error: &KvError) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);
    match error {
        KvError::Unavailable | KvError::Internal(_) => {
            bytes.extend_from_slice(&KV_ERROR_UNAVAILABLE.to_le_bytes());
        }
        KvError::CheckFailed(reason) => {
            bytes.extend_from_slice(&KV_ERROR_CHECK_FAILED.to_le_bytes());
            let reason = match reason {
                KvCheckFailedReason::KeyMissing => KV_CHECK_FAILED_KEY_MISSING,
                KvCheckFailedReason::KeyExists => KV_CHECK_FAILED_KEY_EXISTS,
                KvCheckFailedReason::ValueMismatch => KV_CHECK_FAILED_VALUE_MISMATCH,
            };
            bytes.extend_from_slice(&reason.to_le_bytes());
        }
        KvError::QuotaExceeded {
            used_bytes,
            limit_bytes,
            ..
        } => {
            bytes.extend_from_slice(&KV_ERROR_QUOTA_EXCEEDED.to_le_bytes());
            bytes.extend_from_slice(&used_bytes.to_le_bytes());
            bytes.extend_from_slice(&limit_bytes.to_le_bytes());
        }
    }
    bytes
}

struct ListRequest {
    prefix: Bytes,
    start: Option<Bytes>,
    end: Option<Bytes>,
    after: Option<Bytes>,
}

fn decode_list_request(bytes: Bytes) -> anyhow::Result<ListRequest> {
    if bytes.len() < KV_LIST_REQUEST_HEADER_SIZE {
        anyhow::bail!("kv list request too short");
    }

    let prefix_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let start_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let end_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let after_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap());

    let mut offset = KV_LIST_REQUEST_HEADER_SIZE;
    let prefix = read_list_request_part(&bytes, &mut offset, prefix_len)?;
    let start = if start_len == KV_OPTIONAL_KEY_MISSING {
        None
    } else {
        Some(read_list_request_part(
            &bytes,
            &mut offset,
            start_len as usize,
        )?)
    };
    let end = if end_len == KV_OPTIONAL_KEY_MISSING {
        None
    } else {
        Some(read_list_request_part(
            &bytes,
            &mut offset,
            end_len as usize,
        )?)
    };
    let after = if after_len == KV_OPTIONAL_KEY_MISSING {
        None
    } else {
        Some(read_list_request_part(
            &bytes,
            &mut offset,
            after_len as usize,
        )?)
    };

    if offset != bytes.len() {
        anyhow::bail!("unexpected trailing kv list request bytes");
    }

    Ok(ListRequest {
        prefix,
        start,
        end,
        after,
    })
}

fn read_list_request_part(bytes: &Bytes, offset: &mut usize, len: usize) -> anyhow::Result<Bytes> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("kv list request length overflow"))?;
    if end > bytes.len() {
        anyhow::bail!("unexpected end of kv list request");
    }
    let part = bytes.slice(*offset..end);
    *offset = end;
    Ok(part)
}

fn encode_list_page(page: &KvListPage) -> Result<Bytes, String> {
    let mut out = Vec::with_capacity(
        KV_LIST_RESPONSE_HEADER_SIZE
            + page.next_after.as_ref().map_or(0, Bytes::len)
            + page
                .entries
                .iter()
                .map(|entry| 8 + entry.key.len() + entry.value.len())
                .sum::<usize>(),
    );
    let count = u32::try_from(page.entries.len())
        .map_err(|_| "too many kv list entries to encode".to_string())?;
    out.extend_from_slice(&count.to_le_bytes());
    let next_after_len = page
        .next_after
        .as_ref()
        .map(|next_after| u32::try_from(next_after.len()))
        .transpose()
        .map_err(|_| "kv list cursor too large to encode".to_string())?
        .unwrap_or(KV_OPTIONAL_KEY_MISSING);
    out.extend_from_slice(&next_after_len.to_le_bytes());
    if let Some(next_after) = &page.next_after {
        out.extend_from_slice(next_after);
    }
    for entry in &page.entries {
        let key_len = u32::try_from(entry.key.len())
            .map_err(|_| "kv list key too large to encode".to_string())?;
        let value_len = u32::try_from(entry.value.len())
            .map_err(|_| "kv list value too large to encode".to_string())?;
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(&value_len.to_le_bytes());
        out.extend_from_slice(&entry.key);
        out.extend_from_slice(&entry.value);
    }
    Ok(Bytes::from(out))
}
