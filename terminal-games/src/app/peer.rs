use std::future::Future;

use super::{
    AppServer, AppState, NODE_LATENCY_ERR_UNKNOWN, PEER_RECV_ERR_CHANNEL_DISCONNECTED,
    PEER_SEND_ERR_CHANNEL_CLOSED, PEER_SEND_ERR_CHANNEL_FULL, PEER_SEND_ERR_DATA_TOO_LARGE,
    PEER_SEND_ERR_INVALID_PEER_COUNT, PEER_SEND_ERR_INVALID_PEER_ID,
};
use crate::{
    mesh::{NodeId, PeerId},
    wasm_abi::HostApiRegistration,
};

inventory::submit! { HostApiRegistration::new("peer_send", "peer_send_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::peer_send_v1)) }
inventory::submit! { HostApiRegistration::new("peer_recv", "peer_recv_v1", 1, |linker, module, import| linker.func_wrap(module, import, AppServer::peer_recv_v1)) }
inventory::submit! { HostApiRegistration::new("node_latency", "node_latency_v1", 1, |linker, module, import| linker.func_wrap_async(module, import, AppServer::node_latency_v1)) }
inventory::submit! { HostApiRegistration::new("peer_list", "peer_list_v1", 1, |linker, module, import| linker.func_wrap_async(module, import, AppServer::peer_list_v1)) }

impl AppServer {
    fn peer_send_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        peer_ids_ptr: i32,
        peer_ids_count: u32,
        data_ptr: i32,
        data_len: u32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("peer_send: failed to find host memory");
        };

        if peer_ids_count == 0 || peer_ids_count > 1024 {
            return Ok(PEER_SEND_ERR_INVALID_PEER_COUNT);
        }

        if data_len == 0 {
            return Ok(0);
        }

        if data_len > 64 * 1024 {
            return Ok(PEER_SEND_ERR_DATA_TOO_LARGE);
        }
        let peer_ids_offset = peer_ids_ptr as usize;
        let peer_ids_count = peer_ids_count as usize;
        let mut peer_ids = Vec::with_capacity(peer_ids_count);
        for i in 0..peer_ids_count {
            let offset = i * PeerId::BYTE_LEN;
            let mut peer_id_bytes = [0u8; PeerId::BYTE_LEN];
            mem.read(&caller, peer_ids_offset + offset, &mut peer_id_bytes)?;
            let peer_id = match crate::mesh::PeerId::from_bytes(peer_id_bytes) {
                Ok(peer_id) => peer_id,
                Err(_) => return Ok(PEER_SEND_ERR_INVALID_PEER_ID),
            };
            peer_ids.push(peer_id);
        }

        let data_offset = data_ptr as usize;
        let data_len = data_len as usize;
        let mut data_buf = vec![0u8; data_len];
        mem.read(&caller, data_offset, &mut data_buf)?;

        match caller.data_mut().app.peer_tx.try_send((peer_ids, data_buf)) {
            Ok(_) => Ok(0),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("peer_send: channel full, message dropped");
                Ok(PEER_SEND_ERR_CHANNEL_FULL)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!("peer_send: channel closed");
                Ok(PEER_SEND_ERR_CHANNEL_CLOSED)
            }
        }
    }

    fn peer_recv_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        from_peer_ptr: i32,
        data_ptr: i32,
        data_max_len: u32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("peer_recv: failed to find host memory");
        };

        let msg = match caller.data_mut().app.peer_rx.try_recv() {
            Ok(msg) => msg,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                return Ok(0);
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                return Ok(PEER_RECV_ERR_CHANNEL_DISCONNECTED);
            }
        };

        let from_peer_offset = from_peer_ptr as usize;
        let peer_id_buf = msg.from_peer().to_bytes();

        mem.write(&mut caller, from_peer_offset, &peer_id_buf)?;

        let data_offset = data_ptr as usize;
        let data_max_len = data_max_len as usize;
        let data = msg.data();
        let data_to_write = std::cmp::min(data.len(), data_max_len);

        mem.write(&mut caller, data_offset, &data[..data_to_write])?;

        Ok(data_to_write as i32)
    }

    fn node_latency_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        (node_ptr,): (i32,),
    ) -> Box<dyn Future<Output = wasmtime::Result<i32>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                wasmtime::bail!("node_latency: failed to find host memory");
            };

            let node_offset = node_ptr as usize;
            let mut node_bytes = [0u8; 4];
            mem.read(&caller, node_offset, &mut node_bytes)?;

            let node_id = match NodeId::try_from(node_bytes) {
                Ok(node_id) => node_id,
                Err(_) => return Ok(NODE_LATENCY_ERR_UNKNOWN),
            };

            let mesh = &caller.data().ctx.mesh;
            match mesh.get_node_latency(node_id).await {
                Some(latency) => Ok(latency.as_millis() as i32),
                None => Ok(NODE_LATENCY_ERR_UNKNOWN),
            }
        })
    }

    fn peer_list_v1(
        mut caller: wasmtime::Caller<'_, AppState>,
        (peer_ids_ptr, length, total_count_ptr): (i32, u32, i32),
    ) -> Box<dyn Future<Output = wasmtime::Result<i32>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                wasmtime::bail!("peer_list: failed to find host memory");
            };

            let app_id = caller.data().app.app_id;
            let mesh = &caller.data().ctx.mesh;
            let mut peers = mesh
                .get_peers_for_app(app_id)
                .await
                .into_iter()
                .collect::<Vec<_>>();
            peers.sort_unstable();

            let total_count = peers.len();
            let length = std::cmp::min(length as usize, 65536);

            let total_count_offset = total_count_ptr as usize;
            mem.write(
                &mut caller,
                total_count_offset,
                &(total_count as u32).to_le_bytes(),
            )?;

            let ptr_offset = peer_ids_ptr as usize;

            for (i, peer_id) in peers.iter().take(length).enumerate() {
                let write_offset = ptr_offset + (i * PeerId::BYTE_LEN);
                let peer_id_bytes = peer_id.to_bytes();
                mem.write(&mut caller, write_offset, &peer_id_bytes)?;
            }

            Ok(length as i32)
        })
    }
}
