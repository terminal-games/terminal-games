use std::{net::SocketAddr, sync::Arc};

use rustls_platform_verifier::BuilderVerifierExt;

use super::super::{
    AppServer, AppState, CONN_ERR_CONNECTION_ERROR, CONN_ERR_INVALID_CONN_ID, Connection,
    DIAL_ERR_ADDRESS_TOO_LONG, DIAL_ERR_TOO_MANY_CONNECTIONS, MAX_CONNECTIONS,
    POLL_DIAL_ERR_CONNECTION_FAILED, POLL_DIAL_ERR_DNS_RESOLUTION, POLL_DIAL_ERR_INVALID_DIAL_ID,
    POLL_DIAL_ERR_INVALID_DNS_NAME, POLL_DIAL_ERR_NOT_GLOBALLY_REACHABLE,
    POLL_DIAL_ERR_TASK_FAILED, POLL_DIAL_ERR_TLS_HANDSHAKE, POLL_DIAL_ERR_TOO_MANY_CONNECTIONS,
    POLL_DIAL_PENDING, PendingDial, Stream, is_globally_reachable,
};
use crate::{wasm_abi, wasm_abi::HOST_API_MODULE};

impl AppServer {
    #[rustfmt::skip]
    pub(super) fn link_net_host_functions(
        linker: &mut wasmtime::Linker<AppState>,
    ) -> anyhow::Result<()> {
        linker.func_wrap(HOST_API_MODULE, wasm_abi::net::DIAL.current_import(), Self::host_dial)?;
        linker.func_wrap(HOST_API_MODULE, wasm_abi::net::POLL_DIAL.current_import(), Self::poll_dial)?;
        linker.func_wrap(HOST_API_MODULE, wasm_abi::net::CONN_CLOSE.current_import(), Self::conn_close)?;
        linker.func_wrap(HOST_API_MODULE, wasm_abi::net::CONN_WRITE.current_import(), Self::conn_write)?;
        linker.func_wrap(HOST_API_MODULE, wasm_abi::net::CONN_READ.current_import(), Self::conn_read)?;
        Ok(())
    }

    fn host_dial(
        mut caller: wasmtime::Caller<'_, AppState>,
        address_ptr: i32,
        address_len: u32,
        mode: u32,
    ) -> wasmtime::Result<i32> {
        let mut buf = [0u8; 256];
        if address_len as usize >= buf.len() {
            return Ok(DIAL_ERR_ADDRESS_TOO_LONG);
        }
        let len = address_len as usize;
        let offset = address_ptr as usize;

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("dial: failed to find host memory");
        };

        mem.read(&caller, offset, &mut buf[..len])?;

        if caller.data().conn_manager.active_connection_count() >= MAX_CONNECTIONS {
            return Ok(DIAL_ERR_TOO_MANY_CONNECTIONS);
        }

        let address = String::from_utf8_lossy(&buf[..len]).to_string();

        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let result = Self::do_connect(address, mode).await;
            let _ = tx.send(result);
        });

        let pending = PendingDial { receiver: rx };
        let pending_dials = &mut caller.data_mut().conn_manager.pending_dials;

        for (i, slot) in pending_dials.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(pending);
                return Ok(i as i32);
            }
        }
        let slot_id = pending_dials.len() as i32;
        pending_dials.push(Some(pending));
        Ok(slot_id)
    }

    async fn do_connect(
        address: String,
        mode: u32,
    ) -> Result<(Stream, SocketAddr, SocketAddr), i32> {
        let addrs: Vec<SocketAddr> = match tokio::net::lookup_host(&address).await {
            Ok(iter) => iter.collect(),
            Err(e) => {
                tracing::error!("do_connect: DNS resolution failed: {:?}", e);
                return Err(POLL_DIAL_ERR_DNS_RESOLUTION);
            }
        };

        if addrs.is_empty() {
            return Err(POLL_DIAL_ERR_DNS_RESOLUTION);
        }

        if !addrs.iter().all(|addr| is_globally_reachable(addr.ip())) {
            return Err(POLL_DIAL_ERR_NOT_GLOBALLY_REACHABLE);
        }

        let tcp_stream = match tokio::net::TcpStream::connect(addrs.as_slice()).await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::error!("do_connect: TcpStream::connect failed: {:?}", e);
                return Err(POLL_DIAL_ERR_CONNECTION_FAILED);
            }
        };

        let local_addr = tcp_stream.local_addr().unwrap_or_else(|_| {
            SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
        });
        let remote_addr = tcp_stream.peer_addr().unwrap_or(addrs[0]);

        let stream = if mode == 1 {
            let hostname: String = address.split(':').next().unwrap_or(&address).to_string();

            let mut config =
                match tokio_rustls::rustls::ClientConfig::builder().with_platform_verifier() {
                    Ok(builder) => builder.with_no_client_auth(),
                    Err(e) => {
                        tracing::error!("do_connect: failed to initialize TLS verifier: {:?}", e);
                        return Err(POLL_DIAL_ERR_TLS_HANDSHAKE);
                    }
                };
            config.alpn_protocols.push(b"h2".to_vec());
            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

            let dnsname = match tokio_rustls::rustls::pki_types::ServerName::try_from(hostname) {
                Ok(name) => name,
                Err(_) => {
                    tracing::error!("do_connect: invalid DNS name");
                    return Err(POLL_DIAL_ERR_INVALID_DNS_NAME);
                }
            };

            let tls_stream = match connector.connect(dnsname, tcp_stream).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("do_connect: TLS handshake failed: {:?}", e);
                    return Err(POLL_DIAL_ERR_TLS_HANDSHAKE);
                }
            };

            Stream::Tls(tls_stream)
        } else {
            Stream::Tcp(tcp_stream)
        };

        Ok((stream, local_addr, remote_addr))
    }

    fn poll_dial(
        mut caller: wasmtime::Caller<'_, AppState>,
        dial_id: i32,
        local_addr_ptr: i32,
        local_addr_len_ptr: i32,
        remote_addr_ptr: i32,
        remote_addr_len_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let dial_id_usize = dial_id as usize;

        {
            let conn_manager = &caller.data().conn_manager;
            if dial_id_usize >= conn_manager.pending_dials.len() {
                return Ok(POLL_DIAL_ERR_INVALID_DIAL_ID);
            }
            if conn_manager.pending_dials[dial_id_usize].is_none() {
                return Ok(POLL_DIAL_ERR_INVALID_DIAL_ID);
            }
        }

        let mut pending = caller.data_mut().conn_manager.pending_dials[dial_id_usize]
            .take()
            .ok_or_else(|| wasmtime::Error::msg("pending dial unexpectedly missing"))?;

        match pending.receiver.try_recv() {
            Ok(Ok((stream, local_addr, remote_addr))) => {
                let conn_manager = &mut caller.data_mut().conn_manager;
                let slot = conn_manager.find_free_conn_slot();

                if slot.is_none() {
                    return Ok(POLL_DIAL_ERR_TOO_MANY_CONNECTIONS);
                }

                let Some(slot_id) = slot else {
                    return Ok(POLL_DIAL_ERR_TOO_MANY_CONNECTIONS);
                };
                let conn = Connection { stream };

                if slot_id < conn_manager.connections.len() {
                    conn_manager.connections[slot_id] = Some(conn);
                } else {
                    conn_manager.connections.push(Some(conn));
                }

                let local_addr_str = local_addr.to_string();
                let remote_addr_str = remote_addr.to_string();

                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    wasmtime::bail!("failed to find host memory");
                };

                mem.write(
                    &mut caller,
                    local_addr_ptr as usize,
                    local_addr_str.as_bytes(),
                )?;
                mem.write(
                    &mut caller,
                    local_addr_len_ptr as usize,
                    &(local_addr_str.len() as u32).to_le_bytes(),
                )?;
                mem.write(
                    &mut caller,
                    remote_addr_ptr as usize,
                    remote_addr_str.as_bytes(),
                )?;
                mem.write(
                    &mut caller,
                    remote_addr_len_ptr as usize,
                    &(remote_addr_str.len() as u32).to_le_bytes(),
                )?;

                tracing::info!(conn_id = slot_id, %local_addr, %remote_addr, "dial completed");
                Ok(slot_id as i32)
            }
            Ok(Err(error_code)) => Ok(error_code),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                caller.data_mut().conn_manager.pending_dials[dial_id_usize] = Some(pending);
                Ok(POLL_DIAL_PENDING)
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => Ok(POLL_DIAL_ERR_TASK_FAILED),
        }
    }

    fn conn_close(
        mut caller: wasmtime::Caller<'_, AppState>,
        conn_id: i32,
    ) -> wasmtime::Result<i32> {
        let conn_id_usize = conn_id as usize;
        let connections = &mut caller.data_mut().conn_manager.connections;

        if conn_id_usize >= connections.len() || connections[conn_id_usize].is_none() {
            return Ok(CONN_ERR_INVALID_CONN_ID);
        }

        connections[conn_id_usize] = None;
        Ok(0)
    }

    fn conn_write(
        mut caller: wasmtime::Caller<'_, AppState>,
        conn_id: i32,
        data_ptr: i32,
        data_len: u32,
    ) -> wasmtime::Result<i32> {
        let mut buf = [0u8; 4 * 1024];
        let len = (data_len as usize).min(buf.len());
        let offset = data_ptr as usize;

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("failed to find host memory");
        };

        mem.read(&caller, offset, &mut buf[..len])?;

        let conn_manager = &mut caller.data_mut().conn_manager;

        let conn_id_usize = conn_id as usize;
        if conn_id_usize >= conn_manager.connections.len() {
            return Ok(CONN_ERR_INVALID_CONN_ID);
        }

        let Some(conn) = conn_manager.connections[conn_id_usize].as_mut() else {
            return Ok(CONN_ERR_INVALID_CONN_ID);
        };

        if conn_manager.bandwidth.tokens() < len {
            return Ok(0);
        }
        conn_manager.bandwidth.consume(len);

        match conn.stream.try_write(&buf[..len]) {
            Ok(n) => Ok(n as i32),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => {
                tracing::error!("conn_write: stream write failed: {:?}", e);
                Ok(CONN_ERR_CONNECTION_ERROR)
            }
        }
    }

    fn conn_read(
        mut caller: wasmtime::Caller<'_, AppState>,
        conn_id: i32,
        ptr: i32,
        len: u32,
    ) -> wasmtime::Result<i32> {
        let conn_id_usize = conn_id as usize;

        {
            let connections = &caller.data().conn_manager.connections;
            if conn_id_usize >= connections.len() {
                return Ok(CONN_ERR_INVALID_CONN_ID);
            }
            if connections[conn_id_usize].is_none() {
                return Ok(CONN_ERR_INVALID_CONN_ID);
            };
        }

        let mut buf = [0u8; 4 * 1024];
        let read_len = (len as usize).min(buf.len());

        let n = {
            let connections = &mut caller.data_mut().conn_manager.connections;
            let Some(conn) = connections[conn_id_usize].as_mut() else {
                return Ok(CONN_ERR_INVALID_CONN_ID);
            };

            match conn.stream.try_read(&mut buf[..read_len]) {
                Ok(0) => return Ok(CONN_ERR_CONNECTION_ERROR),
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(0),
                Err(e) => {
                    tracing::error!("conn_read: stream read error: {:?}", e);
                    return Ok(CONN_ERR_CONNECTION_ERROR);
                }
            }
        };

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("failed to find host memory");
        };

        mem.write(&mut caller, ptr as usize, &buf[..n])?;

        Ok(n as i32)
    }
}
