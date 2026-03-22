// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::BTreeMap,
    io,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use anyhow::{Context, Result};
use bytes::BytesMut;
use futures::{Sink, Stream, future::join_all};
use reqwest::header::{AUTHORIZATION, HeaderValue};
use tarpc::client;
use terminal_games::control::{
    AdminControlRpcClient, AuthorControlRpcClient, AuthorTokenClaims, RegionDiscoveryResponse,
    RegionRuntimeStatus, SessionSummary, rpc_context,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{
        Error as WsError, Message, client::IntoClientRequest, protocol::WebSocketConfig,
    },
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::config::{
    AdminProfile, derive_region_urls, load_author_token_for_shortname, parse_author_ref,
    resolve_admin_profile,
};

const CONTROL_RPC_MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

pub fn completion_runtime() -> Option<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()
}

struct ClientWsTransport {
    socket: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    read_buf: BytesMut,
}

impl ClientWsTransport {
    fn new(socket: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Self {
        Self {
            socket,
            read_buf: BytesMut::new(),
        }
    }
}

impl AsyncRead for ClientWsTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.read_buf.is_empty() {
                let len = self.read_buf.len().min(buf.remaining());
                buf.put_slice(&self.read_buf.split_to(len));
                return Poll::Ready(Ok(()));
            }
            let Some(message) = futures::ready!(Pin::new(&mut self.socket).poll_next(cx)) else {
                return Poll::Ready(Ok(()));
            };
            match message {
                Ok(Message::Binary(data)) => self.read_buf = BytesMut::from(&data[..]),
                Ok(Message::Text(text)) => {
                    self.read_buf = BytesMut::from(text.as_bytes());
                }
                Ok(Message::Close(_)) => return Poll::Ready(Ok(())),
                Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => continue,
                Err(error) => return Poll::Ready(Err(ws_error(error))),
            }
        }
    }
}

impl AsyncWrite for ClientWsTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        futures::ready!(Pin::new(&mut self.socket).poll_ready(cx)).map_err(ws_error)?;
        Pin::new(&mut self.socket)
            .start_send(Message::Binary(buf.to_vec()))
            .map_err(ws_error)?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx).map_err(ws_error)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_close(cx).map_err(ws_error)
    }
}

fn ws_error(error: WsError) -> io::Error {
    io::Error::other(error)
}

fn rpc_url(base_url: &str, path: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(base_url)?;
    url.set_scheme(match url.scheme() {
        "https" => "wss",
        "http" => "ws",
        other => {
            return Err(anyhow::anyhow!(
                "unsupported control plane scheme '{other}'"
            ));
        }
    })
    .map_err(|_| anyhow::anyhow!("failed to rewrite control plane URL scheme"))?;
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string())
}

async fn connect_ws(base_url: &str, path: &str, bearer: &str) -> Result<ClientWsTransport> {
    let mut request = rpc_url(base_url, path)?.into_client_request()?;
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {bearer}"))
            .context("invalid bearer token for authorization header")?,
    );
    let (socket, _) = connect_async_with_config(
        request,
        Some(WebSocketConfig {
            max_message_size: Some(CONTROL_RPC_MAX_FRAME_LEN),
            max_frame_size: Some(CONTROL_RPC_MAX_FRAME_LEN),
            ..Default::default()
        }),
        false,
    )
    .await?;
    Ok(ClientWsTransport::new(socket))
}

async fn connect_admin_rpc(base_url: &str, bearer: &str) -> Result<AdminControlRpcClient> {
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(CONTROL_RPC_MAX_FRAME_LEN);
    let transport = tarpc::serde_transport::new::<
        _,
        tarpc::Response<terminal_games::control::AdminControlRpcResponse>,
        tarpc::ClientMessage<terminal_games::control::AdminControlRpcRequest>,
        _,
    >(
        Framed::new(
            connect_ws(base_url, "/control/admin/rpc", bearer).await?,
            codec,
        ),
        tarpc::tokio_serde::formats::Bincode::default(),
    );
    Ok(AdminControlRpcClient::new(client::Config::default(), transport).spawn())
}

async fn connect_author_rpc(claims: &AuthorTokenClaims) -> Result<AuthorControlRpcClient> {
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(CONTROL_RPC_MAX_FRAME_LEN);
    let transport = tarpc::serde_transport::new::<
        _,
        tarpc::Response<terminal_games::control::AuthorControlRpcResponse>,
        tarpc::ClientMessage<terminal_games::control::AuthorControlRpcRequest>,
        _,
    >(
        Framed::new(
            connect_ws(&claims.url, "/control/author/rpc", &claims.encode()?).await?,
            codec,
        ),
        tarpc::tokio_serde::formats::Bincode::default(),
    );
    Ok(AuthorControlRpcClient::new(client::Config::default(), transport).spawn())
}

#[derive(Clone)]
pub struct AdminClient {
    pub profile: AdminProfile,
    token: String,
}

impl AdminClient {
    pub fn load(profile_override: Option<&str>) -> Result<Self> {
        let (_, profile) = resolve_admin_profile(profile_override)?;
        Ok(Self {
            token: profile.password.clone(),
            profile,
        })
    }

    pub async fn rpc(&self) -> Result<AdminControlRpcClient> {
        connect_admin_rpc(&self.profile.url, &self.token).await
    }

    pub async fn rpc_at(&self, base_url: &str) -> Result<AdminControlRpcClient> {
        connect_admin_rpc(base_url, &self.token).await
    }

    pub async fn discover(&self) -> Result<(RegionDiscoveryResponse, BTreeMap<String, String>)> {
        let discovery = self
            .rpc()
            .await?
            .discover(rpc_context())
            .await?
            .map_err(anyhow::Error::msg)?;
        Ok((
            discovery.clone(),
            derive_region_urls(&self.profile.url, &discovery)?,
        ))
    }

    pub async fn region_url(&self, region: &str) -> Result<String> {
        let (_, region_urls) = self.discover().await?;
        region_urls
            .get(region)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown region '{region}'"))
    }

    pub async fn fanout<F, Fut>(&self, mut call: F) -> Result<usize>
    where
        F: FnMut(AdminControlRpcClient) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let (_, region_urls) = self.discover().await?;
        let connect_futures = region_urls
            .values()
            .map(|base_url| connect_admin_rpc(base_url, &self.token));
        let mut futures = Vec::new();
        for result in join_all(connect_futures).await {
            futures.push(call(result?));
        }
        for result in join_all(futures).await {
            result?;
        }
        Ok(region_urls.len())
    }

    pub async fn all_sessions(&self) -> Result<Vec<SessionSummary>> {
        let (_, region_urls) = self.discover().await?;
        let futures = region_urls.values().cloned().map(|base_url| async move {
            let rpc = connect_admin_rpc(&base_url, &self.token).await?;
            rpc.sessions(rpc_context())
                .await?
                .map_err(anyhow::Error::msg)
        });
        let mut sessions = Vec::new();
        for result in join_all(futures).await {
            sessions.extend(result?);
        }
        Ok(sessions)
    }

    pub async fn session_summary(
        &self,
        region: &str,
        local_id: u64,
    ) -> Result<Option<SessionSummary>> {
        let base_url = self.region_url(region).await?;
        let rpc = connect_admin_rpc(&base_url, &self.token).await?;
        Ok(rpc
            .sessions(rpc_context())
            .await?
            .map_err(anyhow::Error::msg)?
            .into_iter()
            .find(|session| session.local_session_id == local_id))
    }

    pub async fn region_statuses(&self) -> Result<Vec<RegionRuntimeStatus>> {
        let (_, region_urls) = self.discover().await?;
        let futures = region_urls.values().cloned().map(|base_url| async move {
            let rpc = connect_admin_rpc(&base_url, &self.token).await?;
            rpc.local_region_status(rpc_context())
                .await?
                .map_err(anyhow::Error::msg)
        });
        let mut statuses = Vec::new();
        for result in join_all(futures).await {
            statuses.push(result?);
        }
        Ok(statuses)
    }
}

#[derive(Clone)]
pub struct AuthorClient {
    pub claims: AuthorTokenClaims,
}

impl AuthorClient {
    pub fn from_claims(claims: AuthorTokenClaims) -> Result<Self> {
        Ok(Self { claims })
    }

    pub fn from_ref(author_ref: &str) -> Result<Self> {
        Self::from_claims(load_author_claims_for_ref(author_ref)?)
    }

    pub async fn rpc(&self) -> Result<AuthorControlRpcClient> {
        connect_author_rpc(&self.claims).await
    }
}

pub fn load_author_claims_for_ref(author_ref: &str) -> Result<AuthorTokenClaims> {
    let (profile, shortname) = parse_author_ref(author_ref)?;
    load_author_token_for_shortname(&shortname, Some(&profile))?.ok_or_else(|| {
        anyhow::anyhow!(
            "no author token configured for '{}' in profile '{}'",
            shortname,
            profile
        )
    })
}
