//! Public KV service and node lifecycle.

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router as AxumRouter;
use axum::routing::get;
use cloud9_proto::generated::cloud9::kv::v1::{
    DeleteResponse, GetResponse, HeadResponse, KvService, KvServiceExt, OwnedDeleteRequestView,
    OwnedGetRequestView, OwnedHeadRequestView, OwnedPutRequestView,
    OwnedRegisterSessionRequestView, OwnedStatusRequestView, PutResponse, RegisterSessionResponse,
    StatusResponse,
};
use connectrpc::{ConnectError, RequestContext, Response, Router as ConnectRouter, ServiceResult};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{info, instrument};

use crate::command::{
    KvApplyResult, KvCommand, KvName, KvState, body_len, key_not_found, validate_etag,
    validate_mutation_request, validate_put_preconditions, validate_value,
};
use crate::config::NodeConfig;
use crate::runtime::RaftRuntime;
use crate::transport::raft_app;

#[derive(Clone)]
pub(crate) struct KvApi {
    config: NodeConfig,
    state: Arc<RwLock<KvState>>,
    runtime: Arc<RaftRuntime>,
}

impl KvApi {
    pub(crate) fn new(
        config: NodeConfig,
        state: Arc<RwLock<KvState>>,
        runtime: Arc<RaftRuntime>,
    ) -> Self {
        Self { config, state, runtime }
    }
}

#[instrument(skip_all)]
pub(crate) async fn launch(config: NodeConfig) -> Result<()> {
    let state = Arc::new(RwLock::new(KvState::new()));
    let runtime = Arc::new(RaftRuntime::open(config.clone(), state.clone())?);
    let api = Arc::new(KvApi::new(config.clone(), state, runtime.clone()));
    let client_app = kv_app(api);
    let raft_app = raft_app(runtime.clone());
    let client_listener = TcpListener::bind(config.client_addr)
        .await
        .with_context(|| format!("binding Cloud9 KV API to {}", config.client_addr))?;
    let raft_listener = TcpListener::bind(config.raft_addr)
        .await
        .with_context(|| format!("binding Cloud9 Raft API to {}", config.raft_addr))?;

    info!(
        node_id = config.node_id.0,
        client_addr = %config.client_addr,
        raft_addr = %config.raft_addr,
        peer_count = config.peers.len(),
        data_dir = config.storage.data_dir.as_str(),
        "serving Cloud9 KV API"
    );

    tokio::try_join!(
        async { axum::serve(client_listener, client_app).await.context("serving KV API") },
        async { axum::serve(raft_listener, raft_app).await.context("serving Raft API") },
        async { runtime.run().await.context("driving Raft runtime") },
    )?;
    Ok(())
}

pub(crate) fn kv_app(api: Arc<KvApi>) -> AxumRouter {
    let connect = api.register(ConnectRouter::new());
    AxumRouter::new()
        .route("/healthz", get(|| async { "ok" }))
        .fallback_service(connect.into_axum_service())
}

#[allow(refining_impl_trait)]
impl KvService for KvApi {
    async fn register_session(
        &self,
        _ctx: RequestContext,
        _request: OwnedRegisterSessionRequestView,
    ) -> ServiceResult<RegisterSessionResponse> {
        match self.runtime.propose(KvCommand::RegisterSession).await? {
            KvApplyResult::RegisterSession(response) => Response::ok(response),
            KvApplyResult::Put(_) | KvApplyResult::Delete(_) | KvApplyResult::ReadBarrier => {
                Err(ConnectError::internal("Raft session command mismatch"))
            }
        }
    }

    async fn head(
        &self,
        _ctx: RequestContext,
        request: OwnedHeadRequestView,
    ) -> ServiceResult<HeadResponse> {
        let name = KvName::new(request.namespace, request.key)?;
        self.runtime.read_barrier().await?;
        let state = self.state.read().await;
        let record = state.entries.get(&name).ok_or_else(key_not_found)?;
        Response::ok(HeadResponse {
            namespace: name.namespace,
            key: name.key,
            etag: record.etag.clone(),
            generation: record.generation,
            size: body_len(&record.body)?,
            ..Default::default()
        })
    }

    async fn get(
        &self,
        _ctx: RequestContext,
        request: OwnedGetRequestView,
    ) -> ServiceResult<GetResponse> {
        let name = KvName::new(request.namespace, request.key)?;
        self.runtime.read_barrier().await?;
        let state = self.state.read().await;
        let record = state.entries.get(&name).ok_or_else(key_not_found)?;
        Response::ok(GetResponse {
            namespace: name.namespace,
            key: name.key,
            etag: record.etag.clone(),
            generation: record.generation,
            size: body_len(&record.body)?,
            body: record.body.clone(),
            ..Default::default()
        })
    }

    async fn put(
        &self,
        _ctx: RequestContext,
        request: OwnedPutRequestView,
    ) -> ServiceResult<PutResponse> {
        validate_mutation_request(request.client_id, request.sequence)?;
        validate_put_preconditions(request.if_match, request.if_none_match)?;
        validate_value(request.body)?;
        KvName::new(request.namespace, request.key)?;

        let command = KvCommand::Put {
            client_id: request.client_id,
            sequence: request.sequence,
            namespace: request.namespace.to_owned(),
            key: request.key.to_owned(),
            body: request.body.to_vec(),
            if_match: request.if_match.to_owned(),
            if_none_match: request.if_none_match,
        };
        match self.runtime.propose(command).await? {
            KvApplyResult::Put(response) => Response::ok(response),
            KvApplyResult::RegisterSession(_)
            | KvApplyResult::Delete(_)
            | KvApplyResult::ReadBarrier => {
                Err(ConnectError::internal("Raft put command mismatch"))
            }
        }
    }

    async fn delete(
        &self,
        _ctx: RequestContext,
        request: OwnedDeleteRequestView,
    ) -> ServiceResult<DeleteResponse> {
        validate_mutation_request(request.client_id, request.sequence)?;
        validate_etag(request.if_match)?;
        KvName::new(request.namespace, request.key)?;

        let command = KvCommand::Delete {
            client_id: request.client_id,
            sequence: request.sequence,
            namespace: request.namespace.to_owned(),
            key: request.key.to_owned(),
            if_match: request.if_match.to_owned(),
        };
        match self.runtime.propose(command).await? {
            KvApplyResult::Delete(response) => Response::ok(response),
            KvApplyResult::RegisterSession(_)
            | KvApplyResult::Put(_)
            | KvApplyResult::ReadBarrier => {
                Err(ConnectError::internal("Raft delete command mismatch"))
            }
        }
    }

    async fn status(
        &self,
        _ctx: RequestContext,
        _request: OwnedStatusRequestView,
    ) -> ServiceResult<StatusResponse> {
        let mode = self.runtime.mode().await;
        let state = self.state.read().await;
        Response::ok(StatusResponse {
            node_id: self.config.node_id.0,
            mode,
            key_count: usize_to_u64(state.entries.len())?,
            ..Default::default()
        })
    }
}

fn usize_to_u64(value: usize) -> Result<u64, ConnectError> {
    u64::try_from(value).map_err(|_| ConnectError::resource_exhausted("key count overflow"))
}
