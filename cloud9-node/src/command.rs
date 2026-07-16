//! Replicated KV commands and deterministic state-machine application.

use std::cmp::Ordering;
use std::collections::HashMap;

use cloud9_proto::generated::cloud9::kv::v1::{
    DeleteResponse, PutResponse, RegisterSessionResponse,
};
use connectrpc::ConnectError;
use serde::{Deserialize, Serialize};

pub(crate) const MAX_VALUE_BYTES: usize = 64 * 1024;
pub(crate) const MAX_NAMESPACE_BYTES: usize = 255;
pub(crate) const MAX_KEY_BYTES: usize = 1024;
pub(crate) const MAX_ETAG_BYTES: usize = 128;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum KvCommand {
    RegisterSession,
    ReadBarrier,
    Put {
        client_id: u64,
        sequence: u64,
        namespace: String,
        key: String,
        body: Vec<u8>,
        if_match: String,
        if_none_match: bool,
    },
    Delete {
        client_id: u64,
        sequence: u64,
        namespace: String,
        key: String,
        if_match: String,
    },
}

pub(crate) enum KvApplyResult {
    RegisterSession(RegisterSessionResponse),
    Put(PutResponse),
    Delete(DeleteResponse),
    ReadBarrier,
}

#[derive(Default)]
pub(crate) struct KvState {
    next_client_id: u64,
    next_generation: u64,
    pub(crate) entries: HashMap<KvName, KvRecord>,
    sessions: HashMap<u64, SessionState>,
}

impl KvState {
    pub(crate) fn new() -> Self {
        Self { next_client_id: 1, next_generation: 1, ..Self::default() }
    }

    pub(crate) fn apply(&mut self, command: KvCommand) -> Result<KvApplyResult, ConnectError> {
        match command {
            KvCommand::RegisterSession => {
                let client_id = self.next_client_id()?;
                Ok(KvApplyResult::RegisterSession(RegisterSessionResponse {
                    client_id,
                    ..Default::default()
                }))
            }
            KvCommand::ReadBarrier => Ok(KvApplyResult::ReadBarrier),
            KvCommand::Put {
                client_id,
                sequence,
                namespace,
                key,
                body,
                if_match,
                if_none_match,
            } => self.apply_put(
                client_id,
                sequence,
                &namespace,
                &key,
                body,
                &if_match,
                if_none_match,
            ),
            KvCommand::Delete { client_id, sequence, namespace, key, if_match } => {
                self.apply_delete(client_id, sequence, &namespace, &key, &if_match)
            }
        }
    }

    fn apply_put(
        &mut self,
        client_id: u64,
        sequence: u64,
        namespace: &str,
        key: &str,
        body: Vec<u8>,
        if_match: &str,
        if_none_match: bool,
    ) -> Result<KvApplyResult, ConnectError> {
        validate_mutation_request(client_id, sequence)?;
        validate_put_preconditions(if_match, if_none_match)?;
        validate_value(&body)?;

        let name = KvName::new(namespace, key)?;
        let request = MutationRequest::Put {
            name: name.clone(),
            body: body.clone(),
            if_match: if_match.to_owned(),
            if_none_match,
        };
        if let Some(response) = cached_put(self, client_id, sequence, &request)? {
            return Ok(KvApplyResult::Put(response));
        }

        if let Err(rejection) =
            check_put_preconditions(self.entries.get(&name), if_match, if_none_match)
        {
            return self.reject(client_id, sequence, request, rejection);
        }
        let size = body_len(&body)?;
        let generation = match self.next_generation() {
            Ok(generation) => generation,
            Err(rejection) => return self.reject(client_id, sequence, request, rejection),
        };
        let etag = etag_for(generation);
        let response = PutResponse {
            namespace: name.namespace.clone(),
            key: name.key.clone(),
            etag: etag.clone(),
            generation,
            size,
            ..Default::default()
        };
        self.entries.insert(name, KvRecord { body, etag, generation });
        self.session_mut(client_id)?.record(
            sequence,
            request,
            MutationResult::Put(response.clone()),
        );
        Ok(KvApplyResult::Put(response))
    }

    fn apply_delete(
        &mut self,
        client_id: u64,
        sequence: u64,
        namespace: &str,
        key: &str,
        if_match: &str,
    ) -> Result<KvApplyResult, ConnectError> {
        validate_mutation_request(client_id, sequence)?;
        validate_etag(if_match)?;

        let name = KvName::new(namespace, key)?;
        let request = MutationRequest::Delete { name: name.clone(), if_match: if_match.to_owned() };
        if let Some(response) = cached_delete(self, client_id, sequence, &request)? {
            return Ok(KvApplyResult::Delete(response));
        }

        if !if_match.is_empty()
            && self.entries.get(&name).is_none_or(|record| if_match != record.etag)
        {
            return self.reject(client_id, sequence, request, MutationRejection::EtagMismatch);
        }
        let removed = self.entries.remove(&name);

        let response = if let Some(record) = removed {
            DeleteResponse {
                namespace: name.namespace,
                key: name.key,
                etag: record.etag,
                generation: record.generation,
                deleted: true,
                ..Default::default()
            }
        } else {
            DeleteResponse {
                namespace: name.namespace,
                key: name.key,
                etag: String::new(),
                generation: 0,
                deleted: false,
                ..Default::default()
            }
        };
        self.session_mut(client_id)?.record(
            sequence,
            request,
            MutationResult::Delete(response.clone()),
        );
        Ok(KvApplyResult::Delete(response))
    }

    fn next_client_id(&mut self) -> Result<u64, ConnectError> {
        let client_id = self.next_client_id;
        self.next_client_id = self
            .next_client_id
            .checked_add(1)
            .ok_or_else(|| ConnectError::resource_exhausted("client id space exhausted"))?;
        self.sessions.insert(client_id, SessionState::default());
        Ok(client_id)
    }

    fn next_generation(&mut self) -> Result<u64, MutationRejection> {
        let generation = self.next_generation;
        self.next_generation =
            self.next_generation.checked_add(1).ok_or(MutationRejection::GenerationExhausted)?;
        Ok(generation)
    }

    fn reject(
        &mut self,
        client_id: u64,
        sequence: u64,
        request: MutationRequest,
        rejection: MutationRejection,
    ) -> Result<KvApplyResult, ConnectError> {
        self.session_mut(client_id)?.record(sequence, request, MutationResult::Rejected(rejection));
        Err(rejection.connect_error())
    }

    fn session(&self, client_id: u64) -> Result<&SessionState, ConnectError> {
        self.sessions
            .get(&client_id)
            .ok_or_else(|| ConnectError::invalid_argument("unknown client session"))
    }

    fn session_mut(&mut self, client_id: u64) -> Result<&mut SessionState, ConnectError> {
        self.sessions
            .get_mut(&client_id)
            .ok_or_else(|| ConnectError::invalid_argument("unknown client session"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct KvName {
    pub(crate) namespace: String,
    pub(crate) key: String,
}

impl KvName {
    pub(crate) fn new(namespace: &str, key: &str) -> Result<Self, ConnectError> {
        if namespace.is_empty() {
            return Err(ConnectError::invalid_argument("namespace must not be empty"));
        }
        if namespace.len() > MAX_NAMESPACE_BYTES {
            return Err(ConnectError::invalid_argument("namespace exceeds 255-byte limit"));
        }
        if key.is_empty() {
            return Err(ConnectError::invalid_argument("key must not be empty"));
        }
        if key.len() > MAX_KEY_BYTES {
            return Err(ConnectError::invalid_argument("key exceeds 1024-byte limit"));
        }
        Ok(Self { namespace: namespace.to_owned(), key: key.to_owned() })
    }
}

#[derive(Clone)]
pub(crate) struct KvRecord {
    pub(crate) body: Vec<u8>,
    pub(crate) etag: String,
    pub(crate) generation: u64,
}

#[derive(Clone, Default)]
struct SessionState {
    sequence: u64,
    request: Option<MutationRequest>,
    result: Option<MutationResult>,
}

#[derive(Clone, PartialEq, Eq)]
enum MutationRequest {
    Put { name: KvName, body: Vec<u8>, if_match: String, if_none_match: bool },
    Delete { name: KvName, if_match: String },
}

#[derive(Clone)]
enum MutationResult {
    Put(PutResponse),
    Delete(DeleteResponse),
    Rejected(MutationRejection),
}

#[derive(Clone, Copy)]
enum MutationRejection {
    KeyExists,
    EtagMismatch,
    GenerationExhausted,
}

impl MutationRejection {
    fn connect_error(self) -> ConnectError {
        match self {
            Self::KeyExists => ConnectError::failed_precondition("key already exists"),
            Self::EtagMismatch => ConnectError::failed_precondition("ETag precondition failed"),
            Self::GenerationExhausted => {
                ConnectError::resource_exhausted("kv generation space exhausted")
            }
        }
    }
}

impl SessionState {
    fn record(&mut self, sequence: u64, request: MutationRequest, result: MutationResult) {
        self.sequence = sequence;
        self.request = Some(request);
        self.result = Some(result);
    }
}

pub(crate) fn validate_mutation_request(client_id: u64, sequence: u64) -> Result<(), ConnectError> {
    if client_id == 0 {
        return Err(ConnectError::invalid_argument("client_id must be registered"));
    }
    if sequence == 0 {
        return Err(ConnectError::invalid_argument("sequence must be positive"));
    }
    Ok(())
}

pub(crate) fn validate_put_preconditions(
    if_match: &str,
    if_none_match: bool,
) -> Result<(), ConnectError> {
    validate_etag(if_match)?;
    if !if_match.is_empty() && if_none_match {
        return Err(ConnectError::invalid_argument(
            "if_match and if_none_match are mutually exclusive",
        ));
    }
    Ok(())
}

pub(crate) fn validate_etag(etag: &str) -> Result<(), ConnectError> {
    if etag.len() > MAX_ETAG_BYTES {
        return Err(ConnectError::invalid_argument("ETag exceeds 128-byte limit"));
    }
    Ok(())
}

pub(crate) fn validate_value(body: &[u8]) -> Result<(), ConnectError> {
    if body.len() > MAX_VALUE_BYTES {
        return Err(ConnectError::resource_exhausted("value exceeds 65536-byte limit"));
    }
    Ok(())
}

fn check_put_preconditions(
    current: Option<&KvRecord>,
    if_match: &str,
    if_none_match: bool,
) -> Result<(), MutationRejection> {
    if if_none_match && current.is_some() {
        return Err(MutationRejection::KeyExists);
    }
    if !if_match.is_empty() {
        match current {
            Some(record) if record.etag == if_match => {}
            Some(_) | None => return Err(MutationRejection::EtagMismatch),
        }
    }
    Ok(())
}

fn cached_put(
    state: &KvState,
    client_id: u64,
    sequence: u64,
    request: &MutationRequest,
) -> Result<Option<PutResponse>, ConnectError> {
    match cached_mutation(state.session(client_id)?, sequence, request)? {
        Some(MutationResult::Put(response)) => Ok(Some(response)),
        Some(MutationResult::Rejected(rejection)) => Err(rejection.connect_error()),
        Some(MutationResult::Delete(_)) => Err(ConnectError::internal("session result mismatch")),
        None => Ok(None),
    }
}

fn cached_delete(
    state: &KvState,
    client_id: u64,
    sequence: u64,
    request: &MutationRequest,
) -> Result<Option<DeleteResponse>, ConnectError> {
    match cached_mutation(state.session(client_id)?, sequence, request)? {
        Some(MutationResult::Delete(response)) => Ok(Some(response)),
        Some(MutationResult::Rejected(rejection)) => Err(rejection.connect_error()),
        Some(MutationResult::Put(_)) => Err(ConnectError::internal("session result mismatch")),
        None => Ok(None),
    }
}

fn cached_mutation(
    session: &SessionState,
    sequence: u64,
    request: &MutationRequest,
) -> Result<Option<MutationResult>, ConnectError> {
    match sequence.cmp(&session.sequence) {
        Ordering::Less => Err(ConnectError::aborted("stale client sequence")),
        Ordering::Greater => Ok(None),
        Ordering::Equal => match (&session.request, &session.result) {
            (Some(cached), Some(result)) if cached == request => Ok(Some(result.clone())),
            (Some(_), Some(_)) => {
                Err(ConnectError::aborted("sequence reused for different request"))
            }
            (None, None) => Err(ConnectError::internal("session cache is incomplete")),
            (Some(_), None) | (None, Some(_)) => {
                Err(ConnectError::internal("session cache is inconsistent"))
            }
        },
    }
}

fn etag_for(generation: u64) -> String {
    format!("\"c9-{generation}\"")
}

pub(crate) fn body_len(body: &[u8]) -> Result<u64, ConnectError> {
    u64::try_from(body.len()).map_err(|_| ConnectError::resource_exhausted("value too large"))
}

pub(crate) fn key_not_found() -> ConnectError {
    ConnectError::not_found("key not found")
}
