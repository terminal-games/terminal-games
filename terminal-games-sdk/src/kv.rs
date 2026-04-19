// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{collections::VecDeque, time::Duration};

const KV_REQ_ERR_INVALID_INPUT: i32 = -1;
const KV_REQ_ERR_TOO_MANY_REQUESTS: i32 = -2;

const KV_POLL_PENDING: i32 = -1;
const KV_POLL_ERR_BUFFER_TOO_SMALL: i32 = -3;
const KV_POLL_ERR_REQUEST_FAILED: i32 = -4;
const KV_POLL_ERR_INVALID_REQUEST_ID: i32 = -8;

const KEY_PART_STRING: u8 = 1;
const KEY_PART_BYTES: u8 = 2;
const KEY_PART_I64: u8 = 3;
const KEY_PART_U64: u8 = 4;
const KEY_PART_BOOL: u8 = 5;

const KV_CMD_SET: u32 = 1;
const KV_CMD_DELETE: u32 = 2;
const KV_CMD_CHECK_VALUE: u32 = 3;
const KV_CMD_CHECK_EXISTS: u32 = 4;
const KV_CMD_CHECK_MISSING: u32 = 5;
const KV_LIST_REQUEST_HEADER_SIZE: usize = 16;
const KV_LIST_RESPONSE_HEADER_SIZE: usize = 8;
const KV_OPTIONAL_KEY_MISSING: u32 = u32::MAX;
const KV_ERROR_UNAVAILABLE: u32 = 1;
const KV_ERROR_CHECK_FAILED: u32 = 2;
const KV_ERROR_QUOTA_EXCEEDED: u32 = 3;
const KV_CHECK_FAILED_KEY_MISSING: u32 = 1;
const KV_CHECK_FAILED_KEY_EXISTS: u32 = 2;
const KV_CHECK_FAILED_VALUE_MISMATCH: u32 = 3;

pub type Key = Vec<KeyPart>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPart {
    String(String),
    Bytes(Vec<u8>),
    I64(i64),
    U64(u64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    String(String),
    Bytes(Vec<u8>),
    I64(i64),
    U64(u64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: Key,
    pub value: Value,
}

pub struct ListIterator {
    prefix: Vec<u8>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    buffer: VecDeque<Entry>,
    exhausted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Set { key: Key, value: Value },
    Delete { key: Key },
    Check { key: Key, value: Value },
    CheckExists { key: Key },
    CheckMissing { key: Key },
}

#[derive(Debug)]
pub enum Error {
    VersionMismatch,
    InvalidInput,
    TooManyRequests,
    InvalidRequestId,
    Unavailable,
    CheckFailed(CheckFailedReason),
    QuotaExceeded { used_bytes: u64, limit_bytes: u64 },
    Decode(String),
    Unknown(i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckFailedReason {
    KeyMissing,
    KeyExists,
    ValueMismatch,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VersionMismatch => {
                write!(f, "terminal-games host version mismatch for terminal_games")
            }
            Self::InvalidInput => write!(f, "invalid kv request"),
            Self::TooManyRequests => write!(f, "too many pending kv requests"),
            Self::InvalidRequestId => write!(f, "invalid kv request ID"),
            Self::Unavailable => write!(f, "kv unavailable"),
            Self::CheckFailed(reason) => match reason {
                CheckFailedReason::KeyMissing => write!(f, "kv check failed: key missing"),
                CheckFailedReason::KeyExists => write!(f, "kv check failed: key exists"),
                CheckFailedReason::ValueMismatch => write!(f, "kv check failed: value mismatch"),
            },
            Self::QuotaExceeded {
                used_bytes,
                limit_bytes,
            } => write!(f, "kv quota exceeded: {used_bytes} > {limit_bytes}"),
            Self::Decode(error) => f.write_str(error),
            Self::Unknown(code) => write!(f, "unknown kv error: {code}"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    fn from_code(code: i32) -> Self {
        match code {
            crate::HOST_API_VERSION_MISMATCH => Self::VersionMismatch,
            KV_REQ_ERR_INVALID_INPUT => Self::InvalidInput,
            KV_REQ_ERR_TOO_MANY_REQUESTS => Self::TooManyRequests,
            KV_POLL_ERR_INVALID_REQUEST_ID => Self::InvalidRequestId,
            _ => Self::Unknown(code),
        }
    }
}

fn decode_request_failed(bytes: Vec<u8>) -> Error {
    let Some(tag_bytes) = bytes.get(..4) else {
        return Error::Decode("invalid kv error payload".into());
    };
    let tag = u32::from_le_bytes(tag_bytes.try_into().unwrap());
    match tag {
        KV_ERROR_UNAVAILABLE if bytes.len() == 4 => Error::Unavailable,
        KV_ERROR_CHECK_FAILED if bytes.len() == 8 => {
            let reason = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
            Error::CheckFailed(match reason {
                KV_CHECK_FAILED_KEY_MISSING => CheckFailedReason::KeyMissing,
                KV_CHECK_FAILED_KEY_EXISTS => CheckFailedReason::KeyExists,
                KV_CHECK_FAILED_VALUE_MISMATCH => CheckFailedReason::ValueMismatch,
                _ => return Error::Decode("invalid kv check-failed payload".into()),
            })
        }
        KV_ERROR_QUOTA_EXCEEDED if bytes.len() == 20 => {
            let used_bytes = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
            let limit_bytes = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
            Error::QuotaExceeded {
                used_bytes,
                limit_bytes,
            }
        }
        _ => Error::Decode("unknown kv error payload".into()),
    }
}

pub trait IntoKeyPart {
    fn into_key_part(self) -> KeyPart;
}

pub trait IntoValue {
    fn into_value(self) -> Value;
}

macro_rules! impl_into_key_string {
    ($($ty:ty),* $(,)?) => {
        $(impl IntoKeyPart for $ty {
            fn into_key_part(self) -> KeyPart {
                KeyPart::String(self.into())
            }
        })*
    };
}

impl_into_key_string!(&str, String);

impl IntoKeyPart for &[u8] {
    fn into_key_part(self) -> KeyPart {
        KeyPart::Bytes(self.to_vec())
    }
}

impl IntoKeyPart for Vec<u8> {
    fn into_key_part(self) -> KeyPart {
        KeyPart::Bytes(self)
    }
}

macro_rules! impl_into_key_i64 {
    ($($ty:ty),* $(,)?) => {
        $(impl IntoKeyPart for $ty {
            fn into_key_part(self) -> KeyPart {
                KeyPart::I64(self as i64)
            }
        })*
    };
}

impl_into_key_i64!(i8, i16, i32, i64, isize);

macro_rules! impl_into_key_u64 {
    ($($ty:ty),* $(,)?) => {
        $(impl IntoKeyPart for $ty {
            fn into_key_part(self) -> KeyPart {
                KeyPart::U64(self as u64)
            }
        })*
    };
}

impl_into_key_u64!(u8, u16, u32, u64, usize);

impl IntoKeyPart for bool {
    fn into_key_part(self) -> KeyPart {
        KeyPart::Bool(self)
    }
}

macro_rules! impl_into_value_string {
    ($($ty:ty),* $(,)?) => {
        $(impl IntoValue for $ty {
            fn into_value(self) -> Value {
                Value::String(self.into())
            }
        })*
    };
}

impl_into_value_string!(&str, String);

impl IntoValue for &[u8] {
    fn into_value(self) -> Value {
        Value::Bytes(self.to_vec())
    }
}

impl IntoValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::Bytes(self)
    }
}

macro_rules! impl_into_value_i64 {
    ($($ty:ty),* $(,)?) => {
        $(impl IntoValue for $ty {
            fn into_value(self) -> Value {
                Value::I64(self as i64)
            }
        })*
    };
}

impl_into_value_i64!(i8, i16, i32, i64, isize);

macro_rules! impl_into_value_u64 {
    ($($ty:ty),* $(,)?) => {
        $(impl IntoValue for $ty {
            fn into_value(self) -> Value {
                Value::U64(self as u64)
            }
        })*
    };
}

impl_into_value_u64!(u8, u16, u32, u64, usize);

impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::Bool(self)
    }
}

impl Into<Key> for KeyPart {
    fn into(self) -> Key {
        vec![self]
    }
}

pub async fn get(key: impl Into<Key>) -> Result<Option<Value>, Error> {
    let key = encode_key(&key.into())?;
    let request_id = unsafe { crate::internal::kv_get(key.as_ptr(), key.len() as u32) };
    if request_id < 0 {
        return Err(Error::from_code(request_id));
    }
    let bytes = poll_get(request_id).await?;
    bytes.map(|bytes| decode_value(&bytes)).transpose()
}

pub async fn get_parts(parts: &[KeyPart]) -> Result<Option<Value>, Error> {
    get(parts.to_vec()).await
}

pub async fn set(value: impl IntoValue, key: impl Into<Key>) -> Result<(), Error> {
    exec([Command::Set {
        key: key.into(),
        value: value.into_value(),
    }])
    .await
}

pub async fn set_parts(value: impl IntoValue, parts: &[KeyPart]) -> Result<(), Error> {
    set(value, parts.to_vec()).await
}

pub fn atomic() -> Atomic {
    Atomic::default()
}

pub async fn exec(commands: impl IntoIterator<Item = Command>) -> Result<(), Error> {
    let commands = commands.into_iter().collect::<Vec<_>>();
    let encoded = encode_commands(commands)?;
    let request_id = unsafe {
        crate::internal::kv_exec(
            encoded.commands.as_ptr() as *const u8,
            (encoded.commands.len() * std::mem::size_of::<GuestCommand>()) as u32,
        )
    };
    if request_id < 0 {
        return Err(Error::from_code(request_id));
    }
    poll_exec(request_id).await
}

pub async fn list(
    prefix: impl Into<Key>,
    start: Option<Key>,
    end: Option<Key>,
) -> Result<ListIterator, Error> {
    Ok(ListIterator {
        prefix: encode_key(&prefix.into())?,
        start: start.as_deref().map(encode_key).transpose()?,
        end: end.as_deref().map(encode_key).transpose()?,
        after: None,
        buffer: VecDeque::new(),
        exhausted: false,
    })
}

pub async fn storage_used() -> Result<u64, Error> {
    let request_id = unsafe { crate::internal::kv_storage_used() };
    if request_id < 0 {
        return Err(Error::from_code(request_id));
    }
    let bytes = poll_storage_used(request_id).await?;
    let bytes: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::Decode("invalid kv storage-used payload length".into()))?;
    Ok(u64::from_le_bytes(bytes))
}

#[derive(Default)]
pub struct Atomic {
    commands: Vec<Command>,
}

impl Atomic {
    pub fn set(mut self, value: impl IntoValue, key: impl Into<Key>) -> Self {
        self.commands.push(Command::Set {
            key: key.into(),
            value: value.into_value(),
        });
        self
    }

    pub fn delete(mut self, key: impl Into<Key>) -> Self {
        self.commands.push(Command::Delete { key: key.into() });
        self
    }

    pub fn check(mut self, value: impl IntoValue, key: impl Into<Key>) -> Self {
        self.commands.push(Command::Check {
            key: key.into(),
            value: value.into_value(),
        });
        self
    }

    pub fn check_exists(mut self, key: impl Into<Key>) -> Self {
        self.commands.push(Command::CheckExists { key: key.into() });
        self
    }

    pub fn check_missing(mut self, key: impl Into<Key>) -> Self {
        self.commands
            .push(Command::CheckMissing { key: key.into() });
        self
    }

    pub async fn exec(self) -> Result<(), Error> {
        exec(self.commands).await
    }
}

impl ListIterator {
    pub async fn next(&mut self) -> Result<Option<Entry>, Error> {
        loop {
            if let Some(entry) = self.buffer.pop_front() {
                return Ok(Some(entry));
            }
            if self.exhausted {
                return Ok(None);
            }
            self.load_next_page().await?;
        }
    }

    pub async fn next_batch(&mut self) -> Result<Vec<Entry>, Error> {
        if self.buffer.is_empty() && !self.exhausted {
            self.load_next_page().await?;
        }
        Ok(std::mem::take(&mut self.buffer).into_iter().collect())
    }

    async fn load_next_page(&mut self) -> Result<(), Error> {
        let request = encode_list_request(
            &self.prefix,
            self.start.as_deref(),
            self.end.as_deref(),
            self.after.as_deref(),
        )?;
        let request_id =
            unsafe { crate::internal::kv_list(request.as_ptr(), request.len() as u32) };
        if request_id < 0 {
            return Err(Error::from_code(request_id));
        }
        let bytes = poll_list(request_id).await?;
        let page = decode_list_page(&bytes)?;
        self.after = page.next_after;
        self.exhausted = self.after.is_none();
        self.buffer = page.entries.into_iter().collect();
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct GuestCommand {
    tag: u32,
    key_ptr: u32,
    key_len: u32,
    value_ptr: u32,
    value_len: u32,
}

struct EncodedCommands {
    _keys: Vec<Vec<u8>>,
    _values: Vec<Vec<u8>>,
    commands: Vec<GuestCommand>,
}

fn encode_commands(commands: impl IntoIterator<Item = Command>) -> Result<EncodedCommands, Error> {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut guest = Vec::new();

    for command in commands {
        let (tag, key, value) = match command {
            Command::Set { key, value } => (KV_CMD_SET, key, Some(value)),
            Command::Delete { key } => (KV_CMD_DELETE, key, None),
            Command::Check { key, value } => (KV_CMD_CHECK_VALUE, key, Some(value)),
            Command::CheckExists { key } => (KV_CMD_CHECK_EXISTS, key, None),
            Command::CheckMissing { key } => (KV_CMD_CHECK_MISSING, key, None),
        };
        keys.push(encode_key(&key)?);
        let key_bytes = keys.last().unwrap();
        let (value_ptr, value_len) = if let Some(value) = value {
            values.push(encode_value(&value)?);
            let value_bytes = values.last().unwrap();
            (value_bytes.as_ptr() as u32, value_bytes.len() as u32)
        } else {
            (0, 0)
        };
        guest.push(GuestCommand {
            tag,
            key_ptr: key_bytes.as_ptr() as u32,
            key_len: key_bytes.len() as u32,
            value_ptr,
            value_len,
        });
    }

    Ok(EncodedCommands {
        _keys: keys,
        _values: values,
        commands: guest,
    })
}

async fn poll_get(request_id: i32) -> Result<Option<Vec<u8>>, Error> {
    let mut buffer = vec![0u8; 256];
    loop {
        let mut len = 0_u32;
        let result = unsafe {
            crate::internal::kv_get_poll(
                request_id,
                buffer.as_mut_ptr(),
                buffer.len() as u32,
                &mut len as *mut u32,
            )
        };
        match result {
            1 => {
                buffer.truncate(len as usize);
                return Ok(Some(buffer));
            }
            0 => return Ok(None),
            KV_POLL_PENDING => tokio::time::sleep(Duration::from_millis(10)).await,
            KV_POLL_ERR_BUFFER_TOO_SMALL => buffer.resize(len as usize, 0),
            KV_POLL_ERR_REQUEST_FAILED => {
                buffer.truncate(len as usize);
                return Err(decode_request_failed(buffer));
            }
            code => return Err(Error::from_code(code)),
        }
    }
}

async fn poll_exec(request_id: i32) -> Result<(), Error> {
    let mut buffer = vec![0u8; 256];
    loop {
        let mut len = 0_u32;
        let result = unsafe {
            crate::internal::kv_exec_poll(
                request_id,
                buffer.as_mut_ptr(),
                buffer.len() as u32,
                &mut len as *mut u32,
            )
        };
        match result {
            1 => return Ok(()),
            KV_POLL_PENDING => tokio::time::sleep(Duration::from_millis(10)).await,
            KV_POLL_ERR_BUFFER_TOO_SMALL => buffer.resize(len as usize, 0),
            KV_POLL_ERR_REQUEST_FAILED => {
                buffer.truncate(len as usize);
                return Err(decode_request_failed(buffer));
            }
            code => return Err(Error::from_code(code)),
        }
    }
}

async fn poll_list(request_id: i32) -> Result<Vec<u8>, Error> {
    let mut buffer = vec![0u8; 256];
    loop {
        let mut len = 0_u32;
        let result = unsafe {
            crate::internal::kv_list_poll(
                request_id,
                buffer.as_mut_ptr(),
                buffer.len() as u32,
                &mut len as *mut u32,
            )
        };
        match result {
            1 => {
                buffer.truncate(len as usize);
                return Ok(buffer);
            }
            KV_POLL_PENDING => tokio::time::sleep(Duration::from_millis(10)).await,
            KV_POLL_ERR_BUFFER_TOO_SMALL => buffer.resize(len as usize, 0),
            KV_POLL_ERR_REQUEST_FAILED => {
                buffer.truncate(len as usize);
                return Err(decode_request_failed(buffer));
            }
            code => return Err(Error::from_code(code)),
        }
    }
}

async fn poll_storage_used(request_id: i32) -> Result<Vec<u8>, Error> {
    let mut buffer = vec![0u8; 8];
    loop {
        let mut len = 0_u32;
        let result = unsafe {
            crate::internal::kv_storage_used_poll(
                request_id,
                buffer.as_mut_ptr(),
                buffer.len() as u32,
                &mut len as *mut u32,
            )
        };
        match result {
            1 => {
                buffer.truncate(len as usize);
                return Ok(buffer);
            }
            KV_POLL_PENDING => tokio::time::sleep(Duration::from_millis(10)).await,
            KV_POLL_ERR_BUFFER_TOO_SMALL => buffer.resize(len as usize, 0),
            KV_POLL_ERR_REQUEST_FAILED => {
                buffer.truncate(len as usize);
                return Err(decode_request_failed(buffer));
            }
            code => return Err(Error::from_code(code)),
        }
    }
}

fn encode_key(parts: &[KeyPart]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    for part in parts {
        match part {
            KeyPart::String(value) => encode_part(KEY_PART_STRING, value.as_bytes(), &mut out)?,
            KeyPart::Bytes(value) => encode_part(KEY_PART_BYTES, value, &mut out)?,
            KeyPart::I64(value) => {
                let sortable = (*value as u64) ^ (1_u64 << 63);
                encode_part(KEY_PART_I64, &sortable.to_be_bytes(), &mut out)?;
            }
            KeyPart::U64(value) => encode_part(KEY_PART_U64, &value.to_be_bytes(), &mut out)?,
            KeyPart::Bool(value) => encode_part(KEY_PART_BOOL, &[u8::from(*value)], &mut out)?,
        }
    }
    Ok(out)
}

fn encode_value(value: &Value) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    match value {
        Value::String(value) => encode_part(KEY_PART_STRING, value.as_bytes(), &mut out)?,
        Value::Bytes(value) => encode_part(KEY_PART_BYTES, value, &mut out)?,
        Value::I64(value) => {
            let sortable = (*value as u64) ^ (1_u64 << 63);
            encode_part(KEY_PART_I64, &sortable.to_be_bytes(), &mut out)?;
        }
        Value::U64(value) => encode_part(KEY_PART_U64, &value.to_be_bytes(), &mut out)?,
        Value::Bool(value) => encode_part(KEY_PART_BOOL, &[u8::from(*value)], &mut out)?,
    }
    Ok(out)
}

fn decode_value(bytes: &[u8]) -> Result<Value, Error> {
    let mut reader = Reader::new(bytes);
    let value = match reader.read_key_part()? {
        KeyPart::String(value) => Value::String(value),
        KeyPart::Bytes(value) => Value::Bytes(value),
        KeyPart::I64(value) => Value::I64(value),
        KeyPart::U64(value) => Value::U64(value),
        KeyPart::Bool(value) => Value::Bool(value),
    };
    reader.finish()?;
    Ok(value)
}

fn decode_key(bytes: &[u8]) -> Result<Key, Error> {
    let mut reader = Reader::new(bytes);
    let mut key = Vec::new();
    while reader.offset < reader.bytes.len() {
        key.push(reader.read_key_part()?);
    }
    Ok(key)
}

struct ListPage {
    entries: Vec<Entry>,
    next_after: Option<Vec<u8>>,
}

fn encode_list_request(
    prefix: &[u8],
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    after: Option<&[u8]>,
) -> Result<Vec<u8>, Error> {
    let prefix_len = u32::try_from(prefix.len())
        .map_err(|_| Error::Decode("kv list prefix too large".into()))?;
    let start_len = start
        .map(|bytes| {
            u32::try_from(bytes.len()).map_err(|_| Error::Decode("kv list start too large".into()))
        })
        .transpose()?
        .unwrap_or(KV_OPTIONAL_KEY_MISSING);
    let end_len = end
        .map(|bytes| {
            u32::try_from(bytes.len()).map_err(|_| Error::Decode("kv list end too large".into()))
        })
        .transpose()?
        .unwrap_or(KV_OPTIONAL_KEY_MISSING);
    let after_len = after
        .map(|bytes| {
            u32::try_from(bytes.len()).map_err(|_| Error::Decode("kv list cursor too large".into()))
        })
        .transpose()?
        .unwrap_or(KV_OPTIONAL_KEY_MISSING);
    let mut out = Vec::with_capacity(
        KV_LIST_REQUEST_HEADER_SIZE
            + prefix.len()
            + start.map_or(0, |bytes| bytes.len())
            + end.map_or(0, |bytes| bytes.len())
            + after.map_or(0, |bytes| bytes.len()),
    );
    out.extend_from_slice(&prefix_len.to_le_bytes());
    out.extend_from_slice(&start_len.to_le_bytes());
    out.extend_from_slice(&end_len.to_le_bytes());
    out.extend_from_slice(&after_len.to_le_bytes());
    out.extend_from_slice(prefix);
    if let Some(start) = start {
        out.extend_from_slice(start);
    }
    if let Some(end) = end {
        out.extend_from_slice(end);
    }
    if let Some(after) = after {
        out.extend_from_slice(after);
    }
    Ok(out)
}

fn decode_list_page(bytes: &[u8]) -> Result<ListPage, Error> {
    if bytes.len() < KV_LIST_RESPONSE_HEADER_SIZE {
        return Err(Error::Decode("unexpected end of kv list payload".into()));
    }
    let mut reader = Reader::new(bytes);
    let count = u32::from_le_bytes(
        reader
            .read_exact(4)?
            .try_into()
            .map_err(|_| Error::Decode("invalid kv list count".into()))?,
    ) as usize;
    let next_after_len = u32::from_le_bytes(
        reader
            .read_exact(4)?
            .try_into()
            .map_err(|_| Error::Decode("invalid kv list cursor length".into()))?,
    );
    let next_after = if next_after_len == KV_OPTIONAL_KEY_MISSING {
        None
    } else {
        Some(reader.read_exact(next_after_len as usize)?.to_vec())
    };
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key_len = u32::from_le_bytes(
            reader
                .read_exact(4)?
                .try_into()
                .map_err(|_| Error::Decode("invalid kv list key length".into()))?,
        ) as usize;
        let value_len = u32::from_le_bytes(
            reader
                .read_exact(4)?
                .try_into()
                .map_err(|_| Error::Decode("invalid kv list value length".into()))?,
        ) as usize;
        let key = decode_key(reader.read_exact(key_len)?)?;
        let value = decode_value(reader.read_exact(value_len)?)?;
        entries.push(Entry { key, value });
    }
    reader.finish()?;
    Ok(ListPage {
        entries,
        next_after,
    })
}

fn encode_part(tag: u8, payload: &[u8], out: &mut Vec<u8>) -> Result<(), Error> {
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| Error::Decode("kv payload too large".into()))?;
    out.push(tag);
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(&self) -> Result<(), Error> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::Decode(format!(
                "unexpected {} trailing kv bytes",
                self.bytes.len().saturating_sub(self.offset)
            )))
        }
    }

    fn read_key_part(&mut self) -> Result<KeyPart, Error> {
        match self.read_u8()? {
            KEY_PART_STRING => Ok(KeyPart::String(
                String::from_utf8(self.read_be_len_bytes()?)
                    .map_err(|_| Error::Decode("invalid UTF-8 in kv string".into()))?,
            )),
            KEY_PART_BYTES => Ok(KeyPart::Bytes(self.read_be_len_bytes()?)),
            KEY_PART_I64 => {
                let bytes = self.read_be_len_bytes()?;
                let sortable = u64::from_be_bytes(
                    bytes
                        .try_into()
                        .map_err(|_| Error::Decode("invalid kv i64 payload length".into()))?,
                );
                Ok(KeyPart::I64((sortable ^ (1_u64 << 63)) as i64))
            }
            KEY_PART_U64 => {
                let bytes = self.read_be_len_bytes()?;
                Ok(KeyPart::U64(u64::from_be_bytes(bytes.try_into().map_err(
                    |_| Error::Decode("invalid kv u64 payload length".into()),
                )?)))
            }
            KEY_PART_BOOL => match self.read_be_len_bytes()?.as_slice() {
                [0] => Ok(KeyPart::Bool(false)),
                [1] => Ok(KeyPart::Bool(true)),
                _ => Err(Error::Decode("invalid kv bool payload".into())),
            },
            other => Err(Error::Decode(format!("unknown kv part tag {other}"))),
        }
    }

    fn read_be_len_bytes(&mut self) -> Result<Vec<u8>, Error> {
        let len = u32::from_be_bytes(
            self.read_exact(4)?
                .try_into()
                .map_err(|_| Error::Decode("invalid kv length".into()))?,
        ) as usize;
        Ok(self.read_exact(len)?.to_vec())
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| Error::Decode("kv length overflow".into()))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::Decode("unexpected end of kv payload".into()))?;
        self.offset = end;
        Ok(bytes)
    }
}

#[macro_export]
macro_rules! kv_key {
    ($($part:expr),* $(,)?) => {
        vec![$($crate::kv::IntoKeyPart::into_key_part($part)),*]
    };
}
