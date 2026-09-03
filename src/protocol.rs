use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rivetkit::{Request, Response};
use serde_json::Value;

pub const ZERO_OFFSET: &str = "0000000000000000_0000000000000000";
pub const MAX_BODY_BYTES: usize = 1_000_000;
pub const MAX_READ_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_READ_ROWS: usize = 128;
pub const LONG_POLL_TIMEOUT_MS: u64 = 2_000;
pub const MAX_SSE_LIFETIME_MS: u64 = 60_000;
pub const MAX_TTL_SECONDS: i64 = 3_153_600_000;
pub const PRODUCER_STATE_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
pub const INTERNAL_HEADER_PREFIX: &str = "x-rivet-ds-h-";

pub const STREAM_OFFSET: &str = "stream-next-offset";
pub const STREAM_CURSOR: &str = "stream-cursor";
pub const STREAM_UP_TO_DATE: &str = "stream-up-to-date";
pub const STREAM_CLOSED: &str = "stream-closed";
pub const STREAM_SEQ: &str = "stream-seq";
pub const STREAM_TTL: &str = "stream-ttl";
pub const STREAM_EXPIRES_AT: &str = "stream-expires-at";
pub const STREAM_FORKED_FROM: &str = "stream-forked-from";
pub const STREAM_FORK_OFFSET: &str = "stream-fork-offset";
pub const STREAM_FORK_SUB_OFFSET: &str = "stream-fork-sub-offset";
pub const PRODUCER_ID: &str = "producer-id";
pub const PRODUCER_EPOCH: &str = "producer-epoch";
pub const PRODUCER_SEQ: &str = "producer-seq";
pub const PRODUCER_EXPECTED_SEQ: &str = "producer-expected-seq";
pub const PRODUCER_RECEIVED_SEQ: &str = "producer-received-seq";
pub const SSE_DATA_ENCODING: &str = "stream-sse-data-encoding";

pub const BRIDGED_REQUEST_HEADERS: &[&str] = &[
    "content-type",
    "if-none-match",
    STREAM_CLOSED,
    STREAM_SEQ,
    STREAM_TTL,
    STREAM_EXPIRES_AT,
    STREAM_FORKED_FROM,
    STREAM_FORK_OFFSET,
    STREAM_FORK_SUB_OFFSET,
    PRODUCER_ID,
    PRODUCER_EPOCH,
    PRODUCER_SEQ,
];

pub fn response(
    status: u16,
    headers: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    body: impl Into<Vec<u8>>,
) -> Result<Response> {
    Response::from_parts(
        status,
        headers
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect(),
        body.into(),
    )
}

pub fn text_response(
    status: u16,
    message: impl Into<String>,
    extra_headers: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
) -> Result<Response> {
    let mut headers = HashMap::from([
        (
            "content-type".to_owned(),
            "text/plain; charset=utf-8".to_owned(),
        ),
        ("cache-control".to_owned(), "no-store".to_owned()),
    ]);
    headers.extend(
        extra_headers
            .into_iter()
            .map(|(name, value)| (name.into(), value.into())),
    );
    Response::from_parts(status, headers, message.into().into_bytes())
}

pub fn public_header_bytes(request: &Request, name: &str) -> Result<Option<Vec<u8>>> {
    let bridged_name = format!("{INTERNAL_HEADER_PREFIX}{name}");
    if let Some(value) = request.headers().get(&bridged_name) {
        return URL_SAFE_NO_PAD
            .decode(value.as_bytes())
            .map(Some)
            .map_err(|error| anyhow!("invalid bridged {name} header: {error}"));
    }
    Ok(request
        .headers()
        .get(name)
        .map(|value| value.as_bytes().to_vec()))
}

pub fn public_header_text(request: &Request, name: &str) -> Result<Option<String>> {
    public_header_bytes(request, name)?
        .map(|bytes| String::from_utf8(bytes).map_err(anyhow::Error::from))
        .transpose()
}

pub fn header_is_true(request: &Request, name: &str) -> Result<bool> {
    Ok(public_header_text(request, name)?.is_some_and(|value| value.eq_ignore_ascii_case("true")))
}

pub fn normalize_content_type(value: Option<&str>) -> String {
    value
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

pub fn sanitize_content_type(value: Option<String>, is_fork: bool) -> Option<String> {
    match value {
        Some(value)
            if !value.trim().is_empty()
                && value
                    .bytes()
                    .all(|byte| byte == b'\t' || (0x20..=0x7e).contains(&byte)) =>
        {
            Some(value)
        }
        _ if is_fork => None,
        _ => Some("application/octet-stream".to_owned()),
    }
}

pub fn validate_offset(value: &str) -> bool {
    if value == "-1" || value == "now" {
        return true;
    }
    let Some((read_seq, byte_offset)) = value.split_once('_') else {
        return false;
    };
    !read_seq.is_empty()
        && !byte_offset.is_empty()
        && read_seq.bytes().all(|byte| byte.is_ascii_digit())
        && byte_offset.bytes().all(|byte| byte.is_ascii_digit())
}

pub fn advance_offset(current: &str, payload_len: usize) -> Result<String> {
    let (read_seq, byte_offset) = current
        .split_once('_')
        .ok_or_else(|| anyhow!("invalid current offset"))?;
    let read_seq: u64 = read_seq.parse()?;
    let byte_offset: u64 = byte_offset.parse()?;
    let next = byte_offset
        .checked_add(5)
        .and_then(|value| value.checked_add(payload_len as u64))
        .ok_or_else(|| anyhow!("stream offset overflow"))?;
    Ok(format!("{read_seq:016}_{next:016}"))
}

pub fn parse_non_negative_safe_integer(value: &str, field: &str) -> Result<i64> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        bail!("Invalid {field}: must be a non-negative integer");
    }
    let parsed: i64 = value
        .parse()
        .map_err(|_| anyhow!("Invalid {field}: must be a non-negative integer"))?;
    if parsed > 9_007_199_254_740_991 {
        bail!("Invalid {field}: must be a non-negative integer");
    }
    Ok(parsed)
}

/// Converts an append into the protocol's comma-terminated storage
/// representation. A later compatibility pass will replace serde's number
/// formatting with the ECMAScript formatter for the remaining numeric edge
/// cases.
pub fn process_json_append(data: &[u8], initial_create: bool) -> Result<Vec<u8>> {
    let parsed: Value = serde_json::from_slice(data).map_err(|_| anyhow!("Invalid JSON"))?;
    let values = match parsed {
        Value::Array(values) => {
            if values.is_empty() {
                if initial_create {
                    return Ok(Vec::new());
                }
                bail!("Empty arrays are not allowed");
            }
            values
        }
        value => vec![value],
    };
    let mut output = Vec::new();
    for value in values {
        serde_json::to_writer(&mut output, &value)?;
        output.push(b',');
    }
    Ok(output)
}

pub fn format_json_messages(messages: &[Vec<u8>]) -> Vec<u8> {
    if messages.is_empty() {
        return b"[]".to_vec();
    }
    let total = messages.iter().map(Vec::len).sum::<usize>();
    let mut output = Vec::with_capacity(total + 2);
    output.push(b'[');
    for message in messages {
        output.extend_from_slice(message);
    }
    if output.last() == Some(&b',') {
        output.pop();
    }
    output.push(b']');
    output
}

pub fn concatenate(messages: &[Vec<u8>]) -> Vec<u8> {
    let mut output = Vec::with_capacity(messages.iter().map(Vec::len).sum());
    for message in messages {
        output.extend_from_slice(message);
    }
    output
}

pub fn encode_sse_data(value: &str) -> String {
    let value = value.replace("\r\n", "\n").replace('\r', "\n");
    let mut encoded = String::new();
    for line in value.split('\n') {
        encoded.push_str("data:");
        encoded.push_str(line);
        encoded.push('\n');
    }
    encoded.push('\n');
    encoded
}

pub fn response_cursor(request_cursor: Option<&str>) -> String {
    const CURSOR_EPOCH_MS: i64 = 1_728_432_000_000;
    const INTERVAL_MS: i64 = 20_000;
    let current = (chrono::Utc::now().timestamp_millis() - CURSOR_EPOCH_MS) / INTERVAL_MS;
    let Some(cursor) = request_cursor.filter(|cursor| {
        !cursor.is_empty() && cursor.len() <= 15 && cursor.bytes().all(|byte| byte.is_ascii_digit())
    }) else {
        return current.to_string();
    };
    let Ok(client) = cursor.parse::<i64>() else {
        return current.to_string();
    };
    if client < current {
        return current.to_string();
    }
    let random = uuid::Uuid::new_v4();
    let bytes = random.as_bytes();
    let jitter_seconds = 1 + u16::from_be_bytes([bytes[0], bytes[1]]) as i64 % 3_600;
    let jitter_intervals = (jitter_seconds + 19) / 20;
    (client + jitter_intervals.max(1)).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_are_lexically_sortable() {
        let first = advance_offset(ZERO_OFFSET, 3).unwrap();
        let second = advance_offset(&first, 20).unwrap();
        assert_eq!(first, "0000000000000000_0000000000000008");
        assert!(second > first);
    }

    #[test]
    fn json_arrays_flatten_into_one_fragment() {
        let encoded = process_json_append(br#"[ {"a": 1}, 2 ]"#, false).unwrap();
        assert_eq!(encoded, br#"{"a":1},2,"#);
        assert_eq!(format_json_messages(&[encoded]), br#"[{"a":1},2]"#);
    }

    #[test]
    fn offset_validation_matches_protocol_shape() {
        assert!(validate_offset("-1"));
        assert!(validate_offset(ZERO_OFFSET));
        assert!(validate_offset("0_0"));
        assert!(!validate_offset("_0"));
        assert!(!validate_offset("0_"));
        assert!(!validate_offset("0000000000000000_000000000000000x"));
    }

    #[test]
    fn cursors_are_numeric_and_advance_client_collisions() {
        let current = response_cursor(None);
        assert!(current.bytes().all(|byte| byte.is_ascii_digit()));
        let advanced = response_cursor(Some(&current));
        assert!(advanced.parse::<i64>().unwrap() > current.parse::<i64>().unwrap());
    }

    #[test]
    fn sse_data_uses_exact_prefix_and_normalizes_line_endings() {
        assert_eq!(
            encode_sse_data("a\r\nb\rc\n"),
            "data:a\ndata:b\ndata:c\ndata:\n\n"
        );
    }
}
