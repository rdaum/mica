// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com> This program is free
// software: you can redistribute it and/or modify it under the terms of the GNU
// Affero General Public License as published by the Free Software Foundation,
// version 3.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
// details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

use std::fmt;
use std::str;

pub const DEFAULT_MAX_HEADER_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;

const MAX_HEADERS: usize = 96;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpHeader {
    pub name: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub version: u8,
    pub headers: Vec<HttpHeader>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub reason: String,
    pub headers: Vec<HttpHeader>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HttpCodecError {
    HeaderTooLarge,
    TooManyHeaders,
    BodyTooLarge,
    InvalidRequest(String),
    InvalidContentLength,
    DuplicateContentLength,
    UnsupportedTransferEncoding,
    InvalidResponseReason,
    InvalidResponseHeaderName,
    InvalidResponseHeaderValue,
}

#[derive(Clone, Debug)]
pub struct HttpCodec {
    buffer: Vec<u8>,
    max_header_bytes: usize,
    max_body_bytes: usize,
}

impl fmt::Display for HttpCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeaderTooLarge => f.write_str("HTTP headers exceed maximum size"),
            Self::TooManyHeaders => f.write_str("HTTP request has too many headers"),
            Self::BodyTooLarge => f.write_str("HTTP body exceeds maximum size"),
            Self::InvalidRequest(error) => write!(f, "invalid HTTP request: {error}"),
            Self::InvalidContentLength => f.write_str("HTTP Content-Length is invalid"),
            Self::DuplicateContentLength => {
                f.write_str("HTTP request has multiple Content-Length headers")
            }
            Self::UnsupportedTransferEncoding => {
                f.write_str("HTTP transfer encoding is not supported")
            }
            Self::InvalidResponseReason => f.write_str("HTTP response reason is invalid"),
            Self::InvalidResponseHeaderName => f.write_str("HTTP response header name is invalid"),
            Self::InvalidResponseHeaderValue => {
                f.write_str("HTTP response header value is invalid")
            }
        }
    }
}

impl std::error::Error for HttpCodecError {}

impl Default for HttpCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpHeader {
    pub fn new(name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

impl HttpRequest {
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.as_slice())
    }

    pub fn connection_should_close(&self) -> bool {
        if header_has_token(self.header("connection"), "close") {
            return true;
        }
        self.version == 0 && !header_has_token(self.header("connection"), "keep-alive")
    }
}

impl HttpResponse {
    pub fn new(status: u16, reason: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            reason: reason.into(),
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub fn text(status: u16, reason: impl Into<String>, body: impl Into<String>) -> Self {
        let mut response = Self::new(status, reason, body.into().into_bytes());
        response.headers.push(HttpHeader::new(
            "Content-Type",
            b"text/plain; charset=utf-8",
        ));
        response
    }

    pub fn html(status: u16, reason: impl Into<String>, body: impl Into<String>) -> Self {
        let mut response = Self::new(status, reason, body.into().into_bytes());
        response
            .headers
            .push(HttpHeader::new("Content-Type", b"text/html; charset=utf-8"));
        response
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push(HttpHeader::new(name, value));
        self
    }
}

impl HttpCodec {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }

    pub fn with_limits(max_header_bytes: usize, max_body_bytes: usize) -> Self {
        Self {
            buffer: Vec::new(),
            max_header_bytes,
            max_body_bytes,
        }
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<Vec<HttpRequest>, HttpCodecError> {
        self.buffer.extend_from_slice(bytes);
        let mut requests = Vec::new();
        while let Some(request) = self.decode_one()? {
            requests.push(request);
        }
        Ok(requests)
    }

    fn decode_one(&mut self) -> Result<Option<HttpRequest>, HttpCodecError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        if self.buffer.len() > self.max_header_bytes && !has_header_end(&self.buffer) {
            return Err(HttpCodecError::HeaderTooLarge);
        }

        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut request = httparse::Request::new(&mut headers);
        let header_len = match request.parse(&self.buffer) {
            Ok(httparse::Status::Complete(header_len)) => header_len,
            Ok(httparse::Status::Partial) => return Ok(None),
            Err(httparse::Error::TooManyHeaders) => return Err(HttpCodecError::TooManyHeaders),
            Err(error) => return Err(HttpCodecError::InvalidRequest(error.to_string())),
        };
        if header_len > self.max_header_bytes {
            return Err(HttpCodecError::HeaderTooLarge);
        }

        let method = request
            .method
            .ok_or_else(|| HttpCodecError::InvalidRequest("missing method".to_owned()))?
            .to_owned();
        let path = request
            .path
            .ok_or_else(|| HttpCodecError::InvalidRequest("missing path".to_owned()))?
            .to_owned();
        let version = request
            .version
            .ok_or_else(|| HttpCodecError::InvalidRequest("missing version".to_owned()))?;
        let headers = request
            .headers
            .iter()
            .map(|header| HttpHeader::new(header.name, header.value))
            .collect::<Vec<_>>();

        reject_unsupported_transfer_encoding(&headers)?;
        let body_len = content_length(&headers)?;
        if body_len > self.max_body_bytes {
            return Err(HttpCodecError::BodyTooLarge);
        }
        let total_len = header_len
            .checked_add(body_len)
            .ok_or(HttpCodecError::BodyTooLarge)?;
        if self.buffer.len() < total_len {
            return Ok(None);
        }

        let body = self.buffer[header_len..total_len].to_vec();
        self.buffer.drain(..total_len);
        Ok(Some(HttpRequest {
            method,
            path,
            version,
            headers,
            body,
        }))
    }
}

pub fn encode_response(response: &HttpResponse, out: &mut Vec<u8>) -> Result<(), HttpCodecError> {
    validate_response(response)?;
    out.extend_from_slice(
        format!("HTTP/1.1 {} {}\r\n", response.status, response.reason).as_bytes(),
    );
    let mut has_content_length = false;
    for header in &response.headers {
        if header.name.eq_ignore_ascii_case("content-length") {
            has_content_length = true;
        }
        out.extend_from_slice(header.name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(&header.value);
        out.extend_from_slice(b"\r\n");
    }
    if !has_content_length {
        out.extend_from_slice(format!("Content-Length: {}\r\n", response.body.len()).as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&response.body);
    Ok(())
}

fn validate_response(response: &HttpResponse) -> Result<(), HttpCodecError> {
    if !is_valid_response_reason(&response.reason) {
        return Err(HttpCodecError::InvalidResponseReason);
    }
    for header in &response.headers {
        if !is_valid_header_name(&header.name) {
            return Err(HttpCodecError::InvalidResponseHeaderName);
        }
        if !is_valid_header_value(&header.value) {
            return Err(HttpCodecError::InvalidResponseHeaderValue);
        }
    }
    Ok(())
}

pub fn is_valid_header_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(is_header_name_byte)
}

pub fn is_valid_header_value(value: &[u8]) -> bool {
    value
        .iter()
        .all(|byte| matches!(byte, b'\t' | b' '..=b'~' | 0x80..=0xff))
}

pub fn is_valid_response_reason(reason: &str) -> bool {
    reason
        .bytes()
        .all(|byte| matches!(byte, b'\t' | b' '..=b'~' | 0x80..=0xff))
}

fn is_header_name_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'!' | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
            | b'0'..=b'9'
            | b'A'..=b'Z'
            | b'a'..=b'z'
    )
}

fn content_length(headers: &[HttpHeader]) -> Result<usize, HttpCodecError> {
    let mut length = None;
    for header in headers {
        if !header.name.eq_ignore_ascii_case("content-length") {
            continue;
        }
        if length.is_some() {
            return Err(HttpCodecError::DuplicateContentLength);
        }
        let value = ascii_trim(&header.value);
        let value = str::from_utf8(value).map_err(|_| HttpCodecError::InvalidContentLength)?;
        length = Some(
            value
                .parse::<usize>()
                .map_err(|_| HttpCodecError::InvalidContentLength)?,
        );
    }
    Ok(length.unwrap_or(0))
}

fn reject_unsupported_transfer_encoding(headers: &[HttpHeader]) -> Result<(), HttpCodecError> {
    for header in headers {
        if !header.name.eq_ignore_ascii_case("transfer-encoding") {
            continue;
        }
        if header_tokens(Some(&header.value)).any(|token| token.eq_ignore_ascii_case("chunked")) {
            return Err(HttpCodecError::UnsupportedTransferEncoding);
        }
        return Err(HttpCodecError::UnsupportedTransferEncoding);
    }
    Ok(())
}

fn header_has_token(value: Option<&[u8]>, token: &str) -> bool {
    header_tokens(value).any(|candidate| candidate.eq_ignore_ascii_case(token))
}

fn header_tokens(value: Option<&[u8]>) -> impl Iterator<Item = &str> {
    value
        .and_then(|value| str::from_utf8(value).ok())
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
}

fn ascii_trim(value: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = value.len();
    while start < end && value[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && value[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &value[start..end]
}

fn has_header_end(bytes: &[u8]) -> bool {
    bytes.windows(4).any(|window| window == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_complete_get_request() {
        let mut codec = HttpCodec::new();

        let requests = codec
            .decode(b"GET /healthz HTTP/1.1\r\nHost: example.test\r\n\r\n")
            .unwrap();

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/healthz");
        assert_eq!(requests[0].version, 1);
        assert_eq!(requests[0].header("host"), Some(b"example.test".as_slice()));
    }

    #[test]
    fn preserves_partial_request_across_decodes() {
        let mut codec = HttpCodec::new();

        assert_eq!(codec.decode(b"GET / HTTP/1.1\r\nHo").unwrap(), vec![]);
        let requests = codec.decode(b"st: example.test\r\n\r\n").unwrap();

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/");
    }

    #[test]
    fn parses_content_length_body() {
        let mut codec = HttpCodec::new();

        let requests = codec
            .decode(b"POST /input HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello")
            .unwrap();

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body, b"hello");
    }

    #[test]
    fn waits_for_incomplete_body() {
        let mut codec = HttpCodec::new();

        assert_eq!(
            codec
                .decode(b"POST /input HTTP/1.1\r\nContent-Length: 5\r\n\r\nhe")
                .unwrap(),
            vec![]
        );
        let requests = codec.decode(b"llo").unwrap();

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body, b"hello");
    }

    #[test]
    fn parses_pipelined_requests() {
        let mut codec = HttpCodec::new();

        let requests = codec
            .decode(b"GET /a HTTP/1.1\r\n\r\nGET /b HTTP/1.1\r\n\r\n")
            .unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/a");
        assert_eq!(requests[1].path, "/b");
    }

    #[test]
    fn rejects_chunked_transfer_encoding() {
        let mut codec = HttpCodec::new();

        assert_eq!(
            codec.decode(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"),
            Err(HttpCodecError::UnsupportedTransferEncoding)
        );
    }

    #[test]
    fn rejects_oversized_header() {
        let mut codec = HttpCodec::with_limits(8, DEFAULT_MAX_BODY_BYTES);

        assert_eq!(
            codec.decode(b"GET /really-long HTTP/1.1\r\n"),
            Err(HttpCodecError::HeaderTooLarge)
        );
    }

    #[test]
    fn rejects_oversized_body() {
        let mut codec = HttpCodec::with_limits(DEFAULT_MAX_HEADER_BYTES, 2);

        assert_eq!(
            codec.decode(b"POST / HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc"),
            Err(HttpCodecError::BodyTooLarge)
        );
    }

    #[test]
    fn encodes_response_with_content_length() {
        let mut out = Vec::new();
        encode_response(&HttpResponse::text(200, "OK", "ok\n"), &mut out).unwrap();

        assert_eq!(
            out,
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 3\r\n\r\nok\n"
        );
    }

    #[test]
    fn rejects_invalid_response_header_name() {
        let mut out = Vec::new();
        let response =
            HttpResponse::new(200, "OK", Vec::new()).with_header("x-bad\r\nset-cookie", b"1");

        assert_eq!(
            encode_response(&response, &mut out),
            Err(HttpCodecError::InvalidResponseHeaderName)
        );
        assert!(out.is_empty());
    }

    #[test]
    fn rejects_invalid_response_header_value() {
        let mut out = Vec::new();
        let response = HttpResponse::new(200, "OK", Vec::new()).with_header("x-test", b"ok\r\nbad");

        assert_eq!(
            encode_response(&response, &mut out),
            Err(HttpCodecError::InvalidResponseHeaderValue)
        );
        assert!(out.is_empty());
    }

    #[test]
    fn rejects_invalid_response_reason() {
        let mut out = Vec::new();
        let response = HttpResponse::new(200, "OK\r\nInjected", Vec::new());

        assert_eq!(
            encode_response(&response, &mut out),
            Err(HttpCodecError::InvalidResponseReason)
        );
        assert!(out.is_empty());
    }
}
