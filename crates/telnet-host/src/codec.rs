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

const IAC: u8 = 0xff;
const SE: u8 = 0xf0;
const SB: u8 = 0xfa;
const WILL: u8 = 0xfb;
const WONT: u8 = 0xfc;
const DO: u8 = 0xfd;
const DONT: u8 = 0xfe;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelnetMode {
    Text,
    Binary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelnetItem {
    Line(String),
    Bytes(Vec<u8>),
    Command(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelnetCodecError {
    MaxLineLengthExceeded,
}

impl fmt::Display for TelnetCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxLineLengthExceeded => f.write_str("maximum telnet line length exceeded"),
        }
    }
}

impl std::error::Error for TelnetCodecError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TelnetState {
    Normal,
    Iac,
    WillWontDoDont,
    Subneg,
    SubnegIac,
}

#[derive(Clone, Debug)]
pub struct TelnetCodec {
    mode: TelnetMode,
    state: TelnetState,
    line: Vec<u8>,
    command: Vec<u8>,
    max_line_length: Option<usize>,
    last_input_was_cr: bool,
}

impl Default for TelnetCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl TelnetCodec {
    pub fn new() -> Self {
        Self {
            mode: TelnetMode::Text,
            state: TelnetState::Normal,
            line: Vec::new(),
            command: Vec::new(),
            max_line_length: None,
            last_input_was_cr: false,
        }
    }

    pub fn with_max_line_length(max_line_length: usize) -> Self {
        Self {
            max_line_length: Some(max_line_length),
            ..Self::new()
        }
    }

    pub const fn mode(&self) -> TelnetMode {
        self.mode
    }

    pub fn set_mode(&mut self, mode: TelnetMode) {
        self.mode = mode;
        if mode == TelnetMode::Binary {
            self.state = TelnetState::Normal;
            self.command.clear();
        }
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<Vec<TelnetItem>, TelnetCodecError> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        match self.mode {
            TelnetMode::Binary => Ok(vec![TelnetItem::Bytes(bytes.to_vec())]),
            TelnetMode::Text => self.decode_text(bytes),
        }
    }

    fn decode_text(&mut self, bytes: &[u8]) -> Result<Vec<TelnetItem>, TelnetCodecError> {
        let mut items = Vec::new();
        for &byte in bytes {
            match self.state {
                TelnetState::Normal => self.decode_normal_byte(byte, &mut items)?,
                TelnetState::Iac => self.decode_iac_byte(byte, &mut items),
                TelnetState::WillWontDoDont => {
                    self.command.push(byte);
                    self.state = TelnetState::Normal;
                    items.push(TelnetItem::Command(std::mem::take(&mut self.command)));
                }
                TelnetState::Subneg => {
                    self.command.push(byte);
                    if byte == IAC {
                        self.state = TelnetState::SubnegIac;
                    }
                }
                TelnetState::SubnegIac => {
                    self.command.push(byte);
                    match byte {
                        SE => {
                            self.state = TelnetState::Normal;
                            items.push(TelnetItem::Command(std::mem::take(&mut self.command)));
                        }
                        IAC => self.state = TelnetState::Subneg,
                        _ => self.state = TelnetState::Subneg,
                    }
                }
            }
        }
        Ok(items)
    }

    fn decode_normal_byte(
        &mut self,
        byte: u8,
        items: &mut Vec<TelnetItem>,
    ) -> Result<(), TelnetCodecError> {
        if byte == IAC {
            self.state = TelnetState::Iac;
            self.command.clear();
            self.command.push(byte);
            return Ok(());
        }
        if byte == b'\r' || (byte == b'\n' && !self.last_input_was_cr) {
            self.last_input_was_cr = byte == b'\r';
            let line = String::from_utf8_lossy(&self.line).into_owned();
            self.line.clear();
            items.push(TelnetItem::Line(line));
            return Ok(());
        }
        if byte == b'\n' && self.last_input_was_cr {
            self.last_input_was_cr = false;
            return Ok(());
        }

        self.last_input_was_cr = false;
        if byte == b'\t' || (byte >= 0x20 && byte != 0x7f) {
            self.line.push(byte);
            if self
                .max_line_length
                .is_some_and(|max| self.line.len() > max)
            {
                self.line.clear();
                return Err(TelnetCodecError::MaxLineLengthExceeded);
            }
        }
        Ok(())
    }

    fn decode_iac_byte(&mut self, byte: u8, items: &mut Vec<TelnetItem>) {
        self.command.push(byte);
        match byte {
            IAC => {
                self.state = TelnetState::Normal;
            }
            SB => {
                self.state = TelnetState::Subneg;
            }
            WILL | WONT | DO | DONT => {
                self.state = TelnetState::WillWontDoDont;
            }
            _ => {
                self.state = TelnetState::Normal;
                items.push(TelnetItem::Command(std::mem::take(&mut self.command)));
            }
        }
    }
}

pub fn encode_telnet_line(line: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(normalize_telnet_line_endings(line).as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn encode_telnet_raw_text(text: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(normalize_telnet_line_endings(text).as_bytes());
}

fn normalize_telnet_line_endings(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                normalized.push_str("\r\n");
            }
            '\n' => normalized.push_str("\r\n"),
            _ => normalized.push(ch),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_mode_parses_cr_lf_and_crlf_lines() {
        let mut codec = TelnetCodec::new();

        assert_eq!(
            codec.decode(b"hello\nworld\r\nagain\r").unwrap(),
            vec![
                TelnetItem::Line("hello".to_owned()),
                TelnetItem::Line("world".to_owned()),
                TelnetItem::Line("again".to_owned()),
            ]
        );
        assert_eq!(
            codec.decode(b"\nafter\n").unwrap(),
            vec![TelnetItem::Line("after".to_owned())]
        );
    }

    #[test]
    fn text_mode_filters_control_bytes_and_preserves_lossy_utf8() {
        let mut codec = TelnetCodec::new();

        assert_eq!(
            codec.decode(b"a\x01\tb\xc0c\n").unwrap(),
            vec![TelnetItem::Line("a\tb\u{fffd}c".to_owned())]
        );
    }

    #[test]
    fn text_mode_emits_two_and_three_byte_telnet_commands() {
        let mut codec = TelnetCodec::new();

        assert_eq!(
            codec.decode(&[IAC, 0xf1, b'h', b'i', b'\n']).unwrap(),
            vec![
                TelnetItem::Command(vec![IAC, 0xf1]),
                TelnetItem::Line("hi".to_owned()),
            ]
        );
        assert_eq!(
            codec.decode(&[IAC, WILL, 1]).unwrap(),
            vec![TelnetItem::Command(vec![IAC, WILL, 1])]
        );
    }

    #[test]
    fn text_mode_emits_subnegotiation_command() {
        let mut codec = TelnetCodec::new();

        assert_eq!(
            codec.decode(&[IAC, SB, 24, b'x', IAC, SE]).unwrap(),
            vec![TelnetItem::Command(vec![IAC, SB, 24, b'x', IAC, SE])]
        );
    }

    #[test]
    fn text_mode_preserves_incomplete_telnet_commands_across_decodes() {
        let mut codec = TelnetCodec::new();

        assert_eq!(codec.decode(&[IAC]).unwrap(), vec![]);
        assert_eq!(
            codec.decode(&[0xf1]).unwrap(),
            vec![TelnetItem::Command(vec![IAC, 0xf1])]
        );
        assert_eq!(codec.decode(&[IAC, DO]).unwrap(), vec![]);
        assert_eq!(
            codec.decode(&[24, b'h', b'i', b'\n']).unwrap(),
            vec![
                TelnetItem::Command(vec![IAC, DO, 24]),
                TelnetItem::Line("hi".to_owned()),
            ]
        );
    }

    #[test]
    fn text_mode_preserves_incomplete_subnegotiation_across_decodes() {
        let mut codec = TelnetCodec::new();

        assert_eq!(codec.decode(&[IAC, SB, 24, 0, 80]).unwrap(), vec![]);
        assert_eq!(codec.decode(&[0, 24, IAC]).unwrap(), vec![]);
        assert_eq!(
            codec.decode(&[SE, b'o', b'k', b'\n']).unwrap(),
            vec![
                TelnetItem::Command(vec![IAC, SB, 24, 0, 80, 0, 24, IAC, SE]),
                TelnetItem::Line("ok".to_owned()),
            ]
        );
    }

    #[test]
    fn text_mode_discards_escaped_iac_in_text_input() {
        let mut codec = TelnetCodec::new();

        assert_eq!(
            codec.decode(&[b'a', IAC, IAC, b'b', b'\n']).unwrap(),
            vec![TelnetItem::Line("ab".to_owned())]
        );
    }

    #[test]
    fn binary_mode_passes_bytes_through() {
        let mut codec = TelnetCodec::new();
        codec.set_mode(TelnetMode::Binary);

        assert_eq!(
            codec.decode(b"hello\n\xff").unwrap(),
            vec![TelnetItem::Bytes(b"hello\n\xff".to_vec())]
        );
    }

    #[test]
    fn max_line_length_errors_and_clears_current_line() {
        let mut codec = TelnetCodec::with_max_line_length(3);

        assert_eq!(
            codec.decode(b"abcd"),
            Err(TelnetCodecError::MaxLineLengthExceeded)
        );
        assert_eq!(
            codec.decode(b"ok\n").unwrap(),
            vec![TelnetItem::Line("ok".to_owned())]
        );
    }

    #[test]
    fn telnet_line_output_uses_crlf() {
        let mut out = Vec::new();
        encode_telnet_line("a\nb\rc\r\nd", &mut out);

        assert_eq!(out, b"a\r\nb\r\nc\r\nd\r\n");
    }
}
