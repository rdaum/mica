//! Frontend syntax support for Mica.
//!
//! This crate intentionally starts with parsing rather than bytecode emission.
//! The surface language still has open semantic questions around dispatch,
//! relation metadata, and filein/fileout expansion, so the first artifact is a
//! concrete syntax tree with source spans and recoverable parse errors.

mod lexer;
mod parser;
mod syntax;

pub use lexer::lex;
pub use parser::parse;
pub use syntax::{CstElement, CstNode, CstToken, Parse, ParseError, SyntaxKind, Token};
