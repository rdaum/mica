//! Frontend syntax support for Mica.
//!
//! This crate intentionally starts with parsing rather than bytecode emission.
//! The surface language still has open semantic questions around dispatch,
//! relation metadata, and filein/fileout expansion, so the first artifact is a
//! concrete syntax tree with source spans and recoverable parse errors.

mod ast;
mod lexer;
mod lower;
mod parser;
mod syntax;

pub use ast::{
    Arg, Ast, BinaryOp, BindingKind, BindingPattern, CatchClause, CollectionItem, EffectKind, Expr,
    FunctionBody, Item, Literal, MethodKind, ObjectClause, Param, ParamMode, UnaryOp,
};
pub use lexer::lex;
pub use lower::parse_ast;
pub use parser::parse;
pub use syntax::{CstElement, CstNode, CstToken, Parse, ParseError, SyntaxKind, Token};
