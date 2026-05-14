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

//! Frontend syntax support for Mica.
//!
//! This crate intentionally starts with parsing rather than bytecode emission.
//! The surface language still has open semantic questions around dispatch,
//! relation metadata, and filein/fileout expansion, so the first artifact is a
//! concrete syntax tree with source spans and recoverable parse errors.

mod ast;
mod backend;
mod hir;
mod lexer;
mod lower;
mod parser;
mod semantics;
mod syntax;

pub use ast::{
    Arg, Ast, BinaryOp, BindingKind, BindingPattern, CatchClause, CollectionItem, EffectKind, Expr,
    FunctionBody, Item, Literal, MethodKind, MethodParam, NodeId, ObjectClause, Param, ParamMode,
    RecoveryClause, Span, UnaryOp,
};
pub use backend::{
    CompileContext, CompileError, CompiledProgram, InstalledMethod, InstalledParam,
    MethodInstallation, MethodRelations, RuleInstallation, compile_semantic, compile_source,
    install_methods, install_methods_from_source, install_rules, install_rules_from_source,
};
pub use hir::{
    HirArg, HirCatch, HirCollectionItem, HirExpr, HirFunctionBody, HirItem, HirParam, HirPlace,
    HirProgram, HirRecovery, HirRelationAtom, HirScatterBinding,
};
pub use lexer::lex;
pub use lower::parse_ast;
pub use parser::parse;
pub use semantics::{
    Binding, BindingId, Diagnostic, DiagnosticCode, LocalKind, Reference, ResolvedName, Scope,
    ScopeId, SemanticProgram, analyze_ast, parse_semantic,
};
pub use syntax::{CstElement, CstNode, CstToken, Parse, ParseError, SyntaxKind, Token};
