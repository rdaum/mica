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
    FunctionBody, Item, Literal, MethodKind, MethodRole, NodeId, ObjectClause, Param, ParamMode,
    Span, UnaryOp,
};
pub use backend::{
    CompileContext, CompileError, CompiledProgram, InstalledMethod, InstalledRole,
    MethodInstallation, MethodRelations, SourceTaskError, SubmittedSourceTask, compile_semantic,
    compile_source, install_methods, install_methods_from_source, submit_source_task,
};
pub use hir::{
    HirArg, HirCatch, HirCollectionItem, HirExpr, HirFunctionBody, HirItem, HirParam, HirPlace,
    HirProgram, HirRelationAtom, HirScatterBinding,
};
pub use lexer::lex;
pub use lower::parse_ast;
pub use parser::parse;
pub use semantics::{
    Binding, BindingId, Diagnostic, DiagnosticCode, LocalKind, Reference, ResolvedName, Scope,
    ScopeId, SemanticProgram, analyze_ast, parse_semantic,
};
pub use syntax::{CstElement, CstNode, CstToken, Parse, ParseError, SyntaxKind, Token};
