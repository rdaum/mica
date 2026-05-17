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

use crate::{
    Arg, Ast, BindingKind, BindingPattern, CatchClause, CollectionItem, EffectKind, Expr,
    FunctionBody, HirArg, HirCatch, HirCollectionItem, HirExpr, HirFunctionBody, HirItem, HirParam,
    HirPlace, HirProgram, HirRecovery, HirRelationAtom, HirScatterBinding, Item, NodeId, Param,
    ParamMode, ParseError, RecoveryClause, Span, parse_ast,
};
use std::collections::{BTreeSet, HashMap};

pub fn parse_semantic(source: &str) -> SemanticProgram {
    let ast = parse_ast(source);
    analyze_ast(&ast)
}

pub fn analyze_ast(ast: &Ast) -> SemanticProgram {
    Analyzer::new(ast).analyze()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ScopeId(pub u32);

impl ScopeId {
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BindingId(pub u32);

impl BindingId {
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticProgram {
    pub hir: HirProgram,
    pub spans: HashMap<NodeId, Span>,
    pub scopes: Vec<Scope>,
    pub bindings: Vec<Binding>,
    pub references: Vec<Reference>,
    pub captures: HashMap<NodeId, Vec<BindingId>>,
    pub diagnostics: Vec<Diagnostic>,
    pub parse_errors: Vec<ParseError>,
}

impl SemanticProgram {
    pub fn span(&self, node: NodeId) -> Option<&Span> {
        self.spans.get(&node)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Scope {
    pub id: ScopeId,
    pub parent: Option<ScopeId>,
    pub owner: Option<NodeId>,
    pub bindings: Vec<BindingId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Binding {
    pub id: BindingId,
    pub name: String,
    pub kind: LocalKind,
    pub mutable: bool,
    pub scope: ScopeId,
    pub declared_at: NodeId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalKind {
    Let,
    Const,
    Param,
    OptionalParam,
    RestParam,
    Loop,
    Catch,
    Function,
}

impl LocalKind {
    fn mutable_by_default(&self) -> bool {
        matches!(self, Self::Let | Self::Loop | Self::Catch | Self::Function)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reference {
    pub node: NodeId,
    pub name: String,
    pub resolution: ResolvedName,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedName {
    Local(BindingId),
    External {
        name: String,
        kind: ExternalNameKind,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalNameKind {
    Relation,
    Runtime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    pub code: DiagnosticCode,
    pub node: NodeId,
    pub span: Span,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiagnosticCode {
    DuplicateBinding,
    AssignToConst,
    InvalidAssignmentTarget,
    InvalidFactChange,
    InvalidRelationRule,
    UnsupportedSyntax,
}

struct Analyzer<'a> {
    ast: &'a Ast,
    spans: HashMap<NodeId, Span>,
    scopes: Vec<Scope>,
    bindings: Vec<Binding>,
    references: Vec<Reference>,
    captures: HashMap<NodeId, BTreeSet<BindingId>>,
    diagnostics: Vec<Diagnostic>,
    function_stack: Vec<FunctionContext>,
}

#[derive(Clone, Copy)]
struct FunctionContext {
    owner: NodeId,
    scope: ScopeId,
}

impl<'a> Analyzer<'a> {
    fn new(ast: &'a Ast) -> Self {
        let mut spans = HashMap::new();
        collect_item_spans(&ast.items, &mut spans);
        Self {
            ast,
            spans,
            scopes: Vec::new(),
            bindings: Vec::new(),
            references: Vec::new(),
            captures: HashMap::new(),
            diagnostics: Vec::new(),
            function_stack: Vec::new(),
        }
    }

    fn analyze(mut self) -> SemanticProgram {
        let root_scope = self.alloc_scope(None, None);
        let items = self.lower_items(&self.ast.items, root_scope);
        self.validate_supported_surface_items(&items);
        SemanticProgram {
            hir: HirProgram { items },
            spans: self.spans,
            scopes: self.scopes,
            bindings: self.bindings,
            references: self.references,
            captures: self
                .captures
                .into_iter()
                .map(|(node, bindings)| (node, bindings.into_iter().collect()))
                .collect(),
            diagnostics: self.diagnostics,
            parse_errors: self.ast.errors.clone(),
        }
    }

    fn alloc_scope(&mut self, parent: Option<ScopeId>, owner: Option<NodeId>) -> ScopeId {
        let id = ScopeId(self.scopes.len() as u32);
        self.scopes.push(Scope {
            id,
            parent,
            owner,
            bindings: Vec::new(),
        });
        id
    }

    fn declare(
        &mut self,
        scope: ScopeId,
        name: impl Into<String>,
        kind: LocalKind,
        declared_at: NodeId,
        span: &Span,
    ) -> BindingId {
        let name = name.into();
        if self.binding_in_scope(scope, &name).is_some() {
            self.diagnostic(
                DiagnosticCode::DuplicateBinding,
                declared_at,
                span.clone(),
                format!("duplicate binding `{name}` in this scope"),
            );
        }
        let id = BindingId(self.bindings.len() as u32);
        let binding = Binding {
            id,
            name,
            mutable: kind.mutable_by_default(),
            kind,
            scope,
            declared_at,
        };
        self.bindings.push(binding);
        self.scopes[scope.0 as usize].bindings.push(id);
        id
    }

    fn binding_in_scope(&self, scope: ScopeId, name: &str) -> Option<BindingId> {
        self.scopes[scope.0 as usize]
            .bindings
            .iter()
            .copied()
            .find(|binding| self.bindings[binding.0 as usize].name == name)
    }

    fn resolve(&mut self, name: &str, node: NodeId, scope: ScopeId) -> ResolvedName {
        let mut current = Some(scope);
        while let Some(scope_id) = current {
            if let Some(binding) = self.binding_in_scope(scope_id, name) {
                self.record_capture(binding, scope);
                let resolution = ResolvedName::Local(binding);
                self.references.push(Reference {
                    node,
                    name: name.to_owned(),
                    resolution: resolution.clone(),
                });
                return resolution;
            }
            current = self.scopes[scope_id.0 as usize].parent;
        }

        let resolution = ResolvedName::External {
            name: name.to_owned(),
            kind: if looks_like_relation_name(name) {
                ExternalNameKind::Relation
            } else {
                ExternalNameKind::Runtime
            },
        };
        self.references.push(Reference {
            node,
            name: name.to_owned(),
            resolution: resolution.clone(),
        });
        resolution
    }

    fn record_capture(&mut self, binding: BindingId, use_scope: ScopeId) {
        let binding_scope = self.bindings[binding.0 as usize].scope;
        for context in self.function_stack.iter().rev() {
            if context.scope == binding_scope {
                break;
            }
            if self.scope_contains(context.scope, use_scope) {
                self.captures
                    .entry(context.owner)
                    .or_default()
                    .insert(binding);
            }
        }
    }

    fn scope_contains(&self, ancestor: ScopeId, mut descendant: ScopeId) -> bool {
        loop {
            if ancestor == descendant {
                return true;
            }
            let Some(parent) = self.scopes[descendant.0 as usize].parent else {
                return false;
            };
            descendant = parent;
        }
    }

    fn validate_supported_surface_items(&mut self, items: &[HirItem]) {
        for item in items {
            match item {
                HirItem::Expr { expr, .. } => self.validate_supported_surface_expr(expr, false),
                HirItem::RelationRule { head, body, .. } => {
                    self.validate_relation_atom_support(head, true, false, false);
                    for atom in body {
                        self.validate_relation_atom_support(atom, true, false, false);
                    }
                }
                HirItem::Method { body, .. } => self.validate_supported_surface_items(body),
            }
        }
    }

    fn validate_supported_surface_expr(&mut self, expr: &HirExpr, _direct_function_binding: bool) {
        match expr {
            HirExpr::QueryVar { id, .. } => {
                self.unsupported(*id, "query variables are only valid as relation arguments");
            }
            HirExpr::List { items, .. } => {
                for item in items {
                    match item {
                        HirCollectionItem::Expr(expr) | HirCollectionItem::Splice(expr) => {
                            self.validate_supported_surface_expr(expr, false);
                        }
                    }
                }
            }
            HirExpr::Map { entries, .. } => {
                for (key, value) in entries {
                    self.validate_supported_surface_expr(key, false);
                    self.validate_supported_surface_expr(value, false);
                }
            }
            HirExpr::Unary { expr, .. } => self.validate_supported_surface_expr(expr, false),
            HirExpr::Binary {
                op: crate::BinaryOp::Range,
                left,
                right,
                ..
            } => {
                self.validate_supported_surface_expr(left, false);
                if !matches!(right.as_ref(), HirExpr::Hole { .. }) {
                    self.validate_supported_surface_expr(right, false);
                }
            }
            HirExpr::Binary { left, right, .. } => {
                self.validate_supported_surface_expr(left, false);
                self.validate_supported_surface_expr(right, false);
            }
            HirExpr::Assign { target, value, .. } => {
                self.validate_supported_surface_place(target);
                self.validate_supported_surface_expr(value, false);
            }
            HirExpr::Call { id, callee, args } => {
                if args.iter().any(|arg| arg.role.is_some()) {
                    self.unsupported(*id, "ordinary calls only support positional arguments");
                }
                if !matches!(
                    callee.as_ref(),
                    HirExpr::LocalRef { .. } | HirExpr::ExternalRef { .. }
                ) && let Some(arg) = args.iter().find(|arg| arg.splice)
                {
                    self.unsupported(
                        arg.id,
                        "argument splices are only supported for direct local or runtime calls",
                    );
                }
                self.validate_supported_surface_expr(callee, false);
                self.validate_args(args);
            }
            HirExpr::RoleDispatch { id, selector, args } => {
                self.validate_dispatch_args(*id, args, "dispatch");
                self.validate_supported_surface_expr(selector, false);
                self.validate_args(args);
            }
            HirExpr::ReceiverDispatch {
                id,
                receiver,
                selector,
                args,
            } => {
                self.validate_receiver_dispatch_args(*id, args);
                self.validate_supported_surface_expr(receiver, false);
                self.validate_supported_surface_expr(selector, false);
                self.validate_args(args);
            }
            HirExpr::Spawn { id, target, delay } => {
                self.validate_spawn_target(*id, target);
                if let Some(delay) = delay {
                    self.validate_supported_surface_expr(delay, false);
                }
            }
            HirExpr::RelationAtom(atom) => {
                self.validate_relation_atom_support(atom, true, true, true)
            }
            HirExpr::FactChange { kind, atom, .. } => {
                self.validate_relation_atom_support(
                    atom,
                    matches!(kind, EffectKind::Retract),
                    true,
                    true,
                );
                if matches!(kind, EffectKind::Assert)
                    && atom.args.iter().any(|arg| {
                        matches!(arg.value, HirExpr::QueryVar { .. } | HirExpr::Hole { .. })
                    })
                {
                    self.unsupported(
                        atom.id,
                        "assert facts cannot contain query variables or holes",
                    );
                }
            }
            HirExpr::Require { condition, .. }
            | HirExpr::One {
                expr: condition, ..
            } => self.validate_supported_surface_expr(condition, false),
            HirExpr::Index {
                collection, index, ..
            } => {
                self.validate_supported_surface_expr(collection, false);
                if let Some(index) = index {
                    self.validate_supported_surface_expr(index, false);
                }
            }
            HirExpr::Field { base, .. } => self.validate_supported_surface_expr(base, false),
            HirExpr::Binding { value, .. } => {
                if let Some(value) = value {
                    let is_direct_function =
                        matches!(value.as_ref(), HirExpr::Function { name: None, .. });
                    self.validate_supported_surface_expr(value, is_direct_function);
                }
            }
            HirExpr::If {
                condition,
                then_items,
                elseif,
                else_items,
                ..
            } => {
                self.validate_supported_surface_expr(condition, false);
                self.validate_supported_surface_items(then_items);
                for (condition, items) in elseif {
                    self.validate_supported_surface_expr(condition, false);
                    self.validate_supported_surface_items(items);
                }
                self.validate_supported_surface_items(else_items);
            }
            HirExpr::Block { items, .. } => self.validate_supported_surface_items(items),
            HirExpr::For { iter, body, .. } => {
                self.validate_supported_surface_expr(iter, false);
                self.validate_supported_surface_items(body);
            }
            HirExpr::While {
                condition, body, ..
            } => {
                self.validate_supported_surface_expr(condition, false);
                self.validate_supported_surface_items(body);
            }
            HirExpr::Return { value, .. } => {
                if let Some(value) = value {
                    self.validate_supported_surface_expr(value, false);
                }
            }
            HirExpr::Raise {
                error,
                message,
                value,
                ..
            } => {
                self.validate_supported_surface_expr(error, false);
                if let Some(message) = message {
                    self.validate_supported_surface_expr(message, false);
                }
                if let Some(value) = value {
                    self.validate_supported_surface_expr(value, false);
                }
            }
            HirExpr::Try {
                body,
                catches,
                finally,
                ..
            } => {
                self.validate_supported_surface_items(body);
                for catch in catches {
                    self.validate_catch_condition(catch.id, catch.condition.as_ref());
                    self.validate_supported_surface_items(&catch.body);
                }
                self.validate_supported_surface_items(finally);
            }
            HirExpr::Function { body, .. } => match body {
                HirFunctionBody::Expr(expr) => {
                    self.validate_supported_surface_expr(expr, false);
                }
                HirFunctionBody::Block(items) => self.validate_supported_surface_items(items),
            },
            HirExpr::Recover { expr, catches, .. } => {
                self.validate_supported_surface_expr(expr, false);
                for catch in catches {
                    self.validate_catch_condition(catch.id, catch.condition.as_ref());
                    self.validate_supported_surface_expr(&catch.value, false);
                }
            }
            HirExpr::Frob { value, .. } => self.validate_supported_surface_expr(value, false),
            HirExpr::Hole { id } => {
                self.unsupported(*id, "holes are only valid as relation arguments")
            }
            HirExpr::LocalRef { .. }
            | HirExpr::ExternalRef { .. }
            | HirExpr::Identity { .. }
            | HirExpr::Symbol { .. }
            | HirExpr::Literal { .. }
            | HirExpr::Break { .. }
            | HirExpr::Continue { .. }
            | HirExpr::Error { .. } => {}
        }
    }

    fn validate_supported_surface_place(&mut self, place: &HirPlace) {
        match place {
            HirPlace::Index {
                collection, index, ..
            } => {
                self.validate_supported_surface_expr(collection, false);
                if let Some(index) = index {
                    self.validate_supported_surface_expr(index, false);
                }
            }
            HirPlace::Dot { base, .. } => self.validate_supported_surface_expr(base, false),
            HirPlace::Invalid { .. } => {}
            HirPlace::Local { .. } => {}
        }
    }

    fn validate_args(&mut self, args: &[HirArg]) {
        for arg in args {
            self.validate_supported_surface_expr(&arg.value, false);
        }
    }

    fn validate_dispatch_args(&mut self, id: NodeId, args: &[HirArg], context: &str) {
        if let Some(arg) = args.iter().find(|arg| arg.splice && arg.role.is_some()) {
            self.unsupported(
                arg.id,
                format!("{context} role values do not support splices; splice a role map"),
            );
        }
        if args.iter().any(|arg| arg.role.is_none() && !arg.splice) {
            self.unsupported(
                id,
                format!("{context} arguments must use explicit role names"),
            );
        }
    }

    fn validate_receiver_dispatch_args(&mut self, id: NodeId, args: &[HirArg]) {
        let has_named = args.iter().any(|arg| arg.role.is_some());
        let has_positional = args.iter().any(|arg| arg.role.is_none() && !arg.splice);
        if has_named && let Some(arg) = args.iter().find(|arg| arg.splice && arg.role.is_some()) {
            self.unsupported(
                arg.id,
                "receiver role dispatch values do not support splices; splice a role map",
            );
        }
        if has_named && has_positional {
            self.unsupported(
                id,
                "receiver dispatch arguments must be all positional or all role-named",
            );
        }
    }

    fn validate_spawn_target(&mut self, id: NodeId, target: &HirExpr) {
        match target {
            HirExpr::RoleDispatch { id, selector, args } => {
                let has_named = args.iter().any(|arg| arg.role.is_some());
                let has_positional = args.iter().any(|arg| arg.role.is_none() && !arg.splice);
                if has_named
                    && let Some(arg) = args.iter().find(|arg| arg.splice && arg.role.is_some())
                {
                    self.unsupported(
                        arg.id,
                        "spawn role values do not support splices; splice a role map",
                    );
                }
                if has_named && has_positional {
                    self.unsupported(
                        *id,
                        "spawn arguments must be all positional or all role-named",
                    );
                }
                self.validate_supported_surface_expr(selector, false);
                self.validate_args(args);
            }
            HirExpr::ReceiverDispatch {
                id,
                receiver,
                selector,
                args,
            } => {
                self.validate_receiver_dispatch_args(*id, args);
                self.validate_supported_surface_expr(receiver, false);
                self.validate_supported_surface_expr(selector, false);
                self.validate_args(args);
            }
            _ => self.unsupported(id, "spawn expects a role or receiver dispatch target"),
        }
    }

    fn validate_relation_atom_support(
        &mut self,
        atom: &HirRelationAtom,
        allow_query_vars: bool,
        allow_holes: bool,
        allow_splices: bool,
    ) {
        for arg in &atom.args {
            if arg.splice && !allow_splices {
                self.unsupported(arg.id, "relation argument splices are not valid here");
            }
            match &arg.value {
                HirExpr::QueryVar { id, .. } if !allow_query_vars => {
                    self.unsupported(*id, "query variables are not valid here");
                }
                HirExpr::Hole { id } if !allow_holes => {
                    self.unsupported(*id, "holes are not valid here");
                }
                HirExpr::QueryVar { .. } | HirExpr::Hole { .. } => {}
                expr => self.validate_supported_surface_expr(expr, false),
            }
        }
    }

    fn validate_catch_condition(&mut self, id: NodeId, condition: Option<&HirExpr>) {
        let Some(condition) = condition else {
            return;
        };
        let _ = id;
        self.validate_supported_surface_expr(condition, false);
    }

    fn lower_items(&mut self, items: &[Item], scope: ScopeId) -> Vec<HirItem> {
        items
            .iter()
            .map(|item| self.lower_item(item, scope))
            .collect()
    }

    fn lower_item(&mut self, item: &Item, scope: ScopeId) -> HirItem {
        match item {
            Item::Expr { id, expr } => HirItem::Expr {
                id: *id,
                expr: self.lower_expr(expr, scope),
            },
            Item::RelationRule {
                id,
                span,
                head,
                body,
            } => {
                let head = self.lower_relation_atom(head, scope).unwrap_or_else(|| {
                    self.diagnostic(
                        DiagnosticCode::InvalidRelationRule,
                        *id,
                        span.clone(),
                        "relation rule head must be a relation atom",
                    );
                    self.error_atom(*id)
                });
                let body = body
                    .iter()
                    .filter_map(|expr| {
                        let atom = self.lower_rule_atom(expr, scope);
                        if atom.is_none() {
                            self.diagnostic(
                                DiagnosticCode::InvalidRelationRule,
                                expr.id(),
                                expr.span().clone(),
                                "relation rule body entries must be relation atoms",
                            );
                        }
                        atom
                    })
                    .collect();
                HirItem::RelationRule {
                    id: *id,
                    head,
                    body,
                }
            }
            Item::Method {
                id,
                kind,
                identity,
                selector,
                clauses,
                params,
                body,
                ..
            } => {
                let method_scope = self.alloc_scope(Some(scope), Some(*id));
                self.function_stack.push(FunctionContext {
                    owner: *id,
                    scope: method_scope,
                });
                let body = self.lower_items(body, method_scope);
                self.function_stack.pop();
                HirItem::Method {
                    id: *id,
                    kind: kind.clone(),
                    identity: identity.clone(),
                    selector: selector.clone(),
                    clauses: clauses.clone(),
                    params: params.clone(),
                    scope: method_scope,
                    body,
                }
            }
        }
    }

    fn lower_rule_atom(&mut self, expr: &Expr, scope: ScopeId) -> Option<HirRelationAtom> {
        match expr {
            Expr::Unary {
                op: crate::UnaryOp::Not,
                expr,
                ..
            } => self.lower_relation_atom(expr, scope).map(|mut atom| {
                atom.negated = true;
                atom
            }),
            _ => self.lower_relation_atom(expr, scope),
        }
    }

    fn lower_expr(&mut self, expr: &Expr, scope: ScopeId) -> HirExpr {
        match expr {
            Expr::Literal { id, value, .. } => HirExpr::Literal {
                id: *id,
                value: value.clone(),
            },
            Expr::Name { id, name, .. } => match self.resolve(name, *id, scope) {
                ResolvedName::Local(binding) => HirExpr::LocalRef { id: *id, binding },
                ResolvedName::External { name, .. } => HirExpr::ExternalRef { id: *id, name },
            },
            Expr::Identity { id, name, .. } => HirExpr::Identity {
                id: *id,
                name: name.clone(),
            },
            Expr::Frob {
                id,
                delegate,
                value,
                ..
            } => HirExpr::Frob {
                id: *id,
                delegate: delegate.clone(),
                value: Box::new(self.lower_expr(value, scope)),
            },
            Expr::Symbol { id, name, .. } => HirExpr::Symbol {
                id: *id,
                name: name.clone(),
            },
            Expr::QueryVar { id, name, .. } => HirExpr::QueryVar {
                id: *id,
                name: name.clone(),
            },
            Expr::Hole { id, .. } => HirExpr::Hole { id: *id },
            Expr::List { id, items, .. } => HirExpr::List {
                id: *id,
                items: items
                    .iter()
                    .map(|item| match item {
                        CollectionItem::Expr(expr) => {
                            HirCollectionItem::Expr(self.lower_expr(expr, scope))
                        }
                        CollectionItem::Splice(expr) => {
                            HirCollectionItem::Splice(self.lower_expr(expr, scope))
                        }
                    })
                    .collect(),
            },
            Expr::Map { id, entries, .. } => HirExpr::Map {
                id: *id,
                entries: entries
                    .iter()
                    .map(|(key, value)| {
                        (self.lower_expr(key, scope), self.lower_expr(value, scope))
                    })
                    .collect(),
            },
            Expr::Unary { id, op, expr, .. } => HirExpr::Unary {
                id: *id,
                op: *op,
                expr: Box::new(self.lower_expr(expr, scope)),
            },
            Expr::Binary {
                id,
                op,
                left,
                right,
                ..
            } => HirExpr::Binary {
                id: *id,
                op: *op,
                left: Box::new(self.lower_expr(left, scope)),
                right: Box::new(self.lower_expr(right, scope)),
            },
            Expr::Assign {
                id, target, value, ..
            } => HirExpr::Assign {
                id: *id,
                target: self.lower_place(target, scope),
                value: Box::new(self.lower_expr(value, scope)),
            },
            Expr::Call {
                id, callee, args, ..
            } if self.expr_relation_name(callee).is_some() => {
                HirExpr::RelationAtom(self.lower_relation_atom_from_parts(*id, callee, args, scope))
            }
            Expr::Call {
                id, callee, args, ..
            } => HirExpr::Call {
                id: *id,
                callee: Box::new(self.lower_expr(callee, scope)),
                args: self.lower_args(args, scope),
            },
            Expr::RoleCall {
                id, selector, args, ..
            } => HirExpr::RoleDispatch {
                id: *id,
                selector: Box::new(self.lower_expr(selector, scope)),
                args: self.lower_args(args, scope),
            },
            Expr::ReceiverCall {
                id,
                receiver,
                selector,
                args,
                ..
            } => HirExpr::ReceiverDispatch {
                id: *id,
                receiver: Box::new(self.lower_expr(receiver, scope)),
                selector: Box::new(self.lower_expr(selector, scope)),
                args: self.lower_args(args, scope),
            },
            Expr::Spawn {
                id, target, delay, ..
            } => HirExpr::Spawn {
                id: *id,
                target: Box::new(self.lower_expr(target, scope)),
                delay: delay
                    .as_ref()
                    .map(|delay| Box::new(self.lower_expr(delay, scope))),
            },
            Expr::Index {
                id,
                collection,
                index,
                ..
            } => HirExpr::Index {
                id: *id,
                collection: Box::new(self.lower_expr(collection, scope)),
                index: index
                    .as_ref()
                    .map(|index| Box::new(self.lower_expr(index, scope))),
            },
            Expr::Field { id, base, name, .. } => HirExpr::Field {
                id: *id,
                base: Box::new(self.lower_expr(base, scope)),
                name: name.clone(),
            },
            Expr::Binding {
                id,
                span,
                kind,
                pattern,
                value,
            } => {
                let hir_value = value
                    .as_ref()
                    .map(|value| Box::new(self.lower_expr(value, scope)));
                let (binding, scatter) = match pattern {
                    BindingPattern::Name(name) => (
                        Some(self.declare(
                            scope,
                            name.clone(),
                            match kind {
                                BindingKind::Let => LocalKind::Let,
                                BindingKind::Const => LocalKind::Const,
                            },
                            *id,
                            span,
                        )),
                        Vec::new(),
                    ),
                    BindingPattern::Scatter(params) => (
                        None,
                        params
                            .iter()
                            .map(|param| {
                                let local_kind = match kind {
                                    BindingKind::Let => LocalKind::Let,
                                    BindingKind::Const => LocalKind::Const,
                                };
                                let binding = self.declare(
                                    scope,
                                    param.name.clone(),
                                    local_kind,
                                    param.id,
                                    span,
                                );
                                let default = param
                                    .default
                                    .as_ref()
                                    .map(|default| self.lower_expr(default, scope));
                                HirScatterBinding {
                                    id: param.id,
                                    binding,
                                    mode: param.mode.clone(),
                                    default,
                                }
                            })
                            .collect(),
                    ),
                };
                HirExpr::Binding {
                    id: *id,
                    binding,
                    scatter,
                    kind: kind.clone(),
                    value: hir_value,
                }
            }
            Expr::If {
                id,
                condition,
                then_items,
                elseif,
                else_items,
                ..
            } => HirExpr::If {
                id: *id,
                condition: Box::new(self.lower_expr(condition, scope)),
                then_items: self.lower_items_in_child_scope(then_items, scope, Some(*id)),
                elseif: elseif
                    .iter()
                    .map(|(condition, items)| {
                        (
                            self.lower_expr(condition, scope),
                            self.lower_items_in_child_scope(items, scope, Some(*id)),
                        )
                    })
                    .collect(),
                else_items: self.lower_items_in_child_scope(else_items, scope, Some(*id)),
            },
            Expr::Block { id, items, .. } => {
                let block_scope = self.alloc_scope(Some(scope), Some(*id));
                HirExpr::Block {
                    id: *id,
                    scope: block_scope,
                    items: self.lower_items(items, block_scope),
                }
            }
            Expr::For {
                id,
                span,
                key,
                value,
                iter,
                body,
            } => {
                let iter = self.lower_expr(iter, scope);
                let loop_scope = self.alloc_scope(Some(scope), Some(*id));
                let key = self.declare(loop_scope, key.clone(), LocalKind::Loop, *id, span);
                let value = value.as_ref().map(|value| {
                    self.declare(loop_scope, value.clone(), LocalKind::Loop, *id, span)
                });
                HirExpr::For {
                    id: *id,
                    scope: loop_scope,
                    key,
                    value,
                    iter: Box::new(iter),
                    body: self.lower_items(body, loop_scope),
                }
            }
            Expr::While {
                id,
                condition,
                body,
                ..
            } => HirExpr::While {
                id: *id,
                condition: Box::new(self.lower_expr(condition, scope)),
                body: self.lower_items_in_child_scope(body, scope, Some(*id)),
            },
            Expr::Return { id, value, .. } => HirExpr::Return {
                id: *id,
                value: value
                    .as_ref()
                    .map(|value| Box::new(self.lower_expr(value, scope))),
            },
            Expr::Raise {
                id,
                error,
                message,
                value,
                ..
            } => HirExpr::Raise {
                id: *id,
                error: Box::new(self.lower_expr(error, scope)),
                message: message
                    .as_ref()
                    .map(|message| Box::new(self.lower_expr(message, scope))),
                value: value
                    .as_ref()
                    .map(|value| Box::new(self.lower_expr(value, scope))),
            },
            Expr::Recover {
                id, expr, catches, ..
            } => HirExpr::Recover {
                id: *id,
                expr: Box::new(self.lower_expr(expr, scope)),
                catches: catches
                    .iter()
                    .map(|catch| self.lower_recovery(catch, scope))
                    .collect(),
            },
            Expr::One { id, expr, .. } => HirExpr::One {
                id: *id,
                expr: Box::new(self.lower_expr(expr, scope)),
            },
            Expr::Break { id, .. } => HirExpr::Break { id: *id },
            Expr::Continue { id, .. } => HirExpr::Continue { id: *id },
            Expr::Try {
                id,
                body,
                catches,
                finally,
                ..
            } => HirExpr::Try {
                id: *id,
                body: self.lower_items_in_child_scope(body, scope, Some(*id)),
                catches: catches
                    .iter()
                    .map(|catch| self.lower_catch(catch, scope))
                    .collect(),
                finally: self.lower_items_in_child_scope(finally, scope, Some(*id)),
            },
            Expr::Function {
                id,
                span,
                name,
                params,
                body,
            } => {
                let name = name
                    .as_ref()
                    .map(|name| self.declare(scope, name.clone(), LocalKind::Function, *id, span));
                let function_scope = self.alloc_scope(Some(scope), Some(*id));
                self.function_stack.push(FunctionContext {
                    owner: *id,
                    scope: function_scope,
                });
                let hir_params = self.declare_params(params, function_scope, span);
                let body = match body {
                    FunctionBody::Expr(expr) => {
                        HirFunctionBody::Expr(Box::new(self.lower_expr(expr, function_scope)))
                    }
                    FunctionBody::Block(items) => {
                        HirFunctionBody::Block(self.lower_items(items, function_scope))
                    }
                };
                self.function_stack.pop();
                let captures = self
                    .captures
                    .get(id)
                    .map(|captures| captures.iter().copied().collect())
                    .unwrap_or_default();
                HirExpr::Function {
                    id: *id,
                    name,
                    scope: function_scope,
                    params: hir_params,
                    captures,
                    body,
                }
            }
            Expr::Effect { id, kind, expr, .. } => match kind {
                EffectKind::Assert | EffectKind::Retract => {
                    if let Some(atom) = self.lower_relation_atom(expr, scope) {
                        HirExpr::FactChange {
                            id: *id,
                            kind: kind.clone(),
                            atom,
                        }
                    } else {
                        self.diagnostic(
                            DiagnosticCode::InvalidFactChange,
                            *id,
                            expr.span().clone(),
                            "assert and retract require a relation atom",
                        );
                        HirExpr::Error { id: *id }
                    }
                }
                EffectKind::Require => HirExpr::Require {
                    id: *id,
                    condition: Box::new(self.lower_expr(expr, scope)),
                },
            },
            Expr::Error { id, .. } => HirExpr::Error { id: *id },
        }
    }

    fn lower_items_in_child_scope(
        &mut self,
        items: &[Item],
        parent: ScopeId,
        owner: Option<NodeId>,
    ) -> Vec<HirItem> {
        let scope = self.alloc_scope(Some(parent), owner);
        self.lower_items(items, scope)
    }

    fn declare_params(&mut self, params: &[Param], scope: ScopeId, span: &Span) -> Vec<HirParam> {
        params
            .iter()
            .map(|param| {
                let kind = match param.mode {
                    ParamMode::Required => LocalKind::Param,
                    ParamMode::Optional => LocalKind::OptionalParam,
                    ParamMode::Rest => LocalKind::RestParam,
                };
                let binding = self.declare(scope, param.name.clone(), kind.clone(), param.id, span);
                let default = param
                    .default
                    .as_ref()
                    .map(|default| self.lower_expr(default, scope));
                HirParam {
                    id: param.id,
                    binding,
                    kind,
                    default,
                }
            })
            .collect()
    }

    fn lower_catch(&mut self, catch: &CatchClause, parent: ScopeId) -> HirCatch {
        let scope = self.alloc_scope(Some(parent), Some(catch.id));
        let binding = catch
            .name
            .as_ref()
            .map(|name| self.declare(scope, name.clone(), LocalKind::Catch, catch.id, &(0..0)));
        let condition = catch
            .condition
            .as_ref()
            .map(|condition| self.lower_expr(condition, scope));
        let body = self.lower_items(&catch.body, scope);
        HirCatch {
            id: catch.id,
            binding,
            condition,
            body,
        }
    }

    fn lower_recovery(&mut self, catch: &RecoveryClause, parent: ScopeId) -> HirRecovery {
        let scope = self.alloc_scope(Some(parent), Some(catch.id));
        let binding = catch
            .name
            .as_ref()
            .map(|name| self.declare(scope, name.clone(), LocalKind::Catch, catch.id, &(0..0)));
        let condition = catch
            .condition
            .as_ref()
            .map(|condition| self.lower_expr(condition, scope));
        let value = self.lower_expr(&catch.value, scope);
        HirRecovery {
            id: catch.id,
            binding,
            condition,
            value,
        }
    }

    fn lower_args(&mut self, args: &[Arg], scope: ScopeId) -> Vec<HirArg> {
        args.iter()
            .map(|arg| HirArg {
                id: arg.id,
                role: arg.role.clone(),
                splice: arg.splice,
                value: self.lower_expr(&arg.value, scope),
            })
            .collect()
    }

    fn lower_relation_atom(&mut self, expr: &Expr, scope: ScopeId) -> Option<HirRelationAtom> {
        match expr {
            Expr::Call {
                id, callee, args, ..
            } => Some(self.lower_relation_atom_from_parts(*id, callee, args, scope)),
            _ => None,
        }
    }

    fn lower_relation_atom_from_parts(
        &mut self,
        id: NodeId,
        callee: &Expr,
        args: &[Arg],
        scope: ScopeId,
    ) -> HirRelationAtom {
        let name = self
            .expr_relation_name(callee)
            .unwrap_or_else(|| match callee {
                Expr::Name { name, .. } => name.clone(),
                _ => "<invalid>".to_owned(),
            });
        self.references.push(Reference {
            node: callee.id(),
            name: name.clone(),
            resolution: ResolvedName::External {
                name: name.clone(),
                kind: ExternalNameKind::Relation,
            },
        });
        HirRelationAtom {
            id,
            name,
            args: self.lower_args(args, scope),
            negated: false,
        }
    }

    fn expr_relation_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Name { name, .. } if looks_like_relation_name(name) => Some(name.clone()),
            _ => None,
        }
    }

    fn lower_place(&mut self, expr: &Expr, scope: ScopeId) -> HirPlace {
        match expr {
            Expr::Name { id, name, span } => match self.resolve(name, *id, scope) {
                ResolvedName::Local(binding) => {
                    if self.bindings[binding.0 as usize].mutable {
                        HirPlace::Local { id: *id, binding }
                    } else {
                        self.diagnostic(
                            DiagnosticCode::AssignToConst,
                            *id,
                            span.clone(),
                            format!("cannot assign to immutable binding `{name}`"),
                        );
                        HirPlace::Invalid {
                            id: *id,
                            span: span.clone(),
                            resolution: Some(ResolvedName::Local(binding)),
                        }
                    }
                }
                resolution => {
                    self.diagnostic(
                        DiagnosticCode::InvalidAssignmentTarget,
                        *id,
                        span.clone(),
                        format!("`{name}` is not an assignable local binding"),
                    );
                    HirPlace::Invalid {
                        id: *id,
                        span: span.clone(),
                        resolution: Some(resolution),
                    }
                }
            },
            Expr::Index {
                id,
                collection,
                index,
                ..
            } => HirPlace::Index {
                id: *id,
                collection: Box::new(self.lower_expr(collection, scope)),
                index: index
                    .as_ref()
                    .map(|index| Box::new(self.lower_expr(index, scope))),
            },
            Expr::Field { id, base, name, .. } => HirPlace::Dot {
                id: *id,
                base: Box::new(self.lower_expr(base, scope)),
                name: name.clone(),
            },
            _ => {
                self.diagnostic(
                    DiagnosticCode::InvalidAssignmentTarget,
                    expr.id(),
                    expr.span().clone(),
                    "left side of assignment is not an assignable place",
                );
                HirPlace::Invalid {
                    id: expr.id(),
                    span: expr.span().clone(),
                    resolution: None,
                }
            }
        }
    }

    fn error_atom(&self, id: NodeId) -> HirRelationAtom {
        HirRelationAtom {
            id,
            name: "<error>".to_owned(),
            args: Vec::new(),
            negated: false,
        }
    }

    fn diagnostic(
        &mut self,
        code: DiagnosticCode,
        node: NodeId,
        span: Span,
        message: impl Into<String>,
    ) {
        self.diagnostics.push(Diagnostic {
            code,
            node,
            span,
            message: message.into(),
        });
    }

    fn unsupported(&mut self, node: NodeId, message: impl Into<String>) {
        let span = self.spans.get(&node).cloned().unwrap_or(0..0);
        self.diagnostic(DiagnosticCode::UnsupportedSyntax, node, span, message);
    }
}

fn looks_like_relation_name(name: &str) -> bool {
    name.chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_uppercase())
}

fn collect_item_spans(items: &[Item], spans: &mut HashMap<NodeId, Span>) {
    for item in items {
        match item {
            Item::Expr { id, expr } => {
                spans.insert(*id, expr.span().clone());
                collect_expr_span(expr, spans);
            }
            Item::RelationRule {
                id,
                span,
                head,
                body,
            } => {
                spans.insert(*id, span.clone());
                collect_expr_span(head, spans);
                collect_expr_spans(body, spans);
            }
            Item::Method { id, span, body, .. } => {
                spans.insert(*id, span.clone());
                collect_item_spans(body, spans);
            }
        }
    }
}

fn collect_expr_spans(exprs: &[Expr], spans: &mut HashMap<NodeId, Span>) {
    for expr in exprs {
        collect_expr_span(expr, spans);
    }
}

fn collect_expr_span(expr: &Expr, spans: &mut HashMap<NodeId, Span>) {
    spans.insert(expr.id(), expr.span().clone());
    match expr {
        Expr::Literal { .. }
        | Expr::Name { .. }
        | Expr::QueryVar { .. }
        | Expr::Identity { .. }
        | Expr::Symbol { .. }
        | Expr::Hole { .. }
        | Expr::Break { .. }
        | Expr::Continue { .. }
        | Expr::Error { .. } => {}
        Expr::Frob { value, .. } => collect_expr_span(value, spans),
        Expr::List { items, .. } => {
            for item in items {
                match item {
                    CollectionItem::Expr(expr) | CollectionItem::Splice(expr) => {
                        collect_expr_span(expr, spans);
                    }
                }
            }
        }
        Expr::Map { entries, .. } => {
            for (key, value) in entries {
                collect_expr_span(key, spans);
                collect_expr_span(value, spans);
            }
        }
        Expr::Unary { expr, .. }
        | Expr::Return {
            value: Some(expr), ..
        }
        | Expr::Effect { expr, .. } => collect_expr_span(expr, spans),
        Expr::Return { value: None, .. } => {}
        Expr::Binary { left, right, .. } => {
            collect_expr_span(left, spans);
            collect_expr_span(right, spans);
        }
        Expr::Assign { target, value, .. } => {
            collect_expr_span(target, spans);
            collect_expr_span(value, spans);
        }
        Expr::Call { callee, args, .. } => {
            collect_expr_span(callee, spans);
            collect_arg_spans(args, spans);
        }
        Expr::RoleCall { selector, args, .. } => {
            collect_expr_span(selector, spans);
            collect_arg_spans(args, spans);
        }
        Expr::ReceiverCall {
            receiver,
            selector,
            args,
            ..
        } => {
            collect_expr_span(receiver, spans);
            collect_expr_span(selector, spans);
            collect_arg_spans(args, spans);
        }
        Expr::Spawn { target, delay, .. } => {
            collect_expr_span(target, spans);
            if let Some(delay) = delay {
                collect_expr_span(delay, spans);
            }
        }
        Expr::Index {
            collection, index, ..
        } => {
            collect_expr_span(collection, spans);
            if let Some(index) = index {
                collect_expr_span(index, spans);
            }
        }
        Expr::Field { base, .. } => collect_expr_span(base, spans),
        Expr::Binding { pattern, value, .. } => {
            if let BindingPattern::Scatter(params) = pattern {
                collect_param_spans(params, spans);
            }
            if let Some(value) = value {
                collect_expr_span(value, spans);
            }
        }
        Expr::If {
            condition,
            then_items,
            elseif,
            else_items,
            ..
        } => {
            collect_expr_span(condition, spans);
            collect_item_spans(then_items, spans);
            for (condition, items) in elseif {
                collect_expr_span(condition, spans);
                collect_item_spans(items, spans);
            }
            collect_item_spans(else_items, spans);
        }
        Expr::Block { items, .. } => collect_item_spans(items, spans),
        Expr::For {
            iter,
            body,
            span,
            key: _,
            value: _,
            ..
        } => {
            collect_expr_span(iter, spans);
            collect_item_spans(body, spans);
            // Loop variables do not have their own source-span field yet.
            spans.entry(expr.id()).or_insert_with(|| span.clone());
        }
        Expr::While {
            condition, body, ..
        } => {
            collect_expr_span(condition, spans);
            collect_item_spans(body, spans);
        }
        Expr::Raise {
            error,
            message,
            value,
            ..
        } => {
            collect_expr_span(error, spans);
            if let Some(message) = message {
                collect_expr_span(message, spans);
            }
            if let Some(value) = value {
                collect_expr_span(value, spans);
            }
        }
        Expr::Recover { expr, catches, .. } => {
            collect_expr_span(expr, spans);
            for catch in catches {
                collect_recovery_span(catch, spans);
            }
        }
        Expr::One { expr, .. } => collect_expr_span(expr, spans),
        Expr::Try {
            body,
            catches,
            finally,
            ..
        } => {
            collect_item_spans(body, spans);
            for catch in catches {
                collect_catch_span(catch, spans);
            }
            collect_item_spans(finally, spans);
        }
        Expr::Function { params, body, .. } => {
            collect_param_spans(params, spans);
            match body {
                FunctionBody::Expr(expr) => collect_expr_span(expr, spans),
                FunctionBody::Block(items) => collect_item_spans(items, spans),
            }
        }
    }
}

fn collect_arg_spans(args: &[Arg], spans: &mut HashMap<NodeId, Span>) {
    for arg in args {
        spans.insert(arg.id, arg.value.span().clone());
        collect_expr_span(&arg.value, spans);
    }
}

fn collect_param_spans(params: &[Param], spans: &mut HashMap<NodeId, Span>) {
    for param in params {
        let span = param
            .default
            .as_ref()
            .map_or_else(|| 0..0, |default| default.span().clone());
        spans.insert(param.id, span);
        if let Some(default) = &param.default {
            collect_expr_span(default, spans);
        }
    }
}

fn collect_catch_span(catch: &CatchClause, spans: &mut HashMap<NodeId, Span>) {
    let span = catch.condition.as_ref().map_or_else(
        || first_item_span(&catch.body).unwrap_or(0..0),
        |condition| condition.span().clone(),
    );
    spans.insert(catch.id, span);
    if let Some(condition) = &catch.condition {
        collect_expr_span(condition, spans);
    }
    collect_item_spans(&catch.body, spans);
}

fn collect_recovery_span(catch: &RecoveryClause, spans: &mut HashMap<NodeId, Span>) {
    spans.insert(catch.id, catch.value.span().clone());
    if let Some(condition) = &catch.condition {
        collect_expr_span(condition, spans);
    }
    collect_expr_span(&catch.value, spans);
}

fn first_item_span(items: &[Item]) -> Option<Span> {
    items.first().map(|item| match item {
        Item::Expr { expr, .. } => expr.span().clone(),
        Item::RelationRule { span, .. } | Item::Method { span, .. } => span.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(source: &str) -> SemanticProgram {
        let program = parse_semantic(source);
        assert_eq!(program.parse_errors, vec![]);
        program
    }

    #[test]
    fn preserves_span_map_for_stable_node_ids() {
        let source = "let x = 1\n\
                      CanMove(#alice, #coin)\n\
                      {y} => x + y";
        let ast = parse_ast(source);
        assert_eq!(ast.errors, vec![]);
        let program = analyze_ast(&ast);
        for id in 0..ast.node_count {
            assert!(
                program.span(NodeId(id)).is_some(),
                "missing span for node {id}"
            );
        }
    }

    #[test]
    fn resolves_nested_scopes_and_captures() {
        let program = parse_ok(
            "let x = 1\n\
             let f = {y} => x + y\n\
             f(2)",
        );
        assert_eq!(program.diagnostics, vec![]);
        let x = program
            .bindings
            .iter()
            .find(|binding| binding.name == "x")
            .unwrap();
        let function = program
            .hir
            .items
            .iter()
            .find_map(|item| match item {
                HirItem::Expr {
                    expr:
                        HirExpr::Binding {
                            value: Some(value), ..
                        },
                    ..
                } => match &**value {
                    HirExpr::Function { id, .. } => Some(*id),
                    _ => None,
                },
                _ => None,
            })
            .unwrap();
        assert_eq!(program.captures.get(&function), Some(&vec![x.id]));
    }

    #[test]
    fn distinguishes_locals_from_external_relations_and_runtime_names() {
        let program = parse_ok(
            "let actor = #alice\n\
             CanMove(actor, #coin)\n\
             format_name(actor)",
        );
        assert_eq!(program.diagnostics, vec![]);
        assert!(program.references.iter().any(|reference| matches!(
            &reference.resolution,
            ResolvedName::External { name, kind: ExternalNameKind::Relation }
                if name == "CanMove"
        )));
        assert!(program.references.iter().any(|reference| matches!(
            &reference.resolution,
            ResolvedName::External { name, kind: ExternalNameKind::Runtime }
                if name == "format_name"
        )));
        assert!(
            program
                .references
                .iter()
                .any(|reference| matches!(reference.resolution, ResolvedName::Local(_)))
        );
    }

    #[test]
    fn validates_assignment_targets_and_const_assignments() {
        let program = parse_ok(
            "const limit = 3\n\
             limit = 4\n\
             1 = limit\n\
             #lamp.name = \"gold\"",
        );
        assert_eq!(program.diagnostics.len(), 2);
        assert!(
            program
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiagnosticCode::AssignToConst)
        );
        assert!(
            program
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiagnosticCode::InvalidAssignmentTarget)
        );
        assert!(matches!(
            &program.hir.items[3],
            HirItem::Expr { expr: HirExpr::Assign { target: HirPlace::Dot { name, .. }, .. }, .. }
                if name == "name"
        ));
    }

    #[test]
    fn reports_duplicate_bindings_and_invalid_fact_changes() {
        let program = parse_ok(
            "let x = 1\n\
             let x = 2\n\
             assert x",
        );
        assert!(
            program
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiagnosticCode::DuplicateBinding)
        );
        assert!(
            program
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiagnosticCode::InvalidFactChange)
        );
    }

    #[test]
    fn normalizes_dispatch_relation_atoms_and_fact_changes() {
        let program = parse_ok(
            ":move(actor: #alice, item: #coin)\n\
             #box:put(item: #coin, prep: :into)\n\
             CanMove(#alice, #coin)\n\
             assert LocatedIn(#coin, #box)\n\
             retract LocatedIn(#coin, _)",
        );
        assert_eq!(program.diagnostics, vec![]);
        assert!(matches!(
            &program.hir.items[0],
            HirItem::Expr {
                expr: HirExpr::RoleDispatch { .. },
                ..
            }
        ));
        assert!(matches!(
            &program.hir.items[1],
            HirItem::Expr {
                expr: HirExpr::ReceiverDispatch { .. },
                ..
            }
        ));
        assert!(matches!(
            &program.hir.items[2],
            HirItem::Expr { expr: HirExpr::RelationAtom(atom), .. } if atom.name == "CanMove"
        ));
        assert!(matches!(
            &program.hir.items[3],
            HirItem::Expr { expr: HirExpr::FactChange { kind: EffectKind::Assert, atom, .. }, .. }
                if atom.name == "LocatedIn"
        ));
        assert!(matches!(
            &program.hir.items[4],
            HirItem::Expr { expr: HirExpr::FactChange { kind: EffectKind::Retract, atom, .. }, .. }
                if atom.name == "LocatedIn"
        ));
    }

    #[test]
    fn keeps_blocks_loops_and_catches_scoped() {
        let program = parse_ok(
            "let items = [1]\n\
             for key, value in items\n\
               let seen = value\n\
             end\n\
             try\n\
               risky()\n\
             catch err\n\
               err\n\
             end",
        );
        assert_eq!(program.diagnostics, vec![]);
        assert!(
            program
                .bindings
                .iter()
                .any(|binding| binding.name == "key" && binding.kind == LocalKind::Loop)
        );
        assert!(
            program
                .bindings
                .iter()
                .any(|binding| binding.name == "value" && binding.kind == LocalKind::Loop)
        );
        assert!(
            program
                .bindings
                .iter()
                .any(|binding| binding.name == "err" && binding.kind == LocalKind::Catch)
        );
        assert!(program.scopes.len() >= 4);
    }

    #[test]
    fn reports_backend_limited_surface_forms() {
        let program = parse_ok(
            "#box:put(#alice)\n\
             try\n\
               raise E_FAIL\n\
             catch err if err.code == E_FAIL\n\
               err\n\
             end\n\
             Visible(@args) :- Source(?x)",
        );

        let messages = program
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.code == DiagnosticCode::UnsupportedSyntax)
            .map(|diagnostic| diagnostic.message.as_str())
            .collect::<Vec<_>>();
        assert!(messages.contains(&"relation argument splices are not valid here"));
    }

    #[test]
    fn allows_receiver_dispatch_positional_or_named_arguments() {
        let program = parse_ok(
            "#box:put(#alice, #coin)\n\
             #box:put(actor: #alice, item: #coin)",
        );

        assert_eq!(program.diagnostics, vec![]);
    }

    #[test]
    fn rejects_mixed_receiver_dispatch_argument_styles() {
        let program = parse_ok("#box:put(#alice, item: #coin)");

        assert!(program.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == DiagnosticCode::UnsupportedSyntax
                && diagnostic.message
                    == "receiver dispatch arguments must be all positional or all role-named"
        }));
    }

    #[test]
    fn rejects_query_variables_in_non_relation_arguments() {
        let program = parse_ok("foo(?value)");

        assert!(program.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == DiagnosticCode::UnsupportedSyntax
                && diagnostic.message == "query variables are only valid as relation arguments"
        }));
    }
}
