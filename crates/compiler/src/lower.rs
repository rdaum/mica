use crate::{
    Arg, Ast, BinaryOp, BindingKind, BindingPattern, CatchClause, CollectionItem, CstElement,
    CstNode, CstToken, EffectKind, Expr, FunctionBody, Item, Literal, MethodKind, ObjectClause,
    Param, ParamMode, ParseError, SyntaxKind, UnaryOp, parse,
};

pub fn parse_ast(source: &str) -> Ast {
    let parse = parse(source);
    let mut lower = Lower::new(source, parse.errors);
    let items = lower.lower_program(&parse.root);
    Ast {
        items,
        errors: lower.errors,
    }
}

struct Lower<'a> {
    source: &'a str,
    errors: Vec<ParseError>,
}

impl<'a> Lower<'a> {
    fn new(source: &'a str, errors: Vec<ParseError>) -> Self {
        Self { source, errors }
    }

    fn lower_program(&mut self, root: &CstNode) -> Vec<Item> {
        self.node_children(root)
            .find(|node| node.kind == SyntaxKind::ItemList)
            .map(|node| self.lower_items(node))
            .unwrap_or_default()
    }

    fn lower_items(&mut self, node: &CstNode) -> Vec<Item> {
        self.node_children(node)
            .filter_map(|child| self.lower_item(child))
            .collect()
    }

    fn lower_item(&mut self, node: &CstNode) -> Option<Item> {
        match node.kind {
            SyntaxKind::ExprStmt => self
                .node_children(node)
                .next()
                .map(|child| match child.kind {
                    SyntaxKind::RelationRule => self.lower_relation_rule(child),
                    _ => Item::Expr(self.lower_expr(child)),
                }),
            SyntaxKind::ObjectItem => Some(self.lower_object_item(node)),
            SyntaxKind::MethodItem => Some(self.lower_method_item(node, MethodKind::Method)),
            SyntaxKind::VerbItem => Some(self.lower_method_item(node, MethodKind::Verb)),
            _ => {
                self.error(node, "expected item");
                None
            }
        }
    }

    fn lower_relation_rule(&mut self, node: &CstNode) -> Item {
        let exprs = self
            .node_children(node)
            .map(|child| self.lower_expr(child))
            .collect::<Vec<_>>();
        let mut iter = exprs.into_iter();
        let head = iter.next().unwrap_or_else(|| self.error_expr(node));
        Item::RelationRule {
            span: node.span.clone(),
            head,
            body: iter.collect(),
        }
    }

    fn lower_object_item(&mut self, node: &CstNode) -> Item {
        let header = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::ObjectHeader);
        let (identity, extends) = header
            .map(|header| self.lower_object_header(header))
            .unwrap_or((None, None));
        let clauses = self
            .node_children(node)
            .filter(|child| child.kind == SyntaxKind::ObjectClause)
            .map(|child| ObjectClause {
                span: child.span.clone(),
                exprs: self
                    .node_children(child)
                    .map(|expr| self.lower_expr(expr))
                    .collect(),
            })
            .collect();
        Item::Object {
            span: node.span.clone(),
            identity,
            extends,
            clauses,
        }
    }

    fn lower_object_header(&self, node: &CstNode) -> (Option<String>, Option<String>) {
        let tokens = self.token_children(node).collect::<Vec<_>>();
        let identity = identity_after_dollar(self.source, &tokens, 0);
        let extends = tokens
            .iter()
            .position(|token| token.kind == SyntaxKind::ExtendsKw)
            .and_then(|idx| identity_after_dollar(self.source, &tokens, idx + 1));
        (identity, extends)
    }

    fn lower_method_item(&mut self, node: &CstNode, kind: MethodKind) -> Item {
        let header = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::MethodHeader);
        let (identity, selector) = header
            .map(|header| self.lower_method_header(header))
            .unwrap_or((None, None));
        let clauses = self
            .node_children(node)
            .filter(|child| child.kind == SyntaxKind::MethodClause)
            .map(|child| self.text(child.span.clone()).trim().to_owned())
            .filter(|text| !text.is_empty())
            .collect();
        let body = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::Block)
            .map(|body| self.lower_items(body))
            .unwrap_or_default();
        Item::Method {
            span: node.span.clone(),
            kind,
            identity,
            selector,
            clauses,
            body,
        }
    }

    fn lower_method_header(&self, node: &CstNode) -> (Option<String>, Option<String>) {
        let tokens = self.token_children(node).collect::<Vec<_>>();
        let identity = identity_after_dollar(self.source, &tokens, 0);
        let selector = tokens
            .iter()
            .position(|token| token.kind == SyntaxKind::Colon)
            .and_then(|idx| tokens.get(idx + 1))
            .filter(|token| token.kind == SyntaxKind::Ident)
            .map(|token| self.text(token.span.clone()).to_owned());
        (identity, selector)
    }

    fn lower_expr(&mut self, node: &CstNode) -> Expr {
        match node.kind {
            SyntaxKind::LiteralExpr => self.lower_literal(node),
            SyntaxKind::NameExpr => self.lower_name(node),
            SyntaxKind::IdentityExpr => self.lower_identity(node),
            SyntaxKind::SymbolExpr => self.lower_symbol(node),
            SyntaxKind::HoleExpr => Expr::Hole {
                span: node.span.clone(),
            },
            SyntaxKind::ListExpr => self.lower_list(node),
            SyntaxKind::MapExpr => self.lower_map(node),
            SyntaxKind::UnaryExpr => self.lower_unary(node),
            SyntaxKind::BinaryExpr => self.lower_binary(node),
            SyntaxKind::AssignExpr => self.lower_assign(node),
            SyntaxKind::CallExpr => self.lower_call(node),
            SyntaxKind::RoleCallExpr => self.lower_role_call(node),
            SyntaxKind::ReceiverCallExpr => self.lower_receiver_call(node),
            SyntaxKind::IndexExpr => self.lower_index(node),
            SyntaxKind::FieldExpr => self.lower_field(node),
            SyntaxKind::LetExpr => self.lower_binding(node, BindingKind::Let),
            SyntaxKind::ConstExpr => self.lower_binding(node, BindingKind::Const),
            SyntaxKind::IfExpr => self.lower_if(node),
            SyntaxKind::BeginExpr => Expr::Block {
                span: node.span.clone(),
                items: self
                    .node_children(node)
                    .find(|child| child.kind == SyntaxKind::Block)
                    .map(|block| self.lower_items(block))
                    .unwrap_or_default(),
            },
            SyntaxKind::ForExpr => self.lower_for(node),
            SyntaxKind::WhileExpr => self.lower_while(node),
            SyntaxKind::ReturnExpr => self.lower_return(node),
            SyntaxKind::BreakExpr => Expr::Break {
                span: node.span.clone(),
            },
            SyntaxKind::ContinueExpr => Expr::Continue {
                span: node.span.clone(),
            },
            SyntaxKind::TryExpr => self.lower_try(node),
            SyntaxKind::FnExpr => self.lower_fn(node),
            SyntaxKind::LambdaExpr => self.lower_lambda(node),
            SyntaxKind::AssertExpr => self.lower_effect(node, EffectKind::Assert),
            SyntaxKind::RetractExpr => self.lower_effect(node, EffectKind::Retract),
            SyntaxKind::RequireExpr => self.lower_effect(node, EffectKind::Require),
            SyntaxKind::GroupExpr => self
                .node_children(node)
                .next()
                .map(|child| self.lower_expr(child))
                .unwrap_or_else(|| self.error_expr(node)),
            SyntaxKind::AtomExpr => self.error_expr(node),
            _ => {
                self.error(node, "expected expression node");
                self.error_expr(node)
            }
        }
    }

    fn lower_literal(&self, node: &CstNode) -> Expr {
        let Some(token) = self.token_children(node).next() else {
            return Expr::Error {
                span: node.span.clone(),
            };
        };
        let value = match token.kind {
            SyntaxKind::Int => Literal::Int(self.text(token.span.clone()).to_owned()),
            SyntaxKind::Float => Literal::Float(self.text(token.span.clone()).to_owned()),
            SyntaxKind::String => Literal::String(unquote(self.text(token.span.clone()))),
            SyntaxKind::TrueKw => Literal::Bool(true),
            SyntaxKind::FalseKw => Literal::Bool(false),
            SyntaxKind::NothingKw => Literal::Nothing,
            _ => Literal::Nothing,
        };
        Expr::Literal {
            span: node.span.clone(),
            value,
        }
    }

    fn lower_name(&self, node: &CstNode) -> Expr {
        Expr::Name {
            span: node.span.clone(),
            name: self.first_text(node, SyntaxKind::Ident).unwrap_or_default(),
        }
    }

    fn lower_identity(&mut self, node: &CstNode) -> Expr {
        let tokens = self.token_children(node).collect::<Vec<_>>();
        let name = identity_after_dollar(self.source, &tokens, 0).unwrap_or_else(|| {
            self.error(node, "expected identity name");
            String::new()
        });
        Expr::Identity {
            span: node.span.clone(),
            name,
        }
    }

    fn lower_symbol(&mut self, node: &CstNode) -> Expr {
        if let Some(name) = self.first_text(node, SyntaxKind::Ident) {
            Expr::Symbol {
                span: node.span.clone(),
                name,
            }
        } else {
            self.node_children(node)
                .next()
                .map(|child| self.lower_expr(child))
                .unwrap_or_else(|| self.error_expr(node))
        }
    }

    fn lower_list(&mut self, node: &CstNode) -> Expr {
        let items = self
            .node_children(node)
            .filter(|child| child.kind == SyntaxKind::ListItem)
            .filter_map(|item| {
                let expr = self.node_children(item).next()?;
                let expr = self.lower_expr(expr);
                if self
                    .token_children(item)
                    .any(|token| token.kind == SyntaxKind::At)
                {
                    Some(CollectionItem::Splice(expr))
                } else {
                    Some(CollectionItem::Expr(expr))
                }
            })
            .collect();
        Expr::List {
            span: node.span.clone(),
            items,
        }
    }

    fn lower_map(&mut self, node: &CstNode) -> Expr {
        let entries = self
            .node_children(node)
            .filter(|child| child.kind == SyntaxKind::MapEntry)
            .filter_map(|entry| {
                let mut exprs = self.node_children(entry).map(|expr| self.lower_expr(expr));
                Some((exprs.next()?, exprs.next()?))
            })
            .collect();
        Expr::Map {
            span: node.span.clone(),
            entries,
        }
    }

    fn lower_unary(&mut self, node: &CstNode) -> Expr {
        let op = self
            .token_children(node)
            .find_map(|token| match token.kind {
                SyntaxKind::Minus => Some(UnaryOp::Neg),
                SyntaxKind::Bang => Some(UnaryOp::Not),
                _ => None,
            })
            .unwrap_or(UnaryOp::Not);
        let expr = self
            .node_children(node)
            .next()
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        Expr::Unary {
            span: node.span.clone(),
            op,
            expr: Box::new(expr),
        }
    }

    fn lower_binary(&mut self, node: &CstNode) -> Expr {
        let op = self
            .token_children(node)
            .find_map(binary_op)
            .unwrap_or(BinaryOp::Add);
        let exprs = self.node_children(node).collect::<Vec<_>>();
        let left = exprs
            .first()
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        let right = exprs
            .get(1)
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        Expr::Binary {
            span: node.span.clone(),
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn lower_assign(&mut self, node: &CstNode) -> Expr {
        let exprs = self.node_children(node).collect::<Vec<_>>();
        let target = exprs
            .first()
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        let value = exprs
            .get(1)
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        Expr::Assign {
            span: node.span.clone(),
            target: Box::new(target),
            value: Box::new(value),
        }
    }

    fn lower_call(&mut self, node: &CstNode) -> Expr {
        let mut children = self.node_children(node);
        let callee = children
            .next()
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        let args = children
            .find(|child| child.kind == SyntaxKind::ArgList)
            .map(|args| self.lower_args(args))
            .unwrap_or_default();
        Expr::Call {
            span: node.span.clone(),
            callee: Box::new(callee),
            args,
        }
    }

    fn lower_role_call(&mut self, node: &CstNode) -> Expr {
        let selector = self.lower_selector_after_colon(node);
        let args = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::ArgList)
            .map(|args| self.lower_args(args))
            .unwrap_or_default();
        Expr::RoleCall {
            span: node.span.clone(),
            selector: Box::new(selector),
            args,
        }
    }

    fn lower_receiver_call(&mut self, node: &CstNode) -> Expr {
        let mut exprs = self.node_children(node);
        let receiver = exprs
            .next()
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        let selector = self.lower_selector_after_colon(node);
        let args = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::ArgList)
            .map(|args| self.lower_args(args))
            .unwrap_or_default();
        Expr::ReceiverCall {
            span: node.span.clone(),
            receiver: Box::new(receiver),
            selector: Box::new(selector),
            args,
        }
    }

    fn lower_selector_after_colon(&mut self, node: &CstNode) -> Expr {
        if let Some(group) = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::GroupExpr)
        {
            return self.lower_expr(group);
        }
        let tokens = self.token_children(node).collect::<Vec<_>>();
        let name = tokens
            .iter()
            .position(|token| token.kind == SyntaxKind::Colon)
            .and_then(|idx| tokens.get(idx + 1))
            .filter(|token| token.kind == SyntaxKind::Ident)
            .map(|token| self.text(token.span.clone()).to_owned())
            .unwrap_or_default();
        Expr::Symbol {
            span: node.span.clone(),
            name,
        }
    }

    fn lower_index(&mut self, node: &CstNode) -> Expr {
        let exprs = self.node_children(node).collect::<Vec<_>>();
        let collection = exprs
            .first()
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        let index = exprs.get(1).map(|child| Box::new(self.lower_expr(child)));
        Expr::Index {
            span: node.span.clone(),
            collection: Box::new(collection),
            index,
        }
    }

    fn lower_field(&mut self, node: &CstNode) -> Expr {
        let base = self
            .node_children(node)
            .next()
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        Expr::Field {
            span: node.span.clone(),
            base: Box::new(base),
            name: self.last_text(node, SyntaxKind::Ident).unwrap_or_default(),
        }
    }

    fn lower_binding(&mut self, node: &CstNode, kind: BindingKind) -> Expr {
        let pattern = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::ParamList)
            .map(|child| BindingPattern::Scatter(self.lower_params(child)))
            .unwrap_or_else(|| {
                BindingPattern::Name(self.first_text(node, SyntaxKind::Ident).unwrap_or_default())
            });
        let value = self
            .node_children(node)
            .find(|child| child.kind != SyntaxKind::ParamList)
            .map(|child| Box::new(self.lower_expr(child)));
        Expr::Binding {
            span: node.span.clone(),
            kind,
            pattern,
            value,
        }
    }

    fn lower_if(&mut self, node: &CstNode) -> Expr {
        let mut exprs = self
            .node_children(node)
            .filter(|child| is_expr_node(child.kind));
        let condition = exprs
            .next()
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        let then_items = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::Block)
            .map(|block| self.lower_items(block))
            .unwrap_or_default();
        let elseif = self
            .node_children(node)
            .filter(|child| child.kind == SyntaxKind::ElseIfClause)
            .map(|clause| {
                let condition = self
                    .node_children(clause)
                    .find(|child| is_expr_node(child.kind))
                    .map(|child| self.lower_expr(child))
                    .unwrap_or_else(|| self.error_expr(clause));
                let body = self
                    .node_children(clause)
                    .find(|child| child.kind == SyntaxKind::Block)
                    .map(|block| self.lower_items(block))
                    .unwrap_or_default();
                (condition, body)
            })
            .collect();
        let else_items = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::ElseClause)
            .and_then(|clause| {
                self.node_children(clause)
                    .find(|child| child.kind == SyntaxKind::Block)
            })
            .map(|block| self.lower_items(block))
            .unwrap_or_default();
        Expr::If {
            span: node.span.clone(),
            condition: Box::new(condition),
            then_items,
            elseif,
            else_items,
        }
    }

    fn lower_for(&mut self, node: &CstNode) -> Expr {
        let names = self
            .token_children(node)
            .filter(|token| token.kind == SyntaxKind::Ident)
            .map(|token| self.text(token.span.clone()).to_owned())
            .collect::<Vec<_>>();
        let iter = self
            .node_children(node)
            .find(|child| is_expr_node(child.kind))
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        let body = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::Block)
            .map(|block| self.lower_items(block))
            .unwrap_or_default();
        Expr::For {
            span: node.span.clone(),
            key: names.first().cloned().unwrap_or_default(),
            value: names.get(1).cloned(),
            iter: Box::new(iter),
            body,
        }
    }

    fn lower_while(&mut self, node: &CstNode) -> Expr {
        let condition = self
            .node_children(node)
            .find(|child| is_expr_node(child.kind))
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        let body = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::Block)
            .map(|block| self.lower_items(block))
            .unwrap_or_default();
        Expr::While {
            span: node.span.clone(),
            condition: Box::new(condition),
            body,
        }
    }

    fn lower_return(&mut self, node: &CstNode) -> Expr {
        let value = self
            .node_children(node)
            .find(|child| is_expr_node(child.kind))
            .map(|child| Box::new(self.lower_expr(child)));
        Expr::Return {
            span: node.span.clone(),
            value,
        }
    }

    fn lower_try(&mut self, node: &CstNode) -> Expr {
        let body = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::Block)
            .map(|block| self.lower_items(block))
            .unwrap_or_default();
        let catches = self
            .node_children(node)
            .filter(|child| child.kind == SyntaxKind::CatchClause)
            .map(|catch| self.lower_catch(catch))
            .collect();
        let finally = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::FinallyClause)
            .and_then(|finally| {
                self.node_children(finally)
                    .find(|child| child.kind == SyntaxKind::Block)
            })
            .map(|block| self.lower_items(block))
            .unwrap_or_default();
        Expr::Try {
            span: node.span.clone(),
            body,
            catches,
            finally,
        }
    }

    fn lower_catch(&mut self, node: &CstNode) -> CatchClause {
        let name = self.first_text(node, SyntaxKind::Ident);
        let condition = self
            .node_children(node)
            .find(|child| is_expr_node(child.kind))
            .map(|child| self.lower_expr(child));
        let body = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::Block)
            .map(|block| self.lower_items(block))
            .unwrap_or_default();
        CatchClause {
            name,
            condition,
            body,
        }
    }

    fn lower_fn(&mut self, node: &CstNode) -> Expr {
        let name = if self
            .token_children(node)
            .any(|token| token.kind == SyntaxKind::FnKw)
        {
            self.first_text(node, SyntaxKind::Ident)
        } else {
            None
        };
        let params = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::ParamList)
            .map(|params| self.lower_params(params))
            .unwrap_or_default();
        let body = if let Some(block) = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::Block)
        {
            FunctionBody::Block(self.lower_items(block))
        } else {
            let expr = self
                .node_children(node)
                .filter(|child| child.kind != SyntaxKind::ParamList)
                .last()
                .map(|child| self.lower_expr(child))
                .unwrap_or_else(|| self.error_expr(node));
            FunctionBody::Expr(Box::new(expr))
        };
        Expr::Function {
            span: node.span.clone(),
            name,
            params,
            body,
        }
    }

    fn lower_lambda(&mut self, node: &CstNode) -> Expr {
        let params = self
            .node_children(node)
            .find(|child| child.kind == SyntaxKind::ParamList)
            .map(|params| self.lower_params(params))
            .unwrap_or_default();
        let body = self
            .node_children(node)
            .find(|child| child.kind != SyntaxKind::ParamList)
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        Expr::Function {
            span: node.span.clone(),
            name: None,
            params,
            body: FunctionBody::Expr(Box::new(body)),
        }
    }

    fn lower_effect(&mut self, node: &CstNode, kind: EffectKind) -> Expr {
        let expr = self
            .node_children(node)
            .find(|child| is_expr_node(child.kind))
            .map(|child| self.lower_expr(child))
            .unwrap_or_else(|| self.error_expr(node));
        Expr::Effect {
            span: node.span.clone(),
            kind,
            expr: Box::new(expr),
        }
    }

    fn lower_args(&mut self, node: &CstNode) -> Vec<Arg> {
        self.node_children(node)
            .filter(|child| child.kind == SyntaxKind::Arg)
            .map(|arg| {
                let role = self
                    .token_children(arg)
                    .find(|token| token.kind == SyntaxKind::Ident)
                    .filter(|_| {
                        self.token_children(arg)
                            .any(|token| token.kind == SyntaxKind::Colon)
                    })
                    .map(|token| self.text(token.span.clone()).to_owned());
                let value = self
                    .node_children(arg)
                    .next()
                    .map(|expr| self.lower_expr(expr))
                    .unwrap_or_else(|| self.error_expr(arg));
                Arg { role, value }
            })
            .collect()
    }

    fn lower_params(&mut self, node: &CstNode) -> Vec<Param> {
        let params = self
            .node_children(node)
            .filter(|child| child.kind == SyntaxKind::Param)
            .map(|param| self.lower_param(param))
            .collect::<Vec<_>>();
        if !params.is_empty() {
            return params;
        }

        let mut mode = ParamMode::Required;
        self.token_children(node)
            .filter_map(|token| match token.kind {
                SyntaxKind::Question => {
                    mode = ParamMode::Optional;
                    None
                }
                SyntaxKind::At => {
                    mode = ParamMode::Rest;
                    None
                }
                SyntaxKind::Ident => {
                    let param = Param {
                        name: self.text(token.span.clone()).to_owned(),
                        mode: mode.clone(),
                        default: None,
                    };
                    mode = ParamMode::Required;
                    Some(param)
                }
                _ => None,
            })
            .collect()
    }

    fn lower_param(&mut self, node: &CstNode) -> Param {
        let mode = if self
            .token_children(node)
            .any(|token| token.kind == SyntaxKind::At)
        {
            ParamMode::Rest
        } else if self
            .token_children(node)
            .any(|token| token.kind == SyntaxKind::Question)
        {
            ParamMode::Optional
        } else {
            ParamMode::Required
        };
        let name = self.first_text(node, SyntaxKind::Ident).unwrap_or_default();
        let default = self
            .node_children(node)
            .find(|child| is_expr_node(child.kind))
            .map(|child| self.lower_expr(child));
        Param {
            name,
            mode,
            default,
        }
    }

    fn error_expr(&mut self, node: &CstNode) -> Expr {
        Expr::Error {
            span: node.span.clone(),
        }
    }

    fn node_children<'n>(&self, node: &'n CstNode) -> impl Iterator<Item = &'n CstNode> + use<'n> {
        node.children.iter().filter_map(|child| match child {
            CstElement::Node(node) => Some(node),
            CstElement::Token(_) => None,
        })
    }

    fn token_children<'n>(
        &self,
        node: &'n CstNode,
    ) -> impl Iterator<Item = &'n CstToken> + use<'n> {
        node.children.iter().filter_map(|child| match child {
            CstElement::Node(_) => None,
            CstElement::Token(token) => Some(token),
        })
    }

    fn first_text(&self, node: &CstNode, kind: SyntaxKind) -> Option<String> {
        self.token_children(node)
            .find(|token| token.kind == kind)
            .map(|token| self.text(token.span.clone()).to_owned())
    }

    fn last_text(&self, node: &CstNode, kind: SyntaxKind) -> Option<String> {
        self.token_children(node)
            .filter(|token| token.kind == kind)
            .last()
            .map(|token| self.text(token.span.clone()).to_owned())
    }

    fn text(&self, span: std::ops::Range<usize>) -> &str {
        &self.source[span]
    }

    fn error(&mut self, node: &CstNode, message: &str) {
        self.errors
            .push(ParseError::new(message, node.span.clone()));
    }
}

fn identity_after_dollar(source: &str, tokens: &[&CstToken], start: usize) -> Option<String> {
    tokens
        .iter()
        .skip(start)
        .position(|token| token.kind == SyntaxKind::Dollar)
        .and_then(|relative| tokens.get(start + relative + 1))
        .filter(|token| matches!(token.kind, SyntaxKind::Ident | SyntaxKind::Int))
        .map(|token| source[token.span.clone()].to_owned())
}

fn unquote(text: &str) -> String {
    text.strip_prefix('"')
        .and_then(|text| text.strip_suffix('"'))
        .unwrap_or(text)
        .to_owned()
}

fn binary_op(token: &CstToken) -> Option<BinaryOp> {
    Some(match token.kind {
        SyntaxKind::EqEq => BinaryOp::Eq,
        SyntaxKind::BangEq => BinaryOp::Ne,
        SyntaxKind::Lt => BinaryOp::Lt,
        SyntaxKind::LtEq => BinaryOp::Le,
        SyntaxKind::Gt => BinaryOp::Gt,
        SyntaxKind::GtEq => BinaryOp::Ge,
        SyntaxKind::Plus => BinaryOp::Add,
        SyntaxKind::Minus => BinaryOp::Sub,
        SyntaxKind::Star => BinaryOp::Mul,
        SyntaxKind::Slash => BinaryOp::Div,
        SyntaxKind::Percent => BinaryOp::Rem,
        SyntaxKind::AmpAmp => BinaryOp::And,
        SyntaxKind::PipePipe => BinaryOp::Or,
        SyntaxKind::DotDot => BinaryOp::Range,
        _ => return None,
    })
}

fn is_expr_node(kind: SyntaxKind) -> bool {
    matches!(
        kind,
        SyntaxKind::LetExpr
            | SyntaxKind::ConstExpr
            | SyntaxKind::IfExpr
            | SyntaxKind::BeginExpr
            | SyntaxKind::ForExpr
            | SyntaxKind::WhileExpr
            | SyntaxKind::ReturnExpr
            | SyntaxKind::BreakExpr
            | SyntaxKind::ContinueExpr
            | SyntaxKind::TryExpr
            | SyntaxKind::FnExpr
            | SyntaxKind::LambdaExpr
            | SyntaxKind::AssertExpr
            | SyntaxKind::RetractExpr
            | SyntaxKind::RequireExpr
            | SyntaxKind::AssignExpr
            | SyntaxKind::BinaryExpr
            | SyntaxKind::UnaryExpr
            | SyntaxKind::CallExpr
            | SyntaxKind::ReceiverCallExpr
            | SyntaxKind::RoleCallExpr
            | SyntaxKind::IndexExpr
            | SyntaxKind::FieldExpr
            | SyntaxKind::ListExpr
            | SyntaxKind::MapExpr
            | SyntaxKind::GroupExpr
            | SyntaxKind::LiteralExpr
            | SyntaxKind::NameExpr
            | SyntaxKind::IdentityExpr
            | SyntaxKind::SymbolExpr
            | SyntaxKind::HoleExpr
            | SyntaxKind::AtomExpr
    )
}

#[cfg(test)]
mod tests {
    use super::parse_ast;
    use crate::{
        BinaryOp, BindingKind, BindingPattern, CollectionItem, EffectKind, Expr, FunctionBody,
        Item, Literal, MethodKind, ParamMode,
    };

    #[test]
    fn lowers_calls_and_collections() {
        let ast = parse_ast(
            "let xs = [1, @rest]\n\
             let opts = {:style -> :brief}\n\
             :move(actor: $alice, item: $coin)\n\
             $box:put($coin, :into)",
        );
        assert_eq!(ast.errors, vec![]);
        assert_eq!(ast.items.len(), 4);

        let Item::Expr(Expr::Binding {
            kind: BindingKind::Let,
            pattern: BindingPattern::Name(name),
            value: Some(value),
            ..
        }) = &ast.items[0]
        else {
            panic!("expected let binding");
        };
        assert_eq!(name, "xs");
        let Expr::List { items, .. } = &**value else {
            panic!("expected list");
        };
        assert!(matches!(items[1], CollectionItem::Splice(_)));

        let Item::Expr(Expr::RoleCall { args, .. }) = &ast.items[2] else {
            panic!("expected role call");
        };
        assert_eq!(args[0].role.as_deref(), Some("actor"));

        let Item::Expr(Expr::ReceiverCall { selector, .. }) = &ast.items[3] else {
            panic!("expected receiver call");
        };
        assert!(matches!(&**selector, Expr::Symbol { name, .. } if name == "put"));
    }

    #[test]
    fn lowers_relation_rule_and_control_forms() {
        let ast = parse_ast(
            "VisibleTo(actor, obj) :- LocatedIn(actor, room), LocatedIn(obj, room)\n\
             if Lit($lamp, true)\n  \"lit\"\nelse\n  \"dark\"\nend",
        );
        assert_eq!(ast.errors, vec![]);
        assert!(matches!(
            &ast.items[0],
            Item::RelationRule { body, .. } if body.len() == 2
        ));
        assert!(matches!(
            &ast.items[1],
            Item::Expr(Expr::If { else_items, .. }) if else_items.len() == 1
        ));
    }

    #[test]
    fn lowers_methods_objects_and_effects() {
        let ast = parse_ast(
            "object $lamp extends $thing\n\
               name = \"brass lamp\"\n\
             end\n\
             method $move_into :move\n\
               roles actor: $player, item: $portable\n\
             do\n\
               require CanMove(actor, item)\n\
               assert LocatedIn(item, destination)\n\
             end",
        );
        assert_eq!(ast.errors, vec![]);
        assert!(matches!(
            &ast.items[0],
            Item::Object { identity: Some(identity), extends: Some(extends), .. }
                if identity == "lamp" && extends == "thing"
        ));
        let Item::Method {
            kind,
            identity,
            selector,
            clauses,
            body,
            ..
        } = &ast.items[1]
        else {
            panic!("expected method");
        };
        assert_eq!(kind, &MethodKind::Method);
        assert_eq!(identity.as_deref(), Some("move_into"));
        assert_eq!(selector.as_deref(), Some("move"));
        assert_eq!(clauses.len(), 1);
        assert!(matches!(
            &body[0],
            Item::Expr(Expr::Effect {
                kind: EffectKind::Require,
                ..
            })
        ));
    }

    #[test]
    fn lowers_functions_loops_and_try() {
        let ast = parse_ast(
            "let f = {x, ?style = :short, @rest} => x + 1\n\
             transaction\n\
               for key, value in properties\n\
                 render_property(key, value)\n\
               end\n\
             end\n\
             try\n\
               risky()\n\
             catch err if err == :perm\n\
               \"permission denied\"\n\
             finally\n\
               cleanup()\n\
             end",
        );
        assert_eq!(ast.errors, vec![]);

        let Item::Expr(Expr::Binding {
            value: Some(value), ..
        }) = &ast.items[0]
        else {
            panic!("expected lambda binding");
        };
        let Expr::Function {
            params,
            body: FunctionBody::Expr(body),
            ..
        } = &**value
        else {
            panic!("expected lambda function");
        };
        assert_eq!(params[1].mode, ParamMode::Optional);
        assert_eq!(params[2].mode, ParamMode::Rest);
        assert!(matches!(
            &**body,
            Expr::Binary {
                op: BinaryOp::Add,
                ..
            }
        ));

        assert!(matches!(
            &ast.items[1],
            Item::Expr(Expr::Block { items, .. })
                if matches!(&items[0], Item::Expr(Expr::For { key, value: Some(value), .. }) if key == "key" && value == "value")
        ));
        assert!(matches!(
            &ast.items[2],
            Item::Expr(Expr::Try { catches, finally, .. }) if catches.len() == 1 && !finally.is_empty()
        ));
    }

    #[test]
    fn lowers_literals_and_field_assignment() {
        let ast = parse_ast("$lamp.name = \"golden lamp\"\ntrue\nnothing");
        assert_eq!(ast.errors, vec![]);
        assert!(matches!(
            &ast.items[0],
            Item::Expr(Expr::Assign { target, value, .. })
                if matches!(&**target, Expr::Field { name, .. } if name == "name")
                    && matches!(&**value, Expr::Literal { value: Literal::String(text), .. } if text == "golden lamp")
        ));
        assert!(matches!(
            &ast.items[1],
            Item::Expr(Expr::Literal {
                value: Literal::Bool(true),
                ..
            })
        ));
        assert!(matches!(
            &ast.items[2],
            Item::Expr(Expr::Literal {
                value: Literal::Nothing,
                ..
            })
        ));
    }
}
