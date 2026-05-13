use crate::{CstElement, CstNode, CstToken, Parse, ParseError, SyntaxKind, Token, lex};

pub fn parse(source: &str) -> Parse {
    let tokens = lex(source);
    Parser::new(&tokens).parse()
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
    errors: Vec<ParseError>,
}

impl<'a> Parser<'a> {
    fn new(tokens: &'a [Token]) -> Self {
        Self {
            tokens,
            pos: 0,
            errors: Vec::new(),
        }
    }

    fn parse(mut self) -> Parse {
        let mut children = Vec::new();
        children.push(CstElement::Node(self.parse_item_list(&[SyntaxKind::Eof])));
        self.consume_separators();
        children.push(self.expect_token(SyntaxKind::Eof, "expected end of file"));
        Parse {
            root: CstNode::new(SyntaxKind::Program, children),
            errors: self.errors,
        }
    }

    fn parse_item_list(&mut self, stops: &[SyntaxKind]) -> CstNode {
        let mut children = Vec::new();
        self.consume_separators();
        while !stops.contains(&self.current_kind()) {
            if self.current_kind() == SyntaxKind::Eof {
                break;
            }
            children.push(CstElement::Node(self.parse_item()));
            self.consume_separators();
        }
        CstNode::new(SyntaxKind::ItemList, children)
    }

    fn parse_item(&mut self) -> CstNode {
        match self.current_kind() {
            SyntaxKind::MethodKw => self.parse_method_like(SyntaxKind::MethodItem),
            SyntaxKind::VerbKw => self.parse_method_like(SyntaxKind::VerbItem),
            SyntaxKind::ObjectKw => self.parse_object_item(),
            _ => self.parse_expr_stmt(),
        }
    }

    fn parse_object_item(&mut self) -> CstNode {
        let mut children = Vec::new();
        children.push(self.bump_element());
        children.push(CstElement::Node(self.parse_object_header()));
        self.consume_separators();
        while !matches!(self.current_kind(), SyntaxKind::EndKw | SyntaxKind::Eof) {
            children.push(CstElement::Node(self.parse_object_clause()));
            self.consume_separators();
        }
        children.push(self.expect_token(SyntaxKind::EndKw, "expected end after object"));
        CstNode::new(SyntaxKind::ObjectItem, children)
    }

    fn parse_object_header(&mut self) -> CstNode {
        let mut children = Vec::new();
        while !matches!(
            self.current_kind(),
            SyntaxKind::Newline | SyntaxKind::Semi | SyntaxKind::EndKw | SyntaxKind::Eof
        ) {
            children.push(self.bump_element());
        }
        CstNode::new(SyntaxKind::ObjectHeader, children)
    }

    fn parse_object_clause(&mut self) -> CstNode {
        let mut children = Vec::new();
        while !matches!(
            self.current_kind(),
            SyntaxKind::Newline | SyntaxKind::Semi | SyntaxKind::EndKw | SyntaxKind::Eof
        ) {
            if Self::starts_expr(self.current_kind()) {
                children.push(CstElement::Node(self.parse_expr(0)));
            } else {
                children.push(self.bump_element());
            }
        }
        CstNode::new(SyntaxKind::ObjectClause, children)
    }

    fn parse_method_like(&mut self, kind: SyntaxKind) -> CstNode {
        let mut children = Vec::new();
        children.push(self.bump_element());
        children.push(CstElement::Node(self.parse_method_header()));
        self.consume_separators();

        while !matches!(
            self.current_kind(),
            SyntaxKind::DoKw | SyntaxKind::EndKw | SyntaxKind::Eof
        ) {
            children.push(CstElement::Node(self.parse_method_clause()));
            self.consume_separators();
        }

        if self.current_kind() == SyntaxKind::DoKw {
            children.push(self.bump_element());
            children.push(CstElement::Node(self.parse_block(&[SyntaxKind::EndKw])));
        }
        children.push(self.expect_token(SyntaxKind::EndKw, "expected end after method body"));
        CstNode::new(kind, children)
    }

    fn parse_method_header(&mut self) -> CstNode {
        let mut children = Vec::new();
        while !matches!(
            self.current_kind(),
            SyntaxKind::Newline
                | SyntaxKind::Semi
                | SyntaxKind::DoKw
                | SyntaxKind::EndKw
                | SyntaxKind::Eof
        ) {
            children.push(self.bump_element());
        }
        CstNode::new(SyntaxKind::MethodHeader, children)
    }

    fn parse_method_clause(&mut self) -> CstNode {
        let mut children = Vec::new();
        while !matches!(
            self.current_kind(),
            SyntaxKind::Newline
                | SyntaxKind::Semi
                | SyntaxKind::DoKw
                | SyntaxKind::EndKw
                | SyntaxKind::Eof
        ) {
            children.push(self.bump_element());
        }
        CstNode::new(SyntaxKind::MethodClause, children)
    }

    fn parse_expr_stmt(&mut self) -> CstNode {
        let mut children = Vec::new();
        let current = self.current_kind();
        if Self::starts_expr(current) {
            let expr = self.parse_expr(0);
            if self.current_kind() == SyntaxKind::ColonDash {
                children.push(CstElement::Node(self.parse_relation_rule(expr)));
            } else {
                children.push(CstElement::Node(expr));
            }
        } else {
            self.error("expected expression");
            children.push(self.bump_element());
        }
        CstNode::new(SyntaxKind::ExprStmt, children)
    }

    fn parse_relation_rule(&mut self, head: CstNode) -> CstNode {
        let mut children = vec![CstElement::Node(head), self.bump_element()];
        loop {
            self.consume_separators();
            let current = self.current_kind();
            if !Self::starts_expr(current) {
                self.error("expected relation atom in rule body");
                break;
            }
            children.push(CstElement::Node(self.parse_expr(0)));
            if self.current_kind() != SyntaxKind::Comma {
                break;
            }
            children.push(self.bump_element());
        }
        CstNode::new(SyntaxKind::RelationRule, children)
    }

    fn parse_block(&mut self, stops: &[SyntaxKind]) -> CstNode {
        let mut children = Vec::new();
        self.consume_separators();
        while !stops.contains(&self.current_kind()) && self.current_kind() != SyntaxKind::Eof {
            children.push(CstElement::Node(self.parse_item()));
            self.consume_separators();
        }
        CstNode::new(SyntaxKind::Block, children)
    }

    fn parse_expr(&mut self, min_bp: u8) -> CstNode {
        let mut lhs = self.parse_prefix();
        loop {
            lhs = match self.current_kind() {
                SyntaxKind::LParen => {
                    if 13 < min_bp {
                        break;
                    }
                    self.parse_call(lhs)
                }
                SyntaxKind::LBracket => {
                    if 13 < min_bp {
                        break;
                    }
                    self.parse_index(lhs)
                }
                SyntaxKind::Dot => {
                    if 13 < min_bp {
                        break;
                    }
                    self.parse_field(lhs)
                }
                SyntaxKind::Colon => {
                    if 13 < min_bp {
                        break;
                    }
                    self.parse_receiver_call(lhs)
                }
                op => {
                    let Some((left_bp, right_bp, kind)) = infix_binding_power(op) else {
                        break;
                    };
                    if left_bp < min_bp {
                        break;
                    }
                    let mut children = vec![CstElement::Node(lhs), self.bump_element()];
                    children.push(CstElement::Node(self.parse_expr(right_bp)));
                    CstNode::new(kind, children)
                }
            };
        }
        lhs
    }

    fn parse_prefix(&mut self) -> CstNode {
        match self.current_kind() {
            SyntaxKind::LetKw => self.parse_binding_expr(SyntaxKind::LetExpr),
            SyntaxKind::ConstKw => self.parse_binding_expr(SyntaxKind::ConstExpr),
            SyntaxKind::IfKw => self.parse_if_expr(),
            SyntaxKind::BeginKw => self.parse_begin_expr(),
            SyntaxKind::ForKw => self.parse_for_expr(),
            SyntaxKind::WhileKw => self.parse_while_expr(),
            SyntaxKind::ReturnKw => self.parse_return_expr(),
            SyntaxKind::RaiseKw => self.parse_raise_expr(),
            SyntaxKind::RecoverKw => self.parse_recover_expr(),
            SyntaxKind::OneKw => self.parse_one_expr(),
            SyntaxKind::BreakKw => self.parse_simple_control_expr(SyntaxKind::BreakExpr),
            SyntaxKind::ContinueKw => self.parse_simple_control_expr(SyntaxKind::ContinueExpr),
            SyntaxKind::TryKw => self.parse_try_expr(),
            SyntaxKind::FnKw => self.parse_fn_expr(),
            SyntaxKind::TransactionKw | SyntaxKind::AtomicKw => self.parse_begin_like_expr(),
            SyntaxKind::AssertKw => self.parse_effect_expr(SyntaxKind::AssertExpr),
            SyntaxKind::RetractKw => self.parse_effect_expr(SyntaxKind::RetractExpr),
            SyntaxKind::RequireKw => self.parse_effect_expr(SyntaxKind::RequireExpr),
            SyntaxKind::Bang | SyntaxKind::Minus => self.parse_unary_expr(),
            SyntaxKind::LParen => self.parse_group_expr(),
            SyntaxKind::LBracket => self.parse_list_expr(),
            SyntaxKind::LBrace if self.looks_like_brace_lambda() => self.parse_brace_lambda_expr(),
            SyntaxKind::LBrace => self.parse_map_expr(),
            SyntaxKind::Dollar
                if matches!(self.nth_kind(1), SyntaxKind::Ident | SyntaxKind::Int) =>
            {
                self.parse_identity_expr()
            }
            SyntaxKind::Dollar => self.single_token_node(SyntaxKind::HoleExpr),
            SyntaxKind::Colon => self.parse_symbol_or_role_call(),
            SyntaxKind::Question if self.nth_kind(1) == SyntaxKind::Ident => {
                self.parse_query_var_expr()
            }
            SyntaxKind::Underscore => self.single_token_node(SyntaxKind::HoleExpr),
            SyntaxKind::Ident => self.single_token_node(SyntaxKind::NameExpr),
            SyntaxKind::ErrorCode
            | SyntaxKind::Int
            | SyntaxKind::Float
            | SyntaxKind::String
            | SyntaxKind::TrueKw
            | SyntaxKind::FalseKw
            | SyntaxKind::NothingKw => self.single_token_node(SyntaxKind::LiteralExpr),
            _ => {
                self.error("expected expression");
                self.single_token_node(SyntaxKind::AtomExpr)
            }
        }
    }

    fn parse_binding_expr(&mut self, kind: SyntaxKind) -> CstNode {
        let mut children = vec![self.bump_element()];
        if self.current_kind() == SyntaxKind::LBracket {
            children.push(CstElement::Node(self.parse_pattern_brackets()));
        } else {
            children.push(
                self.expect_token(SyntaxKind::Ident, "expected binding name or list pattern"),
            );
        }
        if self.current_kind() == SyntaxKind::Eq {
            children.push(self.bump_element());
            children.push(CstElement::Node(self.parse_expr(0)));
        }
        CstNode::new(kind, children)
    }

    fn parse_if_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        children.push(CstElement::Node(self.parse_expr(0)));
        children.push(CstElement::Node(self.parse_block(&[
            SyntaxKind::ElseIfKw,
            SyntaxKind::ElseKw,
            SyntaxKind::EndKw,
        ])));
        while self.current_kind() == SyntaxKind::ElseIfKw {
            let mut clause = vec![self.bump_element()];
            clause.push(CstElement::Node(self.parse_expr(0)));
            clause.push(CstElement::Node(self.parse_block(&[
                SyntaxKind::ElseIfKw,
                SyntaxKind::ElseKw,
                SyntaxKind::EndKw,
            ])));
            children.push(CstElement::Node(CstNode::new(
                SyntaxKind::ElseIfClause,
                clause,
            )));
        }
        if self.current_kind() == SyntaxKind::ElseKw {
            children.push(CstElement::Node(self.parse_else_clause()));
        }
        children.push(self.expect_token(SyntaxKind::EndKw, "expected end after if"));
        CstNode::new(SyntaxKind::IfExpr, children)
    }

    fn parse_else_clause(&mut self) -> CstNode {
        let children = vec![
            self.bump_element(),
            CstElement::Node(self.parse_block(&[SyntaxKind::EndKw])),
        ];
        CstNode::new(SyntaxKind::ElseClause, children)
    }

    fn parse_begin_expr(&mut self) -> CstNode {
        let children = vec![
            self.bump_element(),
            CstElement::Node(self.parse_block(&[SyntaxKind::EndKw])),
            self.expect_token(SyntaxKind::EndKw, "expected end after begin"),
        ];
        CstNode::new(SyntaxKind::BeginExpr, children)
    }

    fn parse_begin_like_expr(&mut self) -> CstNode {
        let children = vec![
            self.bump_element(),
            CstElement::Node(self.parse_block(&[SyntaxKind::EndKw])),
            self.expect_token(SyntaxKind::EndKw, "expected end after block"),
        ];
        CstNode::new(SyntaxKind::BeginExpr, children)
    }

    fn parse_for_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        children.push(self.expect_token(SyntaxKind::Ident, "expected loop binding"));
        if self.current_kind() == SyntaxKind::Comma {
            children.push(self.bump_element());
            children.push(self.expect_token(SyntaxKind::Ident, "expected loop value binding"));
        }
        children.push(self.expect_token(SyntaxKind::InKw, "expected in in for loop"));
        children.push(CstElement::Node(self.parse_expr(0)));
        children.push(CstElement::Node(self.parse_block(&[SyntaxKind::EndKw])));
        children.push(self.expect_token(SyntaxKind::EndKw, "expected end after for"));
        CstNode::new(SyntaxKind::ForExpr, children)
    }

    fn parse_while_expr(&mut self) -> CstNode {
        let children = vec![
            self.bump_element(),
            CstElement::Node(self.parse_expr(0)),
            CstElement::Node(self.parse_block(&[SyntaxKind::EndKw])),
            self.expect_token(SyntaxKind::EndKw, "expected end after while"),
        ];
        CstNode::new(SyntaxKind::WhileExpr, children)
    }

    fn parse_return_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        let current = self.current_kind();
        if Self::starts_expr(current) && !self.at_separator_or_stop() {
            children.push(CstElement::Node(self.parse_expr(0)));
        }
        CstNode::new(SyntaxKind::ReturnExpr, children)
    }

    fn parse_raise_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        if Self::starts_expr(self.current_kind()) && !self.at_separator_or_stop() {
            children.push(CstElement::Node(self.parse_expr(0)));
            for _ in 0..2 {
                if self.current_kind() != SyntaxKind::Comma {
                    break;
                }
                children.push(self.bump_element());
                children.push(CstElement::Node(self.parse_expr(0)));
            }
        }
        CstNode::new(SyntaxKind::RaiseExpr, children)
    }

    fn parse_recover_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        children.push(CstElement::Node(self.parse_expr(0)));
        self.consume_separators();
        while self.current_kind() == SyntaxKind::CatchKw {
            children.push(CstElement::Node(self.parse_recover_clause()));
            self.consume_separators();
        }
        children.push(self.expect_token(SyntaxKind::EndKw, "expected end after recover"));
        CstNode::new(SyntaxKind::RecoverExpr, children)
    }

    fn parse_one_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        children.push(CstElement::Node(self.parse_expr(13)));
        CstNode::new(SyntaxKind::OneExpr, children)
    }

    fn parse_recover_clause(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        if self.current_kind() == SyntaxKind::Ident && self.nth_kind(1) != SyntaxKind::AsKw {
            children.push(self.bump_element());
            if self.current_kind() == SyntaxKind::IfKw {
                children.push(self.bump_element());
                children.push(CstElement::Node(self.parse_expr(0)));
            }
        } else if Self::starts_expr(self.current_kind()) {
            children.push(CstElement::Node(self.parse_expr(0)));
        }
        if self.current_kind() == SyntaxKind::AsKw {
            children.push(self.bump_element());
            children.push(self.expect_token(SyntaxKind::Ident, "expected recovery binding"));
        }
        children.push(self.expect_token(SyntaxKind::FatArrow, "expected => in recovery clause"));
        children.push(CstElement::Node(self.parse_expr(0)));
        CstNode::new(SyntaxKind::RecoverClause, children)
    }

    fn parse_simple_control_expr(&mut self, kind: SyntaxKind) -> CstNode {
        CstNode::new(kind, vec![self.bump_element()])
    }

    fn parse_try_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        children.push(CstElement::Node(self.parse_block(&[
            SyntaxKind::CatchKw,
            SyntaxKind::FinallyKw,
            SyntaxKind::EndKw,
        ])));
        while self.current_kind() == SyntaxKind::CatchKw {
            children.push(CstElement::Node(self.parse_catch_clause()));
        }
        if self.current_kind() == SyntaxKind::FinallyKw {
            children.push(CstElement::Node(self.parse_finally_clause()));
        }
        children.push(self.expect_token(SyntaxKind::EndKw, "expected end after try"));
        CstNode::new(SyntaxKind::TryExpr, children)
    }

    fn parse_catch_clause(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        if self.current_kind() == SyntaxKind::Ident && self.nth_kind(1) != SyntaxKind::AsKw {
            children.push(self.bump_element());
            if self.current_kind() == SyntaxKind::IfKw {
                children.push(self.bump_element());
                children.push(CstElement::Node(self.parse_expr(0)));
            }
        } else if Self::starts_expr(self.current_kind()) {
            children.push(CstElement::Node(self.parse_expr(0)));
        }
        if self.current_kind() == SyntaxKind::AsKw {
            children.push(self.bump_element());
            children.push(self.expect_token(SyntaxKind::Ident, "expected catch binding"));
        }
        children.push(CstElement::Node(self.parse_block(&[
            SyntaxKind::CatchKw,
            SyntaxKind::FinallyKw,
            SyntaxKind::EndKw,
        ])));
        CstNode::new(SyntaxKind::CatchClause, children)
    }

    fn parse_finally_clause(&mut self) -> CstNode {
        let children = vec![
            self.bump_element(),
            CstElement::Node(self.parse_block(&[SyntaxKind::EndKw])),
        ];
        CstNode::new(SyntaxKind::FinallyClause, children)
    }

    fn parse_fn_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        if self.current_kind() == SyntaxKind::Ident && self.nth_kind(1) == SyntaxKind::LParen {
            children.push(self.bump_element());
        }
        children.push(CstElement::Node(self.parse_param_list()));
        if self.current_kind() == SyntaxKind::FatArrow {
            children.push(self.bump_element());
            children.push(CstElement::Node(self.parse_expr(0)));
        } else {
            children.push(CstElement::Node(self.parse_block(&[SyntaxKind::EndKw])));
            children.push(self.expect_token(SyntaxKind::EndKw, "expected end after fn"));
        }
        CstNode::new(SyntaxKind::FnExpr, children)
    }

    fn parse_brace_lambda_expr(&mut self) -> CstNode {
        let children = vec![
            CstElement::Node(self.parse_brace_param_list()),
            self.expect_token(
                SyntaxKind::FatArrow,
                "expected '=>' after lambda parameters",
            ),
            CstElement::Node(self.parse_expr(0)),
        ];
        CstNode::new(SyntaxKind::LambdaExpr, children)
    }

    fn parse_effect_expr(&mut self, kind: SyntaxKind) -> CstNode {
        let mut children = vec![self.bump_element()];
        children.push(CstElement::Node(self.parse_expr(0)));
        CstNode::new(kind, children)
    }

    fn parse_unary_expr(&mut self) -> CstNode {
        let children = vec![self.bump_element(), CstElement::Node(self.parse_expr(12))];
        CstNode::new(SyntaxKind::UnaryExpr, children)
    }

    fn parse_group_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        if self.current_kind() != SyntaxKind::RParen {
            children.push(CstElement::Node(self.parse_expr(0)));
        }
        children.push(self.expect_token(SyntaxKind::RParen, "expected ')'"));
        CstNode::new(SyntaxKind::GroupExpr, children)
    }

    fn parse_list_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        self.parse_delimited_items(SyntaxKind::RBracket, SyntaxKind::ListItem, &mut children);
        children.push(self.expect_token(SyntaxKind::RBracket, "expected ']'"));
        CstNode::new(SyntaxKind::ListExpr, children)
    }

    fn parse_map_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        while !matches!(self.current_kind(), SyntaxKind::RBrace | SyntaxKind::Eof) {
            let entry = vec![
                CstElement::Node(self.parse_expr(0)),
                self.expect_token(SyntaxKind::Arrow, "expected '->' in map entry"),
                CstElement::Node(self.parse_expr(0)),
            ];
            children.push(CstElement::Node(CstNode::new(SyntaxKind::MapEntry, entry)));
            if self.current_kind() != SyntaxKind::Comma {
                break;
            }
            children.push(self.bump_element());
        }
        children.push(self.expect_token(SyntaxKind::RBrace, "expected '}'"));
        CstNode::new(SyntaxKind::MapExpr, children)
    }

    fn parse_brace_param_list(&mut self) -> CstNode {
        self.parse_param_list_between(SyntaxKind::LBrace, SyntaxKind::RBrace)
    }

    fn parse_pattern_brackets(&mut self) -> CstNode {
        self.parse_param_list_between(SyntaxKind::LBracket, SyntaxKind::RBracket)
    }

    fn parse_param_list_between(&mut self, open: SyntaxKind, close: SyntaxKind) -> CstNode {
        let mut children = vec![self.expect_token(open, "expected parameter list")];
        while !matches!(self.current_kind(), SyntaxKind::Eof) && self.current_kind() != close {
            let mut param = Vec::new();
            if matches!(self.current_kind(), SyntaxKind::Question | SyntaxKind::At) {
                param.push(self.bump_element());
            }
            param.push(self.expect_token(SyntaxKind::Ident, "expected parameter name"));
            if self.current_kind() == SyntaxKind::Eq {
                param.push(self.bump_element());
                param.push(CstElement::Node(self.parse_expr(0)));
            }
            children.push(CstElement::Node(CstNode::new(SyntaxKind::Param, param)));
            if self.current_kind() != SyntaxKind::Comma {
                break;
            }
            children.push(self.bump_element());
        }
        children.push(self.expect_token(close, "expected end of parameter list"));
        CstNode::new(SyntaxKind::ParamList, children)
    }

    fn parse_identity_expr(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        if matches!(self.current_kind(), SyntaxKind::Ident | SyntaxKind::Int) {
            children.push(self.bump_element());
        } else {
            children.push(self.missing("expected identity name after '$'"));
        }
        CstNode::new(SyntaxKind::IdentityExpr, children)
    }

    fn parse_symbol_or_role_call(&mut self) -> CstNode {
        let mut children = vec![self.bump_element()];
        if self.current_kind() == SyntaxKind::LParen {
            children.push(CstElement::Node(self.parse_group_expr()));
        } else {
            children.push(self.expect_token(SyntaxKind::Ident, "expected symbol name after ':'"));
        }
        if self.current_kind() == SyntaxKind::LParen {
            children.push(CstElement::Node(self.parse_arg_list()));
            CstNode::new(SyntaxKind::RoleCallExpr, children)
        } else {
            CstNode::new(SyntaxKind::SymbolExpr, children)
        }
    }

    fn parse_call(&mut self, callee: CstNode) -> CstNode {
        let children = vec![
            CstElement::Node(callee),
            CstElement::Node(self.parse_arg_list()),
        ];
        CstNode::new(SyntaxKind::CallExpr, children)
    }

    fn parse_receiver_call(&mut self, receiver: CstNode) -> CstNode {
        let mut children = vec![CstElement::Node(receiver), self.bump_element()];
        if self.current_kind() == SyntaxKind::LParen {
            children.push(CstElement::Node(self.parse_group_expr()));
        } else {
            children.push(self.expect_token(SyntaxKind::Ident, "expected selector after ':'"));
        }
        children.push(CstElement::Node(self.parse_arg_list()));
        CstNode::new(SyntaxKind::ReceiverCallExpr, children)
    }

    fn parse_index(&mut self, collection: CstNode) -> CstNode {
        let mut children = vec![CstElement::Node(collection), self.bump_element()];
        if self.current_kind() != SyntaxKind::RBracket {
            children.push(CstElement::Node(self.parse_expr(0)));
        }
        children.push(self.expect_token(SyntaxKind::RBracket, "expected ']'"));
        CstNode::new(SyntaxKind::IndexExpr, children)
    }

    fn parse_field(&mut self, base: CstNode) -> CstNode {
        let children = vec![
            CstElement::Node(base),
            self.bump_element(),
            self.expect_token(SyntaxKind::Ident, "expected field name after '.'"),
        ];
        CstNode::new(SyntaxKind::FieldExpr, children)
    }

    fn parse_param_list(&mut self) -> CstNode {
        let mut children = Vec::new();
        children.push(self.expect_token(SyntaxKind::LParen, "expected '('"));
        while !matches!(self.current_kind(), SyntaxKind::RParen | SyntaxKind::Eof) {
            let mut param = Vec::new();
            if matches!(self.current_kind(), SyntaxKind::Question | SyntaxKind::At) {
                param.push(self.bump_element());
            }
            param.push(self.expect_token(SyntaxKind::Ident, "expected parameter name"));
            if self.current_kind() == SyntaxKind::Eq {
                param.push(self.bump_element());
                param.push(CstElement::Node(self.parse_expr(0)));
            }
            children.push(CstElement::Node(CstNode::new(SyntaxKind::Param, param)));
            if self.current_kind() != SyntaxKind::Comma {
                break;
            }
            children.push(self.bump_element());
        }
        children.push(self.expect_token(SyntaxKind::RParen, "expected ')'"));
        CstNode::new(SyntaxKind::ParamList, children)
    }

    fn parse_arg_list(&mut self) -> CstNode {
        let mut children = Vec::new();
        children.push(self.expect_token(SyntaxKind::LParen, "expected '('"));
        while !matches!(self.current_kind(), SyntaxKind::RParen | SyntaxKind::Eof) {
            let mut arg = Vec::new();
            if self.current_kind() == SyntaxKind::Ident && self.nth_kind(1) == SyntaxKind::Colon {
                arg.push(self.bump_element());
                arg.push(self.bump_element());
            }
            if self.current_kind() == SyntaxKind::At {
                arg.push(self.bump_element());
            }
            arg.push(CstElement::Node(self.parse_expr(0)));
            children.push(CstElement::Node(CstNode::new(SyntaxKind::Arg, arg)));
            if self.current_kind() != SyntaxKind::Comma {
                break;
            }
            children.push(self.bump_element());
        }
        children.push(self.expect_token(SyntaxKind::RParen, "expected ')'"));
        CstNode::new(SyntaxKind::ArgList, children)
    }

    fn parse_delimited_items(
        &mut self,
        stop: SyntaxKind,
        item_kind: SyntaxKind,
        children: &mut Vec<CstElement>,
    ) {
        while !matches!(self.current_kind(), SyntaxKind::Eof) && self.current_kind() != stop {
            let mut item = Vec::new();
            if self.current_kind() == SyntaxKind::At {
                item.push(self.bump_element());
            }
            item.push(CstElement::Node(self.parse_expr(0)));
            children.push(CstElement::Node(CstNode::new(item_kind, item)));
            if self.current_kind() != SyntaxKind::Comma {
                break;
            }
            children.push(self.bump_element());
        }
    }

    fn single_token_node(&mut self, kind: SyntaxKind) -> CstNode {
        CstNode::new(kind, vec![self.bump_element()])
    }

    fn parse_query_var_expr(&mut self) -> CstNode {
        CstNode::new(
            SyntaxKind::QueryVarExpr,
            vec![
                self.bump_element(),
                self.expect_token(SyntaxKind::Ident, "expected query variable name"),
            ],
        )
    }

    fn starts_expr(kind: SyntaxKind) -> bool {
        matches!(
            kind,
            SyntaxKind::LetKw
                | SyntaxKind::ConstKw
                | SyntaxKind::IfKw
                | SyntaxKind::BeginKw
                | SyntaxKind::ForKw
                | SyntaxKind::WhileKw
                | SyntaxKind::ReturnKw
                | SyntaxKind::RaiseKw
                | SyntaxKind::RecoverKw
                | SyntaxKind::OneKw
                | SyntaxKind::BreakKw
                | SyntaxKind::ContinueKw
                | SyntaxKind::TryKw
                | SyntaxKind::FnKw
                | SyntaxKind::TransactionKw
                | SyntaxKind::AtomicKw
                | SyntaxKind::AssertKw
                | SyntaxKind::RetractKw
                | SyntaxKind::RequireKw
                | SyntaxKind::Bang
                | SyntaxKind::Minus
                | SyntaxKind::LParen
                | SyntaxKind::LBracket
                | SyntaxKind::LBrace
                | SyntaxKind::Dollar
                | SyntaxKind::Colon
                | SyntaxKind::Question
                | SyntaxKind::Underscore
                | SyntaxKind::Ident
                | SyntaxKind::ErrorCode
                | SyntaxKind::Int
                | SyntaxKind::Float
                | SyntaxKind::String
                | SyntaxKind::TrueKw
                | SyntaxKind::FalseKw
                | SyntaxKind::NothingKw
        )
    }

    fn looks_like_brace_lambda(&self) -> bool {
        let mut idx = self.pos;
        let mut depth = 0usize;
        loop {
            let token = self
                .tokens
                .get(idx)
                .or_else(|| self.tokens.last())
                .expect("lexer always emits EOF");
            match token.kind {
                SyntaxKind::LBrace => depth += 1,
                SyntaxKind::RBrace => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return self.nth_non_ws_kind_from(idx + 1, 0) == SyntaxKind::FatArrow;
                    }
                }
                SyntaxKind::Eof => return false,
                _ => {}
            }
            idx += 1;
        }
    }

    fn consume_separators(&mut self) {
        while matches!(self.current_kind(), SyntaxKind::Semi | SyntaxKind::Newline) {
            self.pos += 1;
            self.skip_ws_comments();
        }
    }

    fn at_separator_or_stop(&mut self) -> bool {
        matches!(
            self.current_kind(),
            SyntaxKind::Semi
                | SyntaxKind::Newline
                | SyntaxKind::ElseIfKw
                | SyntaxKind::ElseKw
                | SyntaxKind::CatchKw
                | SyntaxKind::FinallyKw
                | SyntaxKind::EndKw
                | SyntaxKind::Eof
        )
    }

    fn expect_token(&mut self, kind: SyntaxKind, message: &str) -> CstElement {
        if self.current_kind() == kind {
            self.bump_element()
        } else {
            self.missing(message)
        }
    }

    fn missing(&mut self, message: &str) -> CstElement {
        self.error(message);
        let span = self.current_span();
        CstElement::Token(CstToken {
            kind: SyntaxKind::Error,
            span,
        })
    }

    fn bump_element(&mut self) -> CstElement {
        self.skip_ws_comments();
        let token = self
            .tokens
            .get(self.pos)
            .or_else(|| self.tokens.last())
            .expect("lexer always emits EOF")
            .clone();
        if token.kind != SyntaxKind::Eof {
            self.pos += 1;
        }
        self.skip_ws_comments();
        CstElement::Token(token.into())
    }

    fn current_kind(&mut self) -> SyntaxKind {
        self.skip_ws_comments();
        self.tokens
            .get(self.pos)
            .or_else(|| self.tokens.last())
            .expect("lexer always emits EOF")
            .kind
    }

    fn nth_kind(&self, n: usize) -> SyntaxKind {
        self.nth_non_ws_kind_from(self.pos, n)
    }

    fn nth_non_ws_kind_from(&self, start: usize, n: usize) -> SyntaxKind {
        let mut idx = start;
        let mut seen = 0usize;
        loop {
            let token = self
                .tokens
                .get(idx)
                .or_else(|| self.tokens.last())
                .expect("lexer always emits EOF");
            if !matches!(token.kind, SyntaxKind::Whitespace | SyntaxKind::LineComment) {
                if seen == n {
                    return token.kind;
                }
                seen += 1;
            }
            if token.kind == SyntaxKind::Eof {
                return SyntaxKind::Eof;
            }
            idx += 1;
        }
    }

    fn current_span(&mut self) -> std::ops::Range<usize> {
        self.skip_ws_comments();
        self.tokens
            .get(self.pos)
            .or_else(|| self.tokens.last())
            .expect("lexer always emits EOF")
            .span
            .clone()
    }

    fn skip_ws_comments(&mut self) {
        while matches!(
            self.tokens
                .get(self.pos)
                .or_else(|| self.tokens.last())
                .expect("lexer always emits EOF")
                .kind,
            SyntaxKind::Whitespace | SyntaxKind::LineComment
        ) {
            self.pos += 1;
        }
    }

    fn error(&mut self, message: &str) {
        let span = self.current_span();
        self.errors.push(ParseError::new(message, span));
    }
}

fn infix_binding_power(kind: SyntaxKind) -> Option<(u8, u8, SyntaxKind)> {
    Some(match kind {
        SyntaxKind::Eq => (1, 1, SyntaxKind::AssignExpr),
        SyntaxKind::PipePipe => (2, 3, SyntaxKind::BinaryExpr),
        SyntaxKind::AmpAmp => (4, 5, SyntaxKind::BinaryExpr),
        SyntaxKind::EqEq | SyntaxKind::BangEq => (6, 7, SyntaxKind::BinaryExpr),
        SyntaxKind::Lt | SyntaxKind::LtEq | SyntaxKind::Gt | SyntaxKind::GtEq => {
            (8, 9, SyntaxKind::BinaryExpr)
        }
        SyntaxKind::DotDot => (10, 11, SyntaxKind::BinaryExpr),
        SyntaxKind::Plus | SyntaxKind::Minus => (12, 13, SyntaxKind::BinaryExpr),
        SyntaxKind::Star | SyntaxKind::Slash | SyntaxKind::Percent => {
            (14, 15, SyntaxKind::BinaryExpr)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::parse;
    use crate::{CstElement, CstNode, SyntaxKind};

    fn contains(node: &CstNode, kind: SyntaxKind) -> bool {
        node.kind == kind
            || node.children.iter().any(|child| match child {
                CstElement::Node(node) => contains(node, kind),
                CstElement::Token(token) => token.kind == kind,
            })
    }

    #[test]
    fn parses_bracket_lists_and_brace_maps() {
        let parse = parse("let xs = [1, @rest]\nlet opts = {:style -> :brief}");
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::ListExpr));
        assert!(contains(&parse.root, SyntaxKind::MapExpr));
        assert!(contains(&parse.root, SyntaxKind::MapEntry));
    }

    #[test]
    fn parses_bare_dollar_as_range_endpoint_hole() {
        let parse = parse("items[2..$]");
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::BinaryExpr));
        assert!(contains(&parse.root, SyntaxKind::HoleExpr));
    }

    #[test]
    fn parses_bracket_scatter_binding() {
        let parse = parse("let [head, ?middle = 10, @tail] = values");
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::LetExpr));
        assert!(contains(&parse.root, SyntaxKind::ParamList));
        assert!(contains(&parse.root, SyntaxKind::Param));
    }

    #[test]
    fn parses_call_argument_splices() {
        let parse = parse("summarize(first, @rest)");
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::CallExpr));
        assert!(contains(&parse.root, SyntaxKind::At));
    }

    #[test]
    fn parses_query_variables_in_relation_calls() {
        let parse = parse("Location($thing, ?room)");
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::CallExpr));
        assert!(contains(&parse.root, SyntaxKind::QueryVarExpr));
    }

    #[test]
    fn parses_one_relation_query_expression() {
        let parse = parse("one Location($thing, ?room)");
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::OneExpr));
        assert!(contains(&parse.root, SyntaxKind::QueryVarExpr));
    }

    #[test]
    fn parses_role_and_receiver_calls() {
        let parse = parse(":move(actor: $alice, item: $coin)\n$box:put($coin, :into)");
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::RoleCallExpr));
        assert!(contains(&parse.root, SyntaxKind::ReceiverCallExpr));
    }

    #[test]
    fn parses_brace_lambda_without_confusing_it_for_map() {
        let parse = parse("let f = {x, ?style = :short, @rest} => x + 1");
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::LambdaExpr));
        assert!(!contains(&parse.root, SyntaxKind::MapEntry));
    }

    #[test]
    fn parses_expression_blocks_and_relation_rules() {
        let parse = parse(
            "VisibleTo(actor, obj) :- LocatedIn(actor, room), LocatedIn(obj, room)\n\
             if Lit($lamp, true)\n  \"lit\"\nelse\n  \"dark\"\nend",
        );
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::RelationRule));
        assert!(contains(&parse.root, SyntaxKind::IfExpr));
    }

    #[test]
    fn parses_method_fileout_envelope() {
        let parse = parse(
            "method $move_into :move\n\
               roles actor: $player, item: $portable\n\
             do\n\
               require CanMove(actor, item)\n\
               assert LocatedIn(item, destination)\n\
             end",
        );
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::MethodItem));
        assert!(contains(&parse.root, SyntaxKind::RequireExpr));
        assert!(contains(&parse.root, SyntaxKind::AssertExpr));
    }

    #[test]
    fn parses_try_and_object_fileout_envelope() {
        let parse = parse(
            "object $lamp extends $thing\n\
               name = \"brass lamp\"\n\
             end\n\
             try\n\
               risky()\n\
             catch err if err == :perm\n\
               \"permission denied\"\n\
             finally\n\
               cleanup()\n\
             end",
        );
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::ObjectItem));
        assert!(contains(&parse.root, SyntaxKind::TryExpr));
        assert!(contains(&parse.root, SyntaxKind::CatchClause));
        assert!(contains(&parse.root, SyntaxKind::FinallyClause));
    }

    #[test]
    fn parses_transaction_and_key_value_for_loop() {
        let parse = parse(
            "transaction\n\
               for key, value in properties\n\
                 render_property(key, value)\n\
               end\n\
             end",
        );
        assert_eq!(parse.errors, vec![]);
        assert!(contains(&parse.root, SyntaxKind::BeginExpr));
        assert!(contains(&parse.root, SyntaxKind::ForExpr));
    }
}
