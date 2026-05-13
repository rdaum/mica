use crate::{SyntaxKind, Token};

pub fn lex(source: &str) -> Vec<Token> {
    Lexer::new(source).lex()
}

struct Lexer<'a> {
    source: &'a str,
    pos: usize,
    tokens: Vec<Token>,
}

impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            pos: 0,
            tokens: Vec::new(),
        }
    }

    fn lex(mut self) -> Vec<Token> {
        while let Some(ch) = self.peek() {
            let start = self.pos;
            let kind = match ch {
                ' ' | '\t' => {
                    self.consume_while(|c| matches!(c, ' ' | '\t'));
                    SyntaxKind::Whitespace
                }
                '\r' | '\n' => {
                    self.consume_newline();
                    SyntaxKind::Newline
                }
                '/' if self.peek_next() == Some('/') => {
                    self.bump();
                    self.bump();
                    self.consume_while(|c| !matches!(c, '\r' | '\n'));
                    SyntaxKind::LineComment
                }
                '"' => self.lex_string(),
                '0'..='9' => self.lex_number(),
                'a'..='z' | 'A'..='Z' => self.lex_ident_or_keyword(start),
                '_' => {
                    self.bump();
                    if self.peek().is_some_and(is_ident_continue) {
                        self.consume_while(is_ident_continue);
                        SyntaxKind::Ident
                    } else {
                        SyntaxKind::Underscore
                    }
                }
                '(' => self.one(SyntaxKind::LParen),
                ')' => self.one(SyntaxKind::RParen),
                '[' => self.one(SyntaxKind::LBracket),
                ']' => self.one(SyntaxKind::RBracket),
                '{' => self.one(SyntaxKind::LBrace),
                '}' => self.one(SyntaxKind::RBrace),
                ',' => self.one(SyntaxKind::Comma),
                ';' => self.one(SyntaxKind::Semi),
                '$' => self.one(SyntaxKind::Dollar),
                '@' => self.one(SyntaxKind::At),
                '?' => self.one(SyntaxKind::Question),
                '.' if self.peek_next() == Some('.') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::DotDot
                }
                '.' => self.one(SyntaxKind::Dot),
                ':' if self.peek_next() == Some('-') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::ColonDash
                }
                ':' => self.one(SyntaxKind::Colon),
                '=' if self.peek_next() == Some('=') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::EqEq
                }
                '=' if self.peek_next() == Some('>') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::FatArrow
                }
                '=' => self.one(SyntaxKind::Eq),
                '!' if self.peek_next() == Some('=') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::BangEq
                }
                '!' => self.one(SyntaxKind::Bang),
                '<' if self.peek_next() == Some('=') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::LtEq
                }
                '<' => self.one(SyntaxKind::Lt),
                '>' if self.peek_next() == Some('=') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::GtEq
                }
                '>' => self.one(SyntaxKind::Gt),
                '&' if self.peek_next() == Some('&') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::AmpAmp
                }
                '|' if self.peek_next() == Some('|') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::PipePipe
                }
                '-' if self.peek_next() == Some('>') => {
                    self.bump();
                    self.bump();
                    SyntaxKind::Arrow
                }
                '+' => self.one(SyntaxKind::Plus),
                '-' => self.one(SyntaxKind::Minus),
                '*' => self.one(SyntaxKind::Star),
                '/' => self.one(SyntaxKind::Slash),
                '%' => self.one(SyntaxKind::Percent),
                _ => {
                    self.bump();
                    SyntaxKind::Error
                }
            };
            self.tokens.push(Token::new(kind, start, self.pos));
        }
        self.tokens.push(Token::new(
            SyntaxKind::Eof,
            self.source.len(),
            self.source.len(),
        ));
        self.tokens
    }

    fn one(&mut self, kind: SyntaxKind) -> SyntaxKind {
        self.bump();
        kind
    }

    fn lex_string(&mut self) -> SyntaxKind {
        self.bump();
        while let Some(ch) = self.peek() {
            match ch {
                '"' => {
                    self.bump();
                    break;
                }
                '\\' => {
                    self.bump();
                    if self.peek().is_some() {
                        self.bump();
                    }
                }
                _ => {
                    self.bump();
                }
            }
        }
        SyntaxKind::String
    }

    fn lex_number(&mut self) -> SyntaxKind {
        self.consume_while(|c| c.is_ascii_digit());
        if self.peek() == Some('.') && self.peek_next() != Some('.') {
            self.bump();
            self.consume_while(|c| c.is_ascii_digit());
            SyntaxKind::Float
        } else {
            SyntaxKind::Int
        }
    }

    fn lex_ident_or_keyword(&mut self, start: usize) -> SyntaxKind {
        self.consume_while(is_ident_continue);
        let text = &self.source[start..self.pos];
        keyword_kind(text).unwrap_or(SyntaxKind::Ident)
    }

    fn consume_newline(&mut self) {
        if self.peek() == Some('\r') {
            self.bump();
            if self.peek() == Some('\n') {
                self.bump();
            }
        } else {
            self.bump();
        }
    }

    fn consume_while(&mut self, mut pred: impl FnMut(char) -> bool) {
        while self.peek().is_some_and(&mut pred) {
            self.bump();
        }
    }

    fn peek(&self) -> Option<char> {
        self.source[self.pos..].chars().next()
    }

    fn peek_next(&self) -> Option<char> {
        let mut chars = self.source[self.pos..].chars();
        chars.next()?;
        chars.next()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }
}

fn is_ident_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn keyword_kind(text: &str) -> Option<SyntaxKind> {
    Some(match text {
        "let" => SyntaxKind::LetKw,
        "const" => SyntaxKind::ConstKw,
        "if" => SyntaxKind::IfKw,
        "elseif" => SyntaxKind::ElseIfKw,
        "else" => SyntaxKind::ElseKw,
        "end" => SyntaxKind::EndKw,
        "begin" => SyntaxKind::BeginKw,
        "for" => SyntaxKind::ForKw,
        "in" => SyntaxKind::InKw,
        "while" => SyntaxKind::WhileKw,
        "return" => SyntaxKind::ReturnKw,
        "break" => SyntaxKind::BreakKw,
        "continue" => SyntaxKind::ContinueKw,
        "try" => SyntaxKind::TryKw,
        "catch" => SyntaxKind::CatchKw,
        "finally" => SyntaxKind::FinallyKw,
        "fn" => SyntaxKind::FnKw,
        "method" => SyntaxKind::MethodKw,
        "verb" => SyntaxKind::VerbKw,
        "object" => SyntaxKind::ObjectKw,
        "extends" => SyntaxKind::ExtendsKw,
        "do" => SyntaxKind::DoKw,
        "transaction" => SyntaxKind::TransactionKw,
        "atomic" => SyntaxKind::AtomicKw,
        "assert" => SyntaxKind::AssertKw,
        "retract" => SyntaxKind::RetractKw,
        "require" => SyntaxKind::RequireKw,
        "true" => SyntaxKind::TrueKw,
        "false" => SyntaxKind::FalseKw,
        "nothing" => SyntaxKind::NothingKw,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::lex;
    use crate::SyntaxKind;

    #[test]
    fn lexes_modern_collection_tokens() {
        let kinds = lex("[1, @xs] {:lit -> true}")
            .into_iter()
            .map(|t| t.kind)
            .collect::<Vec<_>>();
        assert!(kinds.contains(&SyntaxKind::LBracket));
        assert!(kinds.contains(&SyntaxKind::At));
        assert!(kinds.contains(&SyntaxKind::LBrace));
        assert!(kinds.contains(&SyntaxKind::Arrow));
    }

    #[test]
    fn lexes_relation_rule_arrow() {
        let kinds = lex("VisibleTo(a, b) :- LocatedIn(a, room)")
            .into_iter()
            .map(|t| t.kind)
            .collect::<Vec<_>>();
        assert!(kinds.contains(&SyntaxKind::ColonDash));
    }
}
