use std::ops::Range;

pub type Span = Range<usize>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Ast {
    pub items: Vec<Item>,
    pub errors: Vec<crate::ParseError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Item {
    Expr(Expr),
    RelationRule {
        span: Span,
        head: Expr,
        body: Vec<Expr>,
    },
    Object {
        span: Span,
        identity: Option<String>,
        extends: Option<String>,
        clauses: Vec<ObjectClause>,
    },
    Method {
        span: Span,
        kind: MethodKind,
        identity: Option<String>,
        selector: Option<String>,
        clauses: Vec<String>,
        body: Vec<Item>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MethodKind {
    Method,
    Verb,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectClause {
    pub span: Span,
    pub exprs: Vec<Expr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    Literal {
        span: Span,
        value: Literal,
    },
    Name {
        span: Span,
        name: String,
    },
    Identity {
        span: Span,
        name: String,
    },
    Symbol {
        span: Span,
        name: String,
    },
    Hole {
        span: Span,
    },
    List {
        span: Span,
        items: Vec<CollectionItem>,
    },
    Map {
        span: Span,
        entries: Vec<(Expr, Expr)>,
    },
    Unary {
        span: Span,
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        span: Span,
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Assign {
        span: Span,
        target: Box<Expr>,
        value: Box<Expr>,
    },
    Call {
        span: Span,
        callee: Box<Expr>,
        args: Vec<Arg>,
    },
    RoleCall {
        span: Span,
        selector: Box<Expr>,
        args: Vec<Arg>,
    },
    ReceiverCall {
        span: Span,
        receiver: Box<Expr>,
        selector: Box<Expr>,
        args: Vec<Arg>,
    },
    Index {
        span: Span,
        collection: Box<Expr>,
        index: Option<Box<Expr>>,
    },
    Field {
        span: Span,
        base: Box<Expr>,
        name: String,
    },
    Binding {
        span: Span,
        kind: BindingKind,
        pattern: BindingPattern,
        value: Option<Box<Expr>>,
    },
    If {
        span: Span,
        condition: Box<Expr>,
        then_items: Vec<Item>,
        elseif: Vec<(Expr, Vec<Item>)>,
        else_items: Vec<Item>,
    },
    Block {
        span: Span,
        items: Vec<Item>,
    },
    For {
        span: Span,
        key: String,
        value: Option<String>,
        iter: Box<Expr>,
        body: Vec<Item>,
    },
    While {
        span: Span,
        condition: Box<Expr>,
        body: Vec<Item>,
    },
    Return {
        span: Span,
        value: Option<Box<Expr>>,
    },
    Break {
        span: Span,
    },
    Continue {
        span: Span,
    },
    Try {
        span: Span,
        body: Vec<Item>,
        catches: Vec<CatchClause>,
        finally: Vec<Item>,
    },
    Function {
        span: Span,
        name: Option<String>,
        params: Vec<Param>,
        body: FunctionBody,
    },
    Effect {
        span: Span,
        kind: EffectKind,
        expr: Box<Expr>,
    },
    Error {
        span: Span,
    },
}

impl Expr {
    pub fn span(&self) -> &Span {
        match self {
            Self::Literal { span, .. }
            | Self::Name { span, .. }
            | Self::Identity { span, .. }
            | Self::Symbol { span, .. }
            | Self::Hole { span }
            | Self::List { span, .. }
            | Self::Map { span, .. }
            | Self::Unary { span, .. }
            | Self::Binary { span, .. }
            | Self::Assign { span, .. }
            | Self::Call { span, .. }
            | Self::RoleCall { span, .. }
            | Self::ReceiverCall { span, .. }
            | Self::Index { span, .. }
            | Self::Field { span, .. }
            | Self::Binding { span, .. }
            | Self::If { span, .. }
            | Self::Block { span, .. }
            | Self::For { span, .. }
            | Self::While { span, .. }
            | Self::Return { span, .. }
            | Self::Break { span }
            | Self::Continue { span }
            | Self::Try { span, .. }
            | Self::Function { span, .. }
            | Self::Effect { span, .. }
            | Self::Error { span } => span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Literal {
    Int(String),
    Float(String),
    String(String),
    Bool(bool),
    Nothing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionItem {
    Expr(Expr),
    Splice(Expr),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Arg {
    pub role: Option<String>,
    pub value: Expr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindingKind {
    Let,
    Const,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindingPattern {
    Name(String),
    Scatter(Vec<Param>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Param {
    pub name: String,
    pub mode: ParamMode,
    pub default: Option<Expr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParamMode {
    Required,
    Optional,
    Rest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionBody {
    Expr(Box<Expr>),
    Block(Vec<Item>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatchClause {
    pub name: Option<String>,
    pub condition: Option<Expr>,
    pub body: Vec<Item>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EffectKind {
    Assert,
    Retract,
    Require,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    And,
    Or,
    Range,
}
