use std::ops::Range;

pub type Span = Range<usize>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeId(pub u32);

impl NodeId {
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Ast {
    pub items: Vec<Item>,
    pub errors: Vec<crate::ParseError>,
    pub node_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Item {
    Expr {
        id: NodeId,
        expr: Expr,
    },
    RelationRule {
        id: NodeId,
        span: Span,
        head: Expr,
        body: Vec<Expr>,
    },
    Object {
        id: NodeId,
        span: Span,
        identity: Option<String>,
        extends: Option<String>,
        clauses: Vec<ObjectClause>,
    },
    Method {
        id: NodeId,
        span: Span,
        kind: MethodKind,
        identity: Option<String>,
        selector: Option<String>,
        clauses: Vec<String>,
        roles: Vec<MethodRole>,
        body: Vec<Item>,
    },
}

impl Item {
    pub fn id(&self) -> NodeId {
        match self {
            Self::Expr { id, .. }
            | Self::RelationRule { id, .. }
            | Self::Object { id, .. }
            | Self::Method { id, .. } => *id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MethodKind {
    Method,
    Verb,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MethodRole {
    pub name: String,
    pub restriction: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectClause {
    pub id: NodeId,
    pub span: Span,
    pub exprs: Vec<Expr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    Literal {
        id: NodeId,
        span: Span,
        value: Literal,
    },
    Name {
        id: NodeId,
        span: Span,
        name: String,
    },
    QueryVar {
        id: NodeId,
        span: Span,
        name: String,
    },
    Identity {
        id: NodeId,
        span: Span,
        name: String,
    },
    Symbol {
        id: NodeId,
        span: Span,
        name: String,
    },
    Hole {
        id: NodeId,
        span: Span,
    },
    List {
        id: NodeId,
        span: Span,
        items: Vec<CollectionItem>,
    },
    Map {
        id: NodeId,
        span: Span,
        entries: Vec<(Expr, Expr)>,
    },
    Unary {
        id: NodeId,
        span: Span,
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        id: NodeId,
        span: Span,
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Assign {
        id: NodeId,
        span: Span,
        target: Box<Expr>,
        value: Box<Expr>,
    },
    Call {
        id: NodeId,
        span: Span,
        callee: Box<Expr>,
        args: Vec<Arg>,
    },
    RoleCall {
        id: NodeId,
        span: Span,
        selector: Box<Expr>,
        args: Vec<Arg>,
    },
    ReceiverCall {
        id: NodeId,
        span: Span,
        receiver: Box<Expr>,
        selector: Box<Expr>,
        args: Vec<Arg>,
    },
    Index {
        id: NodeId,
        span: Span,
        collection: Box<Expr>,
        index: Option<Box<Expr>>,
    },
    Field {
        id: NodeId,
        span: Span,
        base: Box<Expr>,
        name: String,
    },
    Binding {
        id: NodeId,
        span: Span,
        kind: BindingKind,
        pattern: BindingPattern,
        value: Option<Box<Expr>>,
    },
    If {
        id: NodeId,
        span: Span,
        condition: Box<Expr>,
        then_items: Vec<Item>,
        elseif: Vec<(Expr, Vec<Item>)>,
        else_items: Vec<Item>,
    },
    Block {
        id: NodeId,
        span: Span,
        items: Vec<Item>,
    },
    For {
        id: NodeId,
        span: Span,
        key: String,
        value: Option<String>,
        iter: Box<Expr>,
        body: Vec<Item>,
    },
    While {
        id: NodeId,
        span: Span,
        condition: Box<Expr>,
        body: Vec<Item>,
    },
    Return {
        id: NodeId,
        span: Span,
        value: Option<Box<Expr>>,
    },
    Raise {
        id: NodeId,
        span: Span,
        error: Box<Expr>,
        message: Option<Box<Expr>>,
        value: Option<Box<Expr>>,
    },
    Recover {
        id: NodeId,
        span: Span,
        expr: Box<Expr>,
        catches: Vec<RecoveryClause>,
    },
    One {
        id: NodeId,
        span: Span,
        expr: Box<Expr>,
    },
    Break {
        id: NodeId,
        span: Span,
    },
    Continue {
        id: NodeId,
        span: Span,
    },
    Try {
        id: NodeId,
        span: Span,
        body: Vec<Item>,
        catches: Vec<CatchClause>,
        finally: Vec<Item>,
    },
    Function {
        id: NodeId,
        span: Span,
        name: Option<String>,
        params: Vec<Param>,
        body: FunctionBody,
    },
    Effect {
        id: NodeId,
        span: Span,
        kind: EffectKind,
        expr: Box<Expr>,
    },
    Error {
        id: NodeId,
        span: Span,
    },
}

impl Expr {
    pub fn id(&self) -> NodeId {
        match self {
            Self::Literal { id, .. }
            | Self::Name { id, .. }
            | Self::QueryVar { id, .. }
            | Self::Identity { id, .. }
            | Self::Symbol { id, .. }
            | Self::Hole { id, .. }
            | Self::List { id, .. }
            | Self::Map { id, .. }
            | Self::Unary { id, .. }
            | Self::Binary { id, .. }
            | Self::Assign { id, .. }
            | Self::Call { id, .. }
            | Self::RoleCall { id, .. }
            | Self::ReceiverCall { id, .. }
            | Self::Index { id, .. }
            | Self::Field { id, .. }
            | Self::Binding { id, .. }
            | Self::If { id, .. }
            | Self::Block { id, .. }
            | Self::For { id, .. }
            | Self::While { id, .. }
            | Self::Return { id, .. }
            | Self::Raise { id, .. }
            | Self::Recover { id, .. }
            | Self::One { id, .. }
            | Self::Break { id, .. }
            | Self::Continue { id, .. }
            | Self::Try { id, .. }
            | Self::Function { id, .. }
            | Self::Effect { id, .. }
            | Self::Error { id, .. } => *id,
        }
    }

    pub fn span(&self) -> &Span {
        match self {
            Self::Literal { span, .. }
            | Self::Name { span, .. }
            | Self::QueryVar { span, .. }
            | Self::Identity { span, .. }
            | Self::Symbol { span, .. }
            | Self::Hole { span, .. }
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
            | Self::Raise { span, .. }
            | Self::Recover { span, .. }
            | Self::One { span, .. }
            | Self::Break { span, .. }
            | Self::Continue { span, .. }
            | Self::Try { span, .. }
            | Self::Function { span, .. }
            | Self::Effect { span, .. }
            | Self::Error { span, .. } => span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Literal {
    Int(String),
    Float(String),
    String(String),
    Bool(bool),
    ErrorCode(String),
    Nothing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionItem {
    Expr(Expr),
    Splice(Expr),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Arg {
    pub id: NodeId,
    pub role: Option<String>,
    pub splice: bool,
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
    pub id: NodeId,
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
    pub id: NodeId,
    pub name: Option<String>,
    pub condition: Option<Expr>,
    pub body: Vec<Item>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryClause {
    pub id: NodeId,
    pub name: Option<String>,
    pub condition: Option<Expr>,
    pub value: Expr,
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
