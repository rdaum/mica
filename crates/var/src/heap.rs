use crate::value::{TAG_LIST, TAG_MAP, TAG_STRING, Value};

pub(crate) enum HeapValue {
    String(Box<str>),
    List(Box<[Value]>),
    Map(Box<[(Value, Value)]>),
}

impl HeapValue {
    pub(crate) fn tag(&self) -> u8 {
        match self {
            Self::String(_) => TAG_STRING,
            Self::List(_) => TAG_LIST,
            Self::Map(_) => TAG_MAP,
        }
    }
}
