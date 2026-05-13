use crate::value::{TAG_BYTES, TAG_LIST, TAG_MAP, TAG_STRING, Value};

pub(crate) enum HeapValue {
    String(Box<str>),
    Bytes(Box<[u8]>),
    List(Box<[Value]>),
    Map(Box<[(Value, Value)]>),
}

impl HeapValue {
    pub(crate) fn tag(&self) -> u8 {
        match self {
            Self::String(_) => TAG_STRING,
            Self::Bytes(_) => TAG_BYTES,
            Self::List(_) => TAG_LIST,
            Self::Map(_) => TAG_MAP,
        }
    }
}
