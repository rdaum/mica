use mica_var::{OrderedKeySink, Value};
use rart::{OverflowKey, OverflowKeyBuilder};

pub(crate) type RadixTupleKey = OverflowKey<64, 16>;

pub(crate) struct RadixTupleKeyBuilder(OverflowKeyBuilder<64, 16>);

impl RadixTupleKeyBuilder {
    fn new() -> Self {
        Self(RadixTupleKey::builder())
    }

    fn finish(self) -> RadixTupleKey {
        self.0.finish()
    }
}

impl OrderedKeySink for RadixTupleKeyBuilder {
    fn push_byte(&mut self, byte: u8) {
        self.0.push(byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

pub(crate) fn key_from_values<'a>(values: impl IntoIterator<Item = &'a Value>) -> RadixTupleKey {
    let mut key = RadixTupleKeyBuilder::new();
    for value in values {
        value.encode_ordered_into(&mut key);
    }
    key.finish()
}
