use crate::value::{INT_MAX, INT_MIN};
use crate::{Identity, Symbol, SymbolMetadata, Value, ValueKind};
use std::mem::{align_of, size_of};

#[test]
fn value_is_one_word() {
    assert_eq!(size_of::<Value>(), 8);
    assert_eq!(align_of::<Value>(), 8);
}

#[test]
fn immediate_constructors_round_trip() {
    assert_eq!(Value::nothing().kind(), ValueKind::Nothing);
    assert_eq!(Value::bool(true).as_bool(), Some(true));
    assert_eq!(Value::bool(false).as_bool(), Some(false));
    assert_eq!(Value::int(INT_MIN).unwrap().as_int(), Some(INT_MIN));
    assert_eq!(Value::int(INT_MAX).unwrap().as_int(), Some(INT_MAX));
    assert!(Value::int(INT_MIN - 1).is_err());
    assert!(Value::int(INT_MAX + 1).is_err());

    let id = Identity::new(0x00ab_cdef).unwrap();
    assert_eq!(Value::identity(id).as_identity(), Some(id));

    let symbol = Symbol::intern("take");
    assert_eq!(Value::symbol(symbol).as_symbol(), Some(symbol));
    assert_eq!(symbol.name(), Some("take"));
    assert_eq!(
        symbol.metadata(),
        Some(SymbolMetadata {
            byte_len: 4,
            char_len: 4,
            is_ascii: true,
        })
    );
    assert_eq!(Symbol::intern("take"), symbol);
    assert_ne!(Symbol::intern("TAKE"), symbol);

    let error_code = Symbol::intern("E_NOT_PORTABLE");
    assert_eq!(Value::error_code(error_code).kind(), ValueKind::ErrorCode);
    assert_eq!(
        Value::error_code(error_code).as_error_code(),
        Some(error_code)
    );
    assert_ne!(Value::error_code(error_code), Value::symbol(error_code));
    assert_eq!(
        format!("{}", Value::error_code(error_code)),
        "E_NOT_PORTABLE"
    );

    let error = Value::error(
        error_code,
        Some("That cannot be taken."),
        Some(Value::symbol(Symbol::intern("lamp"))),
    );
    assert_eq!(error.kind(), ValueKind::Error);
    assert_eq!(error.error_code_symbol(), Some(error_code));
    assert_eq!(
        error.with_error(|error| (
            error.code(),
            error.message().map(str::to_string),
            error.value().cloned(),
        )),
        Some((
            error_code,
            Some("That cannot be taken.".to_string()),
            Some(Value::symbol(Symbol::intern("lamp"))),
        ))
    );
    assert_eq!(
        format!("{error:?}"),
        "error(E_NOT_PORTABLE, \"That cannot be taken.\", :lamp)"
    );
}

#[test]
fn symbols_intern_consistently_across_threads() {
    let handles = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                for _ in 0..256 {
                    assert_eq!(Symbol::intern("look"), Symbol::intern("look"));
                }
                Symbol::intern("look")
            })
        })
        .collect::<Vec<_>>();

    let expected = Symbol::intern("look");
    for handle in handles {
        assert_eq!(handle.join().unwrap(), expected);
    }
}

#[test]
fn float_is_reduced_precision_and_canonicalizes_zero() {
    let value = Value::float(1.25);
    assert_eq!(value.as_float(), Some(1.25));
    assert_eq!(Value::float(-0.0), Value::float(0.0));
}

#[test]
fn numeric_operations_preserve_ints_when_exact() {
    let six = Value::int(6).unwrap();
    let three = Value::int(3).unwrap();
    let four = Value::int(4).unwrap();

    assert_eq!(
        six.checked_add(&three).and_then(|value| value.as_int()),
        Some(9)
    );
    assert_eq!(
        six.checked_sub(&three).and_then(|value| value.as_int()),
        Some(3)
    );
    assert_eq!(
        six.checked_mul(&three).and_then(|value| value.as_int()),
        Some(18)
    );
    assert_eq!(
        six.checked_div(&three).and_then(|value| value.as_int()),
        Some(2)
    );
    assert_eq!(
        six.checked_rem(&four).and_then(|value| value.as_int()),
        Some(2)
    );
    assert_eq!(
        three.checked_neg().and_then(|value| value.as_int()),
        Some(-3)
    );
}

#[test]
fn numeric_operations_fall_back_to_floats() {
    let five = Value::int(5).unwrap();
    let two = Value::int(2).unwrap();
    let half = Value::float(0.5);

    assert_eq!(
        five.checked_div(&two).and_then(|value| value.as_float()),
        Some(2.5)
    );
    assert_eq!(
        five.checked_add(&half).and_then(|value| value.as_float()),
        Some(5.5)
    );
    assert_eq!(five.checked_div(&Value::int(0).unwrap()), None);
}

#[test]
fn string_bytes_list_map_and_range_are_values() {
    let string = Value::string("brass lamp");
    assert_eq!(
        string.with_str(|s| s.to_string()),
        Some("brass lamp".to_string())
    );

    let bytes = Value::bytes([0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(
        bytes.with_bytes(|value| value.to_vec()),
        Some(vec![0xde, 0xad, 0xbe, 0xef])
    );
    assert_eq!(format!("{bytes:?}"), "#bytes(\"\\xde\\xad\\xbe\\xef\")");

    let list = Value::list([Value::int(1).unwrap(), Value::int(2).unwrap()]);
    assert_eq!(list.with_list(|values| values.len()), Some(2));

    let k = Value::symbol(Symbol::intern("color"));
    let red = Value::string("red");
    let blue = Value::string("blue");
    let map = Value::map([(k.clone(), red), (k.clone(), blue)]);
    assert_eq!(map.with_map(|entries| entries.len()), Some(1));
    assert_eq!(
        map.with_map(|entries| entries[0].1.with_str(|s| s.to_string()).unwrap()),
        Some("blue".to_string())
    );
    assert_eq!(map.map_len(), Some(1));
    assert_eq!(
        map.map_get(&k)
            .and_then(|value| value.with_str(str::to_string)),
        Some("blue".to_string())
    );

    let range = Value::range(Value::int(1).unwrap(), Some(Value::int(3).unwrap()));
    assert_eq!(
        range.with_range(|start, end| (start.as_int(), end.and_then(Value::as_int))),
        Some((Some(1), Some(3)))
    );
    assert_eq!(format!("{range}"), "1..3");

    let open_range = Value::range(Value::int(2).unwrap(), None);
    assert_eq!(format!("{open_range}"), "2..$");
}

#[test]
fn heap_values_are_arc_shared_and_acyclic() {
    let value = Value::list([
        Value::string("alpha"),
        Value::symbol(Symbol::intern("beta")),
        Value::identity_raw(42).unwrap(),
    ]);
    assert_eq!(value.heap_strong_count(), Some(1));
    let cloned = value.clone();
    assert_eq!(value.heap_strong_count(), Some(2));
    assert_eq!(value, cloned);
    drop(value);
    assert_eq!(cloned.heap_strong_count(), Some(1));
    assert_eq!(cloned.list_len(), Some(3));
    assert_eq!(
        cloned
            .list_get(0)
            .and_then(|value| value.with_str(str::to_string)),
        Some("alpha".to_string())
    );
}

#[test]
fn list_slices_return_new_list_values() {
    let list = Value::list([
        Value::int(10).unwrap(),
        Value::int(20).unwrap(),
        Value::int(30).unwrap(),
        Value::int(40).unwrap(),
    ]);

    let slice = list.list_slice(1, 3).unwrap();
    assert_eq!(slice.list_len(), Some(2));
    assert_eq!(slice.list_get(0).and_then(|value| value.as_int()), Some(20));
    assert_eq!(slice.list_get(1).and_then(|value| value.as_int()), Some(30));
    assert!(list.list_slice(3, 5).is_none());
}

#[test]
fn indexed_updates_return_new_collection_values() {
    let list = Value::list([
        Value::int(1).unwrap(),
        Value::int(2).unwrap(),
        Value::int(3).unwrap(),
    ]);
    let updated = list
        .index_set(&Value::int(1).unwrap(), Value::int(20).unwrap())
        .unwrap();
    assert_eq!(list.list_get(1).and_then(|value| value.as_int()), Some(2));
    assert_eq!(
        updated.list_get(1).and_then(|value| value.as_int()),
        Some(20)
    );
    assert!(
        list.index_set(&Value::int(10).unwrap(), Value::int(20).unwrap())
            .is_none()
    );

    let key = Value::symbol(Symbol::intern("count"));
    let map = Value::map([(key.clone(), Value::int(1).unwrap())]);
    let replaced = map.index_set(&key, Value::int(2).unwrap()).unwrap();
    assert_eq!(map.map_get(&key).and_then(|value| value.as_int()), Some(1));
    assert_eq!(
        replaced.map_get(&key).and_then(|value| value.as_int()),
        Some(2)
    );

    let inserted_key = Value::symbol(Symbol::intern("other"));
    let inserted = replaced
        .index_set(&inserted_key, Value::int(3).unwrap())
        .unwrap();
    assert_eq!(
        inserted
            .map_get(&inserted_key)
            .and_then(|value| value.as_int()),
        Some(3)
    );
}

#[test]
fn total_order_is_stable() {
    let values = vec![
        Value::map([]),
        Value::error(Symbol::intern("E_TEST"), None::<Box<str>>, None),
        Value::range(Value::int(1).unwrap(), None),
        Value::list([]),
        Value::bytes([1, 2, 3]),
        Value::string("x"),
        Value::error_code(Symbol::intern("E_NOT_PORTABLE")),
        Value::symbol(Symbol::intern("x")),
        Value::identity_raw(1).unwrap(),
        Value::float(1.0),
        Value::int(1).unwrap(),
        Value::bool(true),
        Value::nothing(),
    ];
    let mut sorted = values.clone();
    sorted.sort();
    for pair in sorted.windows(2) {
        assert!(pair[0] <= pair[1]);
    }
    assert_eq!(sorted.first(), Some(&Value::nothing()));
    assert_eq!(sorted.last().unwrap().kind(), ValueKind::Error);
}

#[test]
fn ordered_encoding_preserves_string_order() {
    let a = Value::string("a").ordered_key_bytes();
    let aa = Value::string("aa").ordered_key_bytes();
    let b = Value::string("b").ordered_key_bytes();
    assert!(a < aa);
    assert!(aa < b);
}

#[test]
fn ordered_encoding_preserves_bytes_order() {
    let a = Value::bytes([0x00]).ordered_key_bytes();
    let aa = Value::bytes([0x00, 0x00]).ordered_key_bytes();
    let b = Value::bytes([0x01]).ordered_key_bytes();
    assert!(a < aa);
    assert!(aa < b);
}

#[test]
fn arithmetic_fast_path() {
    assert_eq!(
        Value::int(41).unwrap().checked_add(&Value::int(1).unwrap()),
        Some(Value::int(42).unwrap())
    );
    assert_eq!(
        Value::float(1.5)
            .checked_add(&Value::int(2).unwrap())
            .unwrap()
            .as_float(),
        Some(3.5)
    );
}
