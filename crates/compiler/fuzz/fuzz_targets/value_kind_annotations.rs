#![no_main]

use libfuzzer_sys::fuzz_target;
use mica_compiler::{CstElement, CstNode, lex, parse, parse_semantic};

const VALUE_KINDS: [&str; 16] = [
    "bool",
    "int",
    "float",
    "identity",
    "string",
    "bytes",
    "symbol",
    "error_code",
    "error",
    "capability",
    "frob",
    "function",
    "list",
    "map",
    "range",
    "relation",
];

fuzz_target!(|data: &[u8]| {
    if let Ok(source) = std::str::from_utf8(data) {
        exercise_source(source);
    }
    let Some((&kind_selector, rest)) = data.split_first() else {
        return;
    };
    let kind = VALUE_KINDS[kind_selector as usize % VALUE_KINDS.len()];
    let shape = rest.first().copied().unwrap_or_default() % 5;
    let source = match shape {
        0 => format!("let value: {kind} = dynamic()"),
        1 => format!("fn typed(value: {kind}) -> {kind} => value"),
        2 => format!("for value: {kind} in dynamic()\nend"),
        3 => format!("let [value: {kind}] = dynamic()"),
        4 => format!("verb typed(value: {kind}) -> {kind}\n  return value\nend"),
        _ => unreachable!(),
    };
    exercise_source(&source);
});

fn exercise_source(source: &str) {
    let mut reconstructed = String::with_capacity(source.len());
    for token in lex(source) {
        assert_span(&token.span, source.len());
        reconstructed.push_str(&source[token.span]);
    }
    assert_eq!(reconstructed, source);

    let parsed = parse(source);
    assert_node_spans(&parsed.root, source.len());
    for error in &parsed.errors {
        assert_span(&error.span, source.len());
    }

    let semantic = parse_semantic(source);
    for error in &semantic.parse_errors {
        assert_span(&error.span, source.len());
    }
    for diagnostic in &semantic.diagnostics {
        assert_span(&diagnostic.span, source.len());
    }
}

fn assert_node_spans(node: &CstNode, source_len: usize) {
    assert_span(&node.span, source_len);
    for child in &node.children {
        match child {
            CstElement::Node(node) => assert_node_spans(node, source_len),
            CstElement::Token(token) => assert_span(&token.span, source_len),
        }
    }
}

fn assert_span(span: &std::ops::Range<usize>, source_len: usize) {
    assert!(span.start <= span.end);
    assert!(span.end <= source_len);
}
