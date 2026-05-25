#[cfg(test)]
mod tests {
    use mica_runtime::{SourceRunner, TaskOutcome};
    use mica_source_provider::{build_source_index_file, write_failed_source_index_file};
    use mica_var::{Symbol, Value};
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    fn load_source_relations(runner: &mut SourceRunner) {
        let root = env::current_dir().unwrap().display().to_string();
        load_source_relations_at(runner, &root);
    }

    fn load_source_relations_at(runner: &mut SourceRunner, root: &str) {
        runner
            .run_source(&format!(
                "make_identity(:repo)\n\
                 make_identity(:rev)\n\
                 make_relation(:source/RepositoryRoot, 2)\n\
                 make_relation(:source/RevisionOf, 2)\n\
                 make_relation(:source/RepositoryEntry, 6)\n\
                 make_relation(:source/FileText, 5)\n\
                 make_relation(:source/FileLines, 7)\n\
                 make_relation(:source/FileLineCount, 4)\n\
                 make_relation(:source/FileContentHash, 4)\n\
                 make_relation(:source/SyntaxLine, 8)\n\
                 make_relation(:source/SyntaxOutline, 10)\n\
                 make_relation(:source/SyntaxNodeAt, 11)\n\
                 make_relation(:source/DefinitionAt, 13)\n\
                 make_relation(:source/ReferencesOf, 10)\n\
                 make_relation(:source/SymbolSearch, 11)\n\
                 make_relation(:source/IndexedTextUnit, 9)\n\
                 make_relation(:source/TextSearch, 11)\n\
                 make_relation(:source/SourceIndex, 1)\n\
                 make_relation(:source/IndexRepository, 2)\n\
                 make_relation(:source/IndexRevision, 2)\n\
                 make_relation(:source/IndexProvider, 2)\n\
                 make_relation(:source/IndexStatus, 2)\n\
                 make_relation(:source/IndexVersion, 2)\n\
                 make_relation(:source/IndexBuildError, 2)\n\
                 make_relation(:source/Repository, 1)\n\
                 make_relation(:source/Revision, 1)\n\
                 assert source/Repository(#repo)\n\
                 assert source/Revision(#rev)\n\
                 assert source/RepositoryRoot(#repo, {root:?})\n\
                 assert source/RevisionOf(#rev, #repo)"
            ))
            .unwrap();
    }

    fn with_source_provider_env<T>(f: impl FnOnce() -> T) -> T {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f()
    }

    fn rust_analyzer_available() -> bool {
        std::process::Command::new("rust-analyzer")
            .arg("--version")
            .output()
            .is_ok()
    }

    fn source_provider_root() -> PathBuf {
        env::current_dir()
            .unwrap()
            .parent()
            .unwrap()
            .join("source-provider")
    }

    fn with_source_root_env<T>(source_root: &Path, f: impl FnOnce() -> T) -> T {
        with_source_provider_env(|| {
            let old_source_root = env::var_os("MICA_SOURCE_ROOT");
            unsafe {
                env::set_var("MICA_SOURCE_ROOT", source_root);
            }
            let result = f();
            unsafe {
                if let Some(old_source_root) = old_source_root {
                    env::set_var("MICA_SOURCE_ROOT", old_source_root);
                } else {
                    env::remove_var("MICA_SOURCE_ROOT");
                }
            }
            result
        })
    }

    fn with_source_index_env<T>(
        index_path: &Path,
        rust_analyzer: Option<&Path>,
        f: impl FnOnce() -> T,
    ) -> T {
        with_source_provider_env(|| {
            let old_index = env::var_os("MICA_SOURCE_INDEX");
            let old_rust_analyzer = env::var_os("MICA_RUST_ANALYZER");
            unsafe {
                env::set_var("MICA_SOURCE_INDEX", index_path);
                if let Some(rust_analyzer) = rust_analyzer {
                    env::set_var("MICA_RUST_ANALYZER", rust_analyzer);
                }
            }
            let result = f();
            unsafe {
                if let Some(old_index) = old_index {
                    env::set_var("MICA_SOURCE_INDEX", old_index);
                } else {
                    env::remove_var("MICA_SOURCE_INDEX");
                }
                if let Some(old_rust_analyzer) = old_rust_analyzer {
                    env::set_var("MICA_RUST_ANALYZER", old_rust_analyzer);
                } else {
                    env::remove_var("MICA_RUST_ANALYZER");
                }
            }
            result
        })
    }

    fn load_source_app(runner: &mut SourceRunner) {
        for filein in [
            include_str!("../../../apps/shared/sync-host.mica"),
            include_str!("../../../apps/shared/sync-dom.mica"),
            include_str!("../../../apps/shared/retrieval.mica"),
            include_str!("../../../apps/shared/openai.mica"),
            include_str!("../../../apps/source/core.mica"),
            include_str!("../../../apps/source/retrieval.mica"),
            include_str!("../../../apps/source/ui-session.mica"),
            include_str!("../../../apps/source/ui-policy.mica"),
            include_str!("../../../apps/source/ui-state.mica"),
            include_str!("../../../apps/source/ui-actions.mica"),
            include_str!("../../../apps/source/ui-sync.mica"),
            include_str!("../../../apps/source/ui-compose.mica"),
            include_str!("../../../apps/source/ui-navigator.mica"),
            include_str!("../../../apps/source/ui-retrieval-panel.mica"),
            include_str!("../../../apps/source/ui-agent-panel.mica"),
            include_str!("../../../apps/source/ui-code-panel.mica"),
            include_str!("../../../apps/source/http.mica"),
        ] {
            runner.run_filein(filein).unwrap();
        }
    }

    fn with_source_index_and_root_env<T>(
        index_path: &Path,
        source_root: &Path,
        rust_analyzer: Option<&Path>,
        f: impl FnOnce() -> T,
    ) -> T {
        with_source_provider_env(|| {
            let old_index = env::var_os("MICA_SOURCE_INDEX");
            let old_rust_analyzer = env::var_os("MICA_RUST_ANALYZER");
            let old_source_root = env::var_os("MICA_SOURCE_ROOT");
            unsafe {
                env::set_var("MICA_SOURCE_INDEX", index_path);
                env::set_var("MICA_SOURCE_ROOT", source_root);
                if let Some(rust_analyzer) = rust_analyzer {
                    env::set_var("MICA_RUST_ANALYZER", rust_analyzer);
                }
            }
            let result = f();
            unsafe {
                if let Some(old_index) = old_index {
                    env::set_var("MICA_SOURCE_INDEX", old_index);
                } else {
                    env::remove_var("MICA_SOURCE_INDEX");
                }
                if let Some(old_rust_analyzer) = old_rust_analyzer {
                    env::set_var("MICA_RUST_ANALYZER", old_rust_analyzer);
                } else {
                    env::remove_var("MICA_RUST_ANALYZER");
                }
                if let Some(old_source_root) = old_source_root {
                    env::set_var("MICA_SOURCE_ROOT", old_source_root);
                } else {
                    env::remove_var("MICA_SOURCE_ROOT");
                }
            }
            result
        })
    }

    fn temp_index_path(name: &str) -> PathBuf {
        env::temp_dir().join(format!(
            "mica-source-index-{name}-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ))
    }

    #[test]
    fn source_provider_reads_file_text_from_allowed_root() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "let text = one source/FileText(#repo, #rev, \"Cargo.toml\", ?text, ?hash)\n\
                 return string_contains(text[:text], \"[package]\")",
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
    }

    #[test]
    fn source_provider_reads_line_windows() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "let row = one source/FileLines(#repo, #rev, \"Cargo.toml\", 1, 2, ?lines, ?hash)\n\
                 return row[:lines]",
            )
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!("expected complete outcome, got {:?}", report.outcome);
        };
        value
            .with_list(|values| {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Value::string("[package]"));
            })
            .expect("expected line list");
    }

    #[test]
    fn source_provider_counts_file_lines() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "let line_count = one source/FileLineCount(#repo, #rev, \"Cargo.toml\", ?line_count)\n\
                 return line_count",
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value.as_int().is_some_and(|count| count > 1)
        ));
    }

    #[test]
    fn source_provider_lists_repository_entries() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "for entry in source/RepositoryEntry(#repo, #rev, \"\", ?path, ?kind, ?name)\n\
                   if entry[:path] == \"Cargo.toml\"\n\
                     return entry[:kind]\n\
                   end\n\
                 end\n\
                 return nothing",
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("file")
        ));
    }

    #[test]
    fn source_provider_rejects_escaping_paths() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let error = runner
            .run_source("return source/FileContentHash(#repo, #rev, \"../Cargo.toml\", ?hash)")
            .unwrap_err();
        assert!(format!("{error:?}").contains("parent components"));
    }

    #[test]
    fn source_provider_requires_bound_path() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let error = runner
            .run_source("return source/FileText(#repo, #rev, ?path, ?text, ?hash)")
            .unwrap_err();
        assert!(format!("{error:?}").contains("MissingRequiredBindings"));
    }

    #[test]
    fn syntax_provider_requires_constrained_queries() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let error = runner
            .run_source(
                "return source/SyntaxOutline(#repo, #rev, ?path, ?node, ?kind, ?name, ?start_line, ?end_line, ?start_byte, ?end_byte)",
            )
            .unwrap_err();
        assert!(format!("{error:?}").contains("MissingRequiredBindings"));

        let error = runner
            .run_source(
                "return source/SyntaxNodeAt(#repo, #rev, \"src/lib.rs\", ?offset, ?node, ?kind, ?name, ?start_line, ?end_line, ?start_byte, ?end_byte)",
            )
            .unwrap_err();
        assert!(format!("{error:?}").contains("MissingRequiredBindings"));
    }

    #[test]
    fn source_provider_relations_are_read_only() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let repo = runner.named_identity(Symbol::intern("repo")).unwrap();
        let error = runner
            .run_source("assert source/FileContentHash(#repo, #rev, \"Cargo.toml\", \"x\")")
            .unwrap_err();
        assert!(format!("{error:?}").contains("ReadOnlyRelation"));
        assert!(
            format!("{error:?}").contains(&format!("{repo:?}"))
                || format!("{error:?}").contains("ReadOnlyRelation")
        );
    }

    #[test]
    fn source_provider_returns_rust_syntax_outline() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "for item in source/SyntaxOutline(#repo, #rev, \"src/lib.rs\", ?node, ?kind, ?name, ?start_line, ?end_line, ?start_byte, ?end_byte)\n\
                   return item[:kind] != nothing\n\
                 end\n\
                 return false",
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
    }

    #[test]
    fn source_provider_returns_line_level_syntax_segments() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "let row = one source/SyntaxLine(#repo, #rev, \"src/lib.rs\", 1, 8, 1, ?segments, ?hash)\n\
                 return row[:segments]",
            )
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!("expected complete outcome, got {:?}", report.outcome);
        };
        value
            .with_list(|segments| {
                assert!(!segments.is_empty());
                assert!(segments.iter().any(|segment| {
                    segment
                        .with_map(|entries| {
                            entries
                                .iter()
                                .any(|(key, _)| key == &Value::symbol(Symbol::intern("kind")))
                        })
                        .unwrap_or(false)
                }));
            })
            .expect("expected syntax segment list");
    }

    #[test]
    fn source_provider_reports_nearest_syntax_node() {
        let mut runner = SourceRunner::new_empty();
        load_source_relations(&mut runner);

        let report = runner
            .run_source(
                "let item = one source/SyntaxNodeAt(#repo, #rev, \"src/lib.rs\", 2500, ?node, ?kind, ?name, ?start_line, ?end_line, ?start_byte, ?end_byte)\n\
                 return item[:node] != nothing",
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
    }

    #[test]
    fn rust_analyzer_provider_returns_definition_and_references() {
        if !rust_analyzer_available() {
            return;
        }

        with_source_provider_env(|| {
            let mut runner = SourceRunner::new_empty();
            load_source_relations(&mut runner);
            let source = fs::read_to_string("src/retrieval.rs").unwrap();
            let offset = source.find("parse_vector(").unwrap();

            let report = runner
            .run_source(&format!(
                "for def in source/DefinitionAt(#repo, #rev, \"src/retrieval.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                   if def[:target_path] == \"src/retrieval.rs\"\n\
                     return def\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            if value == Value::nothing() {
                return;
            }
            let symbol = value
                .with_map(|entries| {
                    entries
                        .iter()
                        .find(|(key, _)| key == &Value::symbol(Symbol::intern("symbol")))
                        .map(|(_, value)| value.clone())
                })
                .flatten()
                .and_then(|value| value.with_str(str::to_owned))
                .expect("expected definition symbol");

            let report = runner
            .run_source(&format!(
                "for reference in source/ReferencesOf(#repo, #rev, {symbol:?}, ?path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider, ?name)\n\
                   if reference[:path] == \"src/retrieval.rs\"\n\
                     return reference[:provider]\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.with_str(|provider| provider.contains("rust-analyzer")).unwrap_or(false)
            ));
        });
    }

    #[test]
    fn persistent_source_index_answers_navigation_without_rust_analyzer() {
        let index_path = temp_index_path("navigation");
        let root = source_provider_root();
        build_source_index_file(&root, &index_path).unwrap();
        with_source_index_and_root_env(&index_path, &root, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            load_source_relations_at(&mut runner, &root.display().to_string());
            let source = fs::read_to_string(root.join("src/relations.rs")).unwrap();
            let offset = source.find("LocalSourceProvider::from_env").unwrap();

            let report = runner
                .run_source(&format!(
                    "for def in source/DefinitionAt(#repo, #rev, \"src/relations.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                       if def[:name] == \"LocalSourceProvider\"\n\
                         return [def[:symbol], def[:target_path], def[:start_line], def[:provider]]\n\
                       end\n\
                     end\n\
                     return nothing"
                ))
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            let symbol = value
                .with_list(|values| {
                    assert!(
                        values[0]
                            .with_str(|symbol| symbol.starts_with("idx:src/relations.rs:"))
                            .unwrap_or(false)
                    );
                    assert_eq!(values[1], Value::string("src/relations.rs"));
                    assert!(values[2].as_int().is_some_and(|line| line > 0));
                    assert!(
                        values[3]
                            .with_str(|provider| provider.contains("mica-source-index"))
                            .unwrap_or(false)
                    );
                    values[0].clone()
                })
                .expect("expected indexed definition tuple");

            let report = runner
                .run_source(&format!(
                    "let symbol = {symbol:?}\n\
                     let count = 0\n\
                     for reference in source/ReferencesOf(#repo, #rev, symbol, ?path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider, ?name)\n\
                       if reference[:name] == \"LocalSourceProvider\" && reference[:provider] == \"mica-source-index/static-analysis 4\"\n\
                         count = count + 1\n\
                       end\n\
                     end\n\
                     return count"
                ))
                .unwrap();
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.as_int().is_some_and(|count| count > 1)
            ));

            let report = runner
                .run_source(
                    "for result in source/SymbolSearch(#repo, #rev, \"LocalSource\", 5, ?symbol, ?name, ?kind, ?path, ?start_line, ?end_line, ?provider)\n\
                       if result[:name] == \"LocalSourceProvider\"\n\
                         return result[:provider]\n\
                       end\n\
                     end\n\
                     return nothing",
                )
                .unwrap();
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.with_str(|provider| provider.contains("mica-source-index")).unwrap_or(false)
            ));
        });
        let _ = fs::remove_file(index_path);
    }

    #[test]
    fn persistent_source_index_exposes_chunked_text_units() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-index-text-unit-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::write(
            root_path.join("src/lib.rs"),
            "pub fn actual_corpus_search_target() {\n    let phrase = \"actual corpus retrieval phrase\";\n}\n",
        )
        .unwrap();

        let index_path = temp_index_path("text-unit");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_relations_at(&mut runner, &root_path.display().to_string());
                let report = runner
                    .run_source(
                        "for unit in source/IndexedTextUnit(?unit, ?ordinal, ?kind, ?title, ?path, ?start_line, ?end_line, ?model, ?text)\n\
                           if unit[:path] == \"src/lib.rs\"\n\
                             return [unit[:kind], unit[:title], unit[:start_line], unit[:end_line], unit[:model], string_contains(unit[:text], \"actual corpus retrieval phrase\")]\n\
                           end\n\
                         end\n\
                         return nothing",
                    )
                    .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert_eq!(values[0], Value::string("rust"));
                        assert_eq!(values[1], Value::string("src/lib.rs:1-3"));
                        assert_eq!(values[2].as_int(), Some(1));
                        assert_eq!(values[3].as_int(), Some(3));
                        assert_eq!(values[4], Value::string("source-workspace"));
                        assert_eq!(values[5], Value::bool(true));
                    })
                    .expect("expected indexed text unit tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn persistent_source_index_skips_generated_book_output() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-index-generated-book-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("mdbook/src")).unwrap();
        fs::create_dir_all(root_path.join("mdbook/book")).unwrap();
        fs::write(
            root_path.join("mdbook/src/language.md"),
            "# Language\n\nAuthored btree retrieval notes.\n",
        )
        .unwrap();
        fs::write(
            root_path.join("mdbook/book/searchindex.js"),
            "window.search = { docs: ['generated btree search index'] };\n",
        )
        .unwrap();

        let index_path = temp_index_path("generated-book");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_relations_at(&mut runner, &root_path.display().to_string());
                let report = runner
                    .run_source(
                        "let has_authored_doc = false\n\
                         let has_generated_book = false\n\
                         for unit in source/IndexedTextUnit(?unit, ?ordinal, ?kind, ?title, ?path, ?start_line, ?end_line, ?model, ?text)\n\
                           if unit[:path] == \"mdbook/src/language.md\"\n\
                             has_authored_doc = true\n\
                           end\n\
                           if unit[:path] == \"mdbook/book/searchindex.js\"\n\
                             has_generated_book = true\n\
                           end\n\
                         end\n\
                         return [has_authored_doc, has_generated_book]",
                    )
                    .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert_eq!(values[0], Value::bool(true));
                        assert_eq!(values[1], Value::bool(false));
                    })
                    .expect("expected generated book exclusion tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn persistent_source_index_text_search_ranks_and_filters_source_units() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-index-text-search-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("crates/relation-kernel/src")).unwrap();
        fs::create_dir_all(root_path.join("crates/relation-kernel/tests")).unwrap();
        fs::create_dir_all(root_path.join("sketches")).unwrap();
        fs::create_dir_all(root_path.join("mdbook/book")).unwrap();
        fs::write(
            root_path.join("crates/relation-kernel/src/btree.rs"),
            "pub struct BTreeIndex;\nimpl BTreeIndex {\n    pub fn search_btree_node(&self) {}\n}\n",
        )
        .unwrap();
        fs::write(
            root_path.join("crates/relation-kernel/tests/btree_tests.rs"),
            "#[test]\nfn btree_insert_visible_in_tests() {}\n",
        )
        .unwrap();
        fs::write(
            root_path.join("sketches/btree-notes.md"),
            "# BTree notes\n\nSketches about btree relation indexing.\n",
        )
        .unwrap();
        fs::write(
            root_path.join("mdbook/book/searchindex.js"),
            "window.search = { docs: ['generated btree search index'] };\n",
        )
        .unwrap();

        let index_path = temp_index_path("text-search");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_relations_at(&mut runner, &root_path.display().to_string());
                let report = runner
                    .run_source(
                        "let first_path = nothing\n\
                         let first_line = 0\n\
                         let first_snippet = nothing\n\
                         let saw_generated_book = false\n\
                         for result in source/TextSearch(\"btree\", 8, \"all\", ?unit, ?score, ?path, ?start_line, ?end_line, ?kind, ?title, ?snippet)\n\
                           if first_path == nothing\n\
                             first_path = result[:path]\n\
                             first_line = result[:start_line]\n\
                             first_snippet = result[:snippet]\n\
                           end\n\
                           if result[:path] == \"mdbook/book/searchindex.js\"\n\
                             saw_generated_book = true\n\
                           end\n\
                         end\n\
                         let tests_only = true\n\
                         let tests_count = 0\n\
                         for result in source/TextSearch(\"btree\", 8, \"tests\", ?unit, ?score, ?path, ?start_line, ?end_line, ?kind, ?title, ?snippet)\n\
                           tests_count = tests_count + 1\n\
                           if string_contains(result[:path], \"/tests/\") == false\n\
                             tests_only = false\n\
                           end\n\
                         end\n\
                         return [first_path, first_line, string_contains(first_snippet, \"btree\") || string_contains(first_snippet, \"BTree\"), saw_generated_book, tests_count, tests_only]",
                    )
                    .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert_eq!(
                            values[0],
                            Value::string("crates/relation-kernel/src/btree.rs")
                        );
                        assert_eq!(values[1].as_int(), Some(1));
                        assert_eq!(values[2], Value::bool(true));
                        assert_eq!(values[3], Value::bool(false));
                        assert_eq!(values[4].as_int(), Some(1));
                        assert_eq!(values[5], Value::bool(true));
                    })
                    .expect("expected text search tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn persistent_source_index_keeps_mica_namespace_symbols_whole() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-index-mica-symbol-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(&root_path).unwrap();
        let source_path = "session.mica";
        let source = "make_relation(:session/CanAssumeActor, 2)\n\
                      assert session/CanAssumeActor(#web, #alice)\n";
        fs::write(root_path.join(source_path), source).unwrap();

        let index_path = temp_index_path("mica-symbol");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            load_source_relations_at(&mut runner, &root_path.display().to_string());
            let offset =
                source.find("session/CanAssumeActor(#web").unwrap() + "session/CanAssume".len();

            let report = runner
                .run_source(&format!(
                    "for def in source/DefinitionAt(#repo, #rev, {source_path:?}, {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                       return [def[:name], def[:kind], def[:target_path], def[:start_line], def[:provider]]\n\
                     end\n\
                     return nothing",
                    source_path = source_path,
                ))
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::string("session/CanAssumeActor"));
                    assert_eq!(values[1], Value::string("relation"));
                    assert_eq!(values[2], Value::string(source_path));
                    assert_eq!(values[3].as_int(), Some(1));
                    assert_eq!(
                        values[4],
                        Value::string("mica-source-index/static-analysis 4")
                    );
                })
                .expect("expected Mica definition tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn persistent_source_index_status_reports_build_failures() {
        let index_path = temp_index_path("failed");
        write_failed_source_index_file(Path::new("."), &index_path, "synthetic failure").unwrap();
        with_source_index_env(&index_path, None, || {
            let mut runner = SourceRunner::new_empty();
            load_source_relations(&mut runner);
            let report = runner
                .run_source(
                    "for index in source/SourceIndex(?index)\n\
                       let status = one source/IndexStatus(index[:index], ?status)\n\
                       let error = one source/IndexBuildError(index[:index], ?error)\n\
                       return [status, error]\n\
                     end\n\
                     return nothing",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::string("failed"));
                    assert_eq!(values[1], Value::string("synthetic failure"));
                })
                .expect("expected index status tuple");
        });
        let _ = fs::remove_file(index_path);
    }

    #[test]
    fn rust_analyzer_definition_accepts_identifier_start_offset() {
        if !rust_analyzer_available() {
            return;
        }

        with_source_provider_env(|| {
            let mut runner = SourceRunner::new_empty();
            load_source_relations(&mut runner);
            let source = fs::read_to_string("src/retrieval.rs").unwrap();
            let offset = source.find("fn parse_vector").unwrap() + "fn ".len();

            let report = runner
            .run_source(&format!(
                "for def in source/DefinitionAt(#repo, #rev, \"src/retrieval.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                   if def[:target_path] == \"src/retrieval.rs\"\n\
                     return def[:provider]\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            let TaskOutcome::Complete { value, .. } = &report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            if value == &Value::nothing() {
                return;
            }
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.with_str(|provider| provider.contains("rust-analyzer")).unwrap_or(false)
            ));
        });
    }

    #[test]
    fn rust_analyzer_module_definition_can_link_to_target_file() {
        if !rust_analyzer_available() {
            return;
        }

        with_source_provider_env(|| {
            let mut runner = SourceRunner::new_empty();
            load_source_relations(&mut runner);
            let source = fs::read_to_string("src/lib.rs").unwrap();
            let offset = source.find("mod retrieval").unwrap() + "mod ".len();

            let report = runner
            .run_source(&format!(
                "for def in source/DefinitionAt(#repo, #rev, \"src/lib.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                   if def[:target_path] == \"src/retrieval.rs\"\n\
                     return def[:start_line]\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            let TaskOutcome::Complete { value, .. } = &report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            if value == &Value::nothing() {
                return;
            }
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.as_int() == Some(1)
            ));
        });
    }

    #[test]
    fn rust_analyzer_module_definition_uses_workspace_relative_path() {
        if !rust_analyzer_available() {
            return;
        }

        let current_dir = env::current_dir().unwrap();
        let workspace = current_dir.parent().and_then(Path::parent).unwrap();
        with_source_root_env(workspace, || {
            let workspace = workspace.display().to_string();
            let mut runner = SourceRunner::new_empty();
            load_source_relations_at(&mut runner, &workspace);
            let source = fs::read_to_string("src/lib.rs").unwrap();
            let offset = source.find("mod retrieval").unwrap() + "mod ".len();

            let report = runner
            .run_source(&format!(
                "for def in source/DefinitionAt(#repo, #rev, \"crates/runtime/src/lib.rs\", {offset}, ?symbol, ?name, ?kind, ?target_path, ?start_line, ?end_line, ?start_byte, ?end_byte, ?provider)\n\
                   if def[:target_path] == \"crates/runtime/src/retrieval.rs\"\n\
                     return def[:start_line]\n\
                   end\n\
                 end\n\
                 return nothing"
            ))
            .unwrap();
            let TaskOutcome::Complete { value, .. } = &report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            if value == &Value::nothing() {
                return;
            }
            assert!(matches!(
                report.outcome,
                TaskOutcome::Complete { value, .. } if value.as_int() == Some(1)
            ));
        });
    }

    #[test]
    fn source_app_select_symbol_sync_event_updates_session_state() {
        let root_path = env::current_dir().unwrap().join(".cache").join(format!(
            "source-app-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let src_dir = root_path.join("src");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(
            src_dir.join("lib.rs"),
            "mod source_provider;\nfn call_provider() { source_provider::boot(); }\n",
        )
        .unwrap();
        fs::write(src_dir.join("source_provider.rs"), "pub fn boot() {}\n").unwrap();
        let root = root_path.display().to_string();
        let source_path = "src/lib.rs";
        let source_text_path = root_path.join(source_path);
        let index_path = temp_index_path("source-app");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/shared/retrieval.mica"),
                include_str!("../../../apps/shared/openai.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/retrieval.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-policy.mica"),
                include_str!("../../../apps/source/ui-state.mica"),
                include_str!("../../../apps/source/ui-actions.mica"),
                include_str!("../../../apps/source/ui-sync.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/ui-navigator.mica"),
                include_str!("../../../apps/source/ui-retrieval-panel.mica"),
                include_str!("../../../apps/source/ui-agent-panel.mica"),
                include_str!("../../../apps/source/ui-code-panel.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let source = fs::read_to_string(&source_text_path).unwrap();
            let offset = source.find("boot").unwrap();
            let report = runner
                .run_source(&format!(
                    "let fields = {{:path -> {source_path:?}, :byte -> {byte:?}}}\n\
                     let handled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_select_symbol\", fields)\n\
                     let references_handled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_find_references\", {{}})\n\
                     let path = one source/SelectedPath(endpoint(), ?path)\n\
                     let symbol = one source/SelectedSymbol(endpoint(), ?symbol)\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     return [handled, references_handled, path, symbol != nothing, string_contains(payload, \"source index\"), string_contains(payload, \"static-analysis\"), string_contains(payload, \"source_provider::boot\")]",
                    source_path = source_path,
                    byte = offset.to_string()
                ))
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::string("src/source_provider.rs"));
                    assert_eq!(values[3], Value::bool(true));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(true));
                    assert_eq!(values[6], Value::bool(true));
                })
                .expect("expected select-symbol state tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_annotation_cards_link_to_source_lines() {
        with_source_provider_env(|| {
            let root = env::current_dir()
                .unwrap()
                .parent()
                .and_then(Path::parent)
                .unwrap()
                .display()
                .to_string();
            let old_source_root = env::var_os("MICA_SOURCE_ROOT");
            unsafe {
                env::set_var("MICA_SOURCE_ROOT", &root);
            }
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/shared/retrieval.mica"),
                include_str!("../../../apps/shared/openai.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/retrieval.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-policy.mica"),
                include_str!("../../../apps/source/ui-state.mica"),
                include_str!("../../../apps/source/ui-actions.mica"),
                include_str!("../../../apps/source/ui-sync.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/ui-navigator.mica"),
                include_str!("../../../apps/source/ui-retrieval-panel.mica"),
                include_str!("../../../apps/source/ui-agent-panel.mica"),
                include_str!("../../../apps/source/ui-code-panel.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let report = runner
                .run_source(
                    "let fields = {:path -> \"apps/mud/ui-session.mica\"}\n\
                     let opened = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_file\", fields)\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     let jump_fields = {:path -> \"apps/mud/ui-session.mica\", :line -> \"8\"}\n\
                     let jumped = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_jump_to_line\", jump_fields)\n\
                     let selected = one source/SelectedLine(endpoint(), ?line)\n\
                     return [opened, string_contains(payload, \"source-span-form\"), string_contains(payload, \"source-span-button\"), string_contains(payload, \"source-line-annotation-marker\"), string_contains(payload, \"The app returns a DOM tree to the host\"), jumped, selected]",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::bool(true));
                    assert_eq!(values[3], Value::bool(true));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(true));
                    assert_eq!(values[6].as_int(), Some(8));
                })
                .expect("expected annotation link tuple");
            unsafe {
                if let Some(old_source_root) = old_source_root {
                    env::set_var("MICA_SOURCE_ROOT", old_source_root);
                } else {
                    env::remove_var("MICA_SOURCE_ROOT");
                }
            }
        });
    }

    #[test]
    fn source_app_retrieval_search_records_context_and_opens_citations() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-search-citation-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::create_dir_all(root_path.join("apps/mud")).unwrap();
        fs::create_dir_all(root_path.join("crates/runtime/src")).unwrap();
        fs::write(
            root_path.join("src/lib.rs"),
            "pub fn sync_view_tree() {\n    render_dom_snapshot();\n}\nfn render_dom_snapshot() {}\n",
        )
        .unwrap();
        fs::write(
            root_path.join("apps/mud/ui-session.mica"),
            "verb mud_placeholder()\n  return nothing\nend\n",
        )
        .unwrap();
        fs::write(
            root_path.join("crates/runtime/src/lib.rs"),
            "pub fn runtime() {}\n",
        )
        .unwrap();

        let index_path = temp_index_path("source-app-search-citation");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_app(&mut runner);
                let root = root_path.display().to_string();
                runner
                    .run_source(&format!(
                        "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                    ))
                    .unwrap();

                let report = runner
                .run_source(
                    "source/prewarm_retrieval_index(#web)\n\
                     let fields = {:question -> \"sync_view_tree\", :scope -> \"code\"}\n\
                     let searched = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_retrieve\", fields)\n\
                     let plan = one source/SelectedRetrievalPlan(endpoint(), ?plan)\n\
                     let question = one source/SelectedRetrievalQuestion(endpoint(), ?question)\n\
                     let status = one source/SelectedRetrievalStatus(endpoint(), ?status)\n\
                     let scope = one source/SelectedRetrievalScope(endpoint(), ?scope)\n\
                     let kind = one PlanKind(plan, ?kind)\n\
                     let has_context = false\n\
                     let subject = nothing\n\
                     for found in ContextForPlan(?context, plan)\n\
                       has_context = true\n\
                       subject = one ContextSubject(found[:context], ?subject)\n\
                       break\n\
                     end\n\
                     let citation = source/RetrievalCitation(plan, subject)\n\
                     let citation_text = one source/RetrievalCitationText(plan, subject, ?text)\n\
                     let allowed = source/can_retrieve_subject(#web, subject)\n\
                     let embedded = false\n\
                     for row in IndexEntryEmbedding(#source/retrieval_index, ?subject, \"source-workspace\", ?embedding)\n\
                       embedded = true\n\
                       break\n\
                     end\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     let opened = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_retrieval_citation\", {:subject -> to_literal(subject)})\n\
                     let path = one source/SelectedPath(endpoint(), ?path)\n\
                     let line = one source/SelectedLine(endpoint(), ?line)\n\
                     return [searched, plan != nothing, question, status, scope, kind, has_context, citation, string_contains(citation_text, \"sync_view_tree\"), allowed, embedded, string_contains(payload, \"source-retrieval-panel\"), string_contains(payload, \"sync_view_tree\"), opened, path, line]",
                )
                .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert_eq!(values[0], Value::bool(true));
                        assert_eq!(values[1], Value::bool(true));
                        assert_eq!(values[2], Value::string("sync_view_tree"));
                        assert!(
                            values[3]
                                .with_str(|status| status.ends_with(" search results"))
                                .unwrap_or(false)
                        );
                        assert_eq!(values[4], Value::string("code"));
                        assert_eq!(values[5], Value::string("text_search"));
                        assert_eq!(values[6], Value::bool(true));
                        assert_eq!(values[7], Value::bool(true));
                        assert_eq!(values[8], Value::bool(true));
                        assert_eq!(values[9], Value::bool(true));
                        assert_eq!(values[10], Value::bool(true));
                        assert_eq!(values[11], Value::bool(true));
                        assert_eq!(values[12], Value::bool(true));
                        assert_eq!(values[13], Value::bool(true));
                        assert_eq!(values[14], Value::string("src/lib.rs"));
                        assert_eq!(values[15].as_int(), Some(1));
                    })
                    .expect("expected source retrieval tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_retrieval_uses_indexed_corpus_text_units() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-corpus-retrieval-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::write(
            root_path.join("src/lib.rs"),
            "pub fn actual_corpus_search_target() {\n    let phrase = \"actual corpus retrieval phrase\";\n}\n",
        )
        .unwrap();

        let index_path = temp_index_path("source-app-corpus-retrieval");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_app(&mut runner);
                let root = root_path.display().to_string();
                runner
                    .run_source(&format!(
                        "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                         assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                    ))
                    .unwrap();

                let report = runner
                    .run_source(
                        "source/prewarm_retrieval_index(#web)\n\
                         source/run_retrieval_query(endpoint(), #web, \"actual corpus retrieval phrase\")\n\
                         let plan = one source/SelectedRetrievalPlan(endpoint(), ?plan)\n\
                         let selected = nothing\n\
                         let text_matches = false\n\
                         for found in ContextForPlan(?context, plan)\n\
                           let subject = one ContextSubject(found[:context], ?subject)\n\
                           let path = source/retrieval_text_unit_path(subject)\n\
                           if path == \"src/lib.rs\"\n\
                             selected = subject\n\
                             let text = source/retrieval_text_unit_text(subject)\n\
                             text_matches = string_contains(text, \"actual corpus retrieval phrase\")\n\
                             break\n\
                           end\n\
                         end\n\
                         let opened = false\n\
                         if selected != nothing\n\
                           opened = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_retrieval_citation\", {:subject -> to_literal(selected)})\n\
                         end\n\
                         let selected_path = one source/SelectedPath(endpoint(), ?path)\n\
                         let selected_line = one source/SelectedLine(endpoint(), ?line)\n\
                         return [selected != nothing, text_matches, opened, selected_path, selected_line]",
                    )
                    .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert_eq!(values[0], Value::bool(true));
                        assert_eq!(values[1], Value::bool(true));
                        assert_eq!(values[2], Value::bool(true));
                        assert_eq!(values[3], Value::string("src/lib.rs"));
                        assert_eq!(values[4].as_int(), Some(2));
                    })
                    .expect("expected indexed corpus retrieval tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_search_boosts_lexical_results_when_vector_index_is_ready() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-vector-boost-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::write(
            root_path.join("src/lib.rs"),
            "pub fn actual_corpus_search_target() {\n    let phrase = \"actual corpus retrieval phrase\";\n}\n",
        )
        .unwrap();

        let index_path = temp_index_path("source-app-vector-boost");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_app(&mut runner);
                let root = root_path.display().to_string();
                runner
                    .run_source(&format!(
                        "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                         assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                    ))
                    .unwrap();

                let report = runner
                    .run_source(
                        "let indexed = source/prewarm_retrieval_index(#web)\n\
                         let entry_count_before = 0\n\
                         for row in IndexEntryEmbedding(#source/retrieval_index, ?subject, \"source-workspace\", ?embedding)\n\
                           entry_count_before = entry_count_before + 1\n\
                         end\n\
                         source/run_retrieval_query(endpoint(), #web, \"actual corpus retrieval phrase\")\n\
                         let plan = one source/SelectedRetrievalPlan(endpoint(), ?plan)\n\
                         let semantic_status = one source/SelectedRetrievalSemanticStatus(endpoint(), ?status)\n\
                         let reason = nothing\n\
                         for found in ContextForPlan(?context, plan)\n\
                           let subject = one ContextSubject(found[:context], ?subject)\n\
                           if source/retrieval_text_unit_path(subject) == \"src/lib.rs\"\n\
                             reason = one ContextReason(found[:context], ?reason)\n\
                             break\n\
                           end\n\
                         end\n\
                         let entry_count_after = 0\n\
                         for row in IndexEntryEmbedding(#source/retrieval_index, ?subject, \"source-workspace\", ?embedding)\n\
                           entry_count_after = entry_count_after + 1\n\
                         end\n\
                         return [indexed, entry_count_before, entry_count_after, semantic_status, reason]",
                    )
                    .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert!(values[0].as_int().unwrap_or_default() > 0);
                        assert!(values[1].as_int().unwrap_or_default() > 0);
                        assert_eq!(values[1], values[2]);
                        assert_eq!(values[3], Value::string("vector_boosted"));
                        assert_eq!(values[4], Value::string("text_search+nearest_embedding"));
                    })
                    .expect("expected vector boost tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_retrieval_search_does_not_index_inline() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-no-inline-indexing-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::write(
            root_path.join("src/lib.rs"),
            "pub fn computed_relation_boundary() {}\n",
        )
        .unwrap();

        let index_path = temp_index_path("source-app-no-inline-indexing");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_app(&mut runner);
                let root = root_path.display().to_string();
                runner
                    .run_source(&format!(
                        "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                    ))
                    .unwrap();
                let report = runner
                .run_source(
                    "let fields = {:question -> \"computed relation boundary\"}\n\
                     let searched = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_retrieve\", fields)\n\
                     let question = one source/SelectedRetrievalQuestion(endpoint(), ?question)\n\
                     let status = one source/SelectedRetrievalStatus(endpoint(), ?status)\n\
                     let semantic_status = one source/SelectedRetrievalSemanticStatus(endpoint(), ?status)\n\
                     let plan = one source/SelectedRetrievalPlan(endpoint(), ?plan)\n\
                     let embedded = false\n\
                     for row in IndexEntryEmbedding(#source/retrieval_index, ?subject, \"source-workspace\", ?embedding)\n\
                       embedded = true\n\
                       break\n\
                     end\n\
                     return [searched, question, status, semantic_status, embedded == false, plan != nothing]",
                )
                .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert_eq!(values[0], Value::bool(true));
                        assert_eq!(values[1], Value::string("computed relation boundary"));
                        assert_eq!(values[2], Value::string("indexing source"));
                        assert_eq!(values[3], Value::string("indexing"));
                        assert_eq!(values[4], Value::bool(true));
                        assert_eq!(values[5], Value::bool(false));
                    })
                    .expect("expected no-inline-indexing tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_retrieval_search_records_submitted_query() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-query-record-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("docs")).unwrap();
        fs::create_dir_all(root_path.join("apps/mud")).unwrap();
        fs::create_dir_all(root_path.join("crates/runtime/src")).unwrap();
        fs::write(
            root_path.join("docs/retrieval.md"),
            "# Computed relation boundary\n\nThe computed relation boundary is visible in search snippets.\n",
        )
        .unwrap();
        fs::write(
            root_path.join("apps/mud/ui-session.mica"),
            "verb mud_placeholder()\n  return nothing\nend\n",
        )
        .unwrap();
        fs::write(
            root_path.join("crates/runtime/src/lib.rs"),
            "pub fn runtime() {}\n",
        )
        .unwrap();

        let index_path = temp_index_path("source-app-query-record");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_app(&mut runner);
                let root = root_path.display().to_string();
                runner
                    .run_source(&format!(
                        "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                    ))
                    .unwrap();
                let report = runner
                .run_source(
                    "let fields = {:question -> \"computed relation boundary\", :scope -> \"docs\"}\n\
                     let searched = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_retrieve\", fields)\n\
                     let question = one source/SelectedRetrievalQuestion(endpoint(), ?question)\n\
                     let scope = one source/SelectedRetrievalScope(endpoint(), ?scope)\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     return [searched, question, scope, string_contains(payload, \"computed relation boundary\"), string_contains(payload, \"Computed relation boundary\"), string_contains(payload, \"sync_view_tree renders DOM sync\")]",
                )
                .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert_eq!(values[0], Value::bool(true));
                        assert_eq!(values[1], Value::string("computed relation boundary"));
                        assert_eq!(values[2], Value::string("docs"));
                        assert_eq!(values[3], Value::bool(true));
                        assert_eq!(values[4], Value::bool(true));
                        assert_eq!(values[5], Value::bool(false));
                    })
                    .expect("expected submitted query tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_retrieval_marks_changed_text_stale() {
        with_source_provider_env(|| {
            let mut runner = SourceRunner::new_empty();
            load_source_app(&mut runner);
            let report = runner
                .run_source(
                    "source/prewarm_retrieval_index(#web)\n\
                     source/run_retrieval_query(endpoint(), #web, \"where is DOM sync rendered?\")\n\
                     retract TextUnitText(#source/text_symbol_sync_view_tree, _)\n\
                     assert TextUnitText(#source/text_symbol_sync_view_tree, \"changed DOM sync retrieval text\")\n\
                     let status = source/retrieval_subject_status(#source/text_symbol_sync_view_tree, \"source-workspace\")\n\
                     let refresh = EmbeddingRefreshNeeded(#source/retrieval_index, #source/text_symbol_sync_view_tree, \"source-workspace\")\n\
                     let index_status = one IndexEntryStatus(#source/retrieval_index, #source/text_symbol_sync_view_tree, \"source-workspace\", ?status)\n\
                     return [status, refresh, index_status]",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::string("stale"));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::string("stale"));
                })
                .expect("expected stale retrieval tuple");
        });
    }

    #[test]
    fn source_app_retrieval_marks_changed_index_version_stale() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-retrieval-index-stale-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::write(
            root_path.join("src/lib.rs"),
            "pub fn sync_view_tree() {}\npub fn render_dom_snapshot() {}\n",
        )
        .unwrap();

        let index_path = temp_index_path("source-app-retrieval-version-stale");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_app(&mut runner);
                let root = root_path.display().to_string();
                runner
                    .run_source(&format!(
                        "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                    ))
                    .unwrap();

                let report = runner
                .run_source(
                    "source/prewarm_retrieval_index(#web)\n\
                     source/run_retrieval_query(endpoint(), #web, \"sync_view_tree\")\n\
                     let plan = one source/SelectedRetrievalPlan(endpoint(), ?plan)\n\
                     let artifact_index = one source/RetrievalArtifactIndex(plan, ?index)\n\
                     let current_version = one source/IndexVersion(artifact_index, ?version)\n\
                     let stale_before = source/StaleRetrievalArtifact(artifact_index, plan)\n\
                     retract source/RetrievalArtifactIndexVersion(plan, _)\n\
                     assert source/RetrievalArtifactIndexVersion(plan, \"outdated\")\n\
                     let stale_after = source/StaleRetrievalArtifact(artifact_index, plan)\n\
                     return [artifact_index != #source/retrieval_index, current_version != nothing, stale_before, stale_after]",
                )
                .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert_eq!(values[0], Value::bool(true));
                        assert_eq!(values[1], Value::bool(true));
                        assert_eq!(values[2], Value::bool(false));
                        assert_eq!(values[3], Value::bool(true));
                    })
                    .expect("expected stale retrieval artifact tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_symbol_neighbourhood_search_records_retrieval_plan() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-symbol-neighbourhood-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("apps/mud")).unwrap();
        fs::create_dir_all(root_path.join("crates/runtime/src")).unwrap();
        fs::write(
            root_path.join("apps/mud/ui-session.mica"),
            "verb sync_view_tree()\n  return nothing\nend\n",
        )
        .unwrap();
        fs::write(
            root_path.join("crates/runtime/src/lib.rs"),
            "pub fn runtime() {}\n",
        )
        .unwrap();

        let index_path = temp_index_path("source-app-symbol-neighbourhood");
        build_source_index_file(&root_path, &index_path).unwrap();
        with_source_index_and_root_env(
            &index_path,
            &root_path,
            Some(Path::new("/bin/false")),
            || {
                let mut runner = SourceRunner::new_empty();
                load_source_app(&mut runner);
                let root = root_path.display().to_string();
                runner
                    .run_source(&format!(
                        "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                    ))
                    .unwrap();
                let report = runner
                .run_source(
                    "retract source/SelectedSymbol(endpoint(), _)\n\
                     retract source/SelectedSymbolName(endpoint(), _)\n\
                     retract source/SelectedSymbolKind(endpoint(), _)\n\
                     assert source/SelectedSymbol(endpoint(), #source/symbol_sync_view_tree)\n\
                     assert source/SelectedSymbolName(endpoint(), \"sync_view_tree\")\n\
                     assert source/SelectedSymbolKind(endpoint(), \"verb\")\n\
                     assert source/SelectedDefinitionPath(endpoint(), \"apps/mud/ui-session.mica\")\n\
                     assert source/SelectedDefinitionStartLine(endpoint(), 8)\n\
                     assert source/SelectedDefinitionEndLine(endpoint(), 17)\n\
                     assert source/SelectedSymbolProvider(endpoint(), \"test\")\n\
                     source/prewarm_retrieval_index(#web)\n\
                     let handled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_retrieve_symbol\", {})\n\
                     let plan = one source/SelectedRetrievalPlan(endpoint(), ?plan)\n\
                     let kind = one PlanKind(plan, ?kind)\n\
                     let question = one source/SelectedRetrievalQuestion(endpoint(), ?question)\n\
                     let has_context = false\n\
                     for found in ContextForPlan(?context, plan)\n\
                       has_context = true\n\
                     end\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     return [handled, kind, string_contains(question, \"sync_view_tree\"), has_context, string_contains(payload, \"Search neighbourhood\"), string_contains(payload, \"source_symbol_neighbourhood\")]",
                )
                .unwrap();
                let TaskOutcome::Complete { value, .. } = report.outcome else {
                    panic!("expected complete outcome, got {:?}", report.outcome);
                };
                value
                    .with_list(|values| {
                        assert_eq!(values[0], Value::bool(true));
                        assert_eq!(values[1], Value::string("source_symbol_neighbourhood"));
                        assert_eq!(values[2], Value::bool(true));
                        assert_eq!(values[3], Value::bool(true));
                        assert_eq!(values[4], Value::bool(true));
                        assert_eq!(values[5], Value::bool(true));
                    })
                    .expect("expected symbol neighbourhood tuple");
            },
        );
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_scroll_window_keeps_syntax_rows_for_long_files() {
        with_source_provider_env(|| {
            let root_path = env::current_dir().unwrap().join("target").join(format!(
                "source-app-long-file-fixture-{}-{}",
                std::process::id(),
                std::thread::current().name().unwrap_or("test")
            ));
            fs::create_dir_all(&root_path).unwrap();
            let mut body = String::new();
            for line in 1..=800 {
                body.push_str(&format!(
                    "pub fn generated_{line}(value: Result<String, String>) -> Option<String> {{ value.ok() }}\n"
                ));
            }
            fs::write(root_path.join("long.rs"), body).unwrap();
            let root = root_path.display().to_string();

            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/shared/retrieval.mica"),
                include_str!("../../../apps/shared/openai.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/retrieval.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-policy.mica"),
                include_str!("../../../apps/source/ui-state.mica"),
                include_str!("../../../apps/source/ui-actions.mica"),
                include_str!("../../../apps/source/ui-sync.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/ui-navigator.mica"),
                include_str!("../../../apps/source/ui-retrieval-panel.mica"),
                include_str!("../../../apps/source/ui-agent-panel.mica"),
                include_str!("../../../apps/source/ui-code-panel.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let report = runner
                .run_source(
                    "let open_fields = {:path -> \"long.rs\"}\n\
                     let opened = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_file\", open_fields)\n\
                     let initial_revision = sync_view_revision(31)\n\
                     let initial_payload = dom_snapshot_payload(31, initial_revision, sync_view_tree(31, initial_revision))\n\
                     let window_fields = {:path -> \"long.rs\", :window_start -> \"561\"}\n\
                     let moved = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_set_window\", window_fields)\n\
                     let next_revision = sync_view_revision(31)\n\
                     let next_payload = dom_snapshot_payload(31, next_revision, sync_view_tree(31, next_revision))\n\
                     return [opened, string_contains(initial_payload, \"source-code-line\"), string_contains(initial_payload, \"generated_240\"), string_contains(initial_payload, \"generated_800\"), moved, string_contains(next_payload, \"generated_800\"), string_contains(next_payload, \"source-line-number\"), string_contains(next_payload, \"source-code-spacer-top\")]",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::bool(true));
                    assert_eq!(values[3], Value::bool(false));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(true));
                    assert_eq!(values[6], Value::bool(true));
                    assert_eq!(values[7], Value::bool(true));
                })
                .expect("expected long-file source window tuple");

            let _ = fs::remove_dir_all(root_path);
        });
    }

    #[test]
    fn source_app_unknown_rust_symbol_is_noop() {
        let root_path = env::current_dir().unwrap().join(".cache").join(format!(
            "source-app-unknown-symbol-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let src_dir = root_path.join("src");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(
            src_dir.join("lib.rs"),
            "pub fn call_provider() -> Result<String, String> { Ok(String::new()) }\n",
        )
        .unwrap();
        let root = root_path.display().to_string();
        let source_path = "src/lib.rs";
        let source_text_path = root_path.join(source_path);
        let index_path = temp_index_path("source-app-unknown-symbol");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/shared/retrieval.mica"),
                include_str!("../../../apps/shared/openai.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/retrieval.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-policy.mica"),
                include_str!("../../../apps/source/ui-state.mica"),
                include_str!("../../../apps/source/ui-actions.mica"),
                include_str!("../../../apps/source/ui-sync.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/ui-navigator.mica"),
                include_str!("../../../apps/source/ui-retrieval-panel.mica"),
                include_str!("../../../apps/source/ui-agent-panel.mica"),
                include_str!("../../../apps/source/ui-code-panel.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let source = fs::read_to_string(&source_text_path).unwrap();
            let offset = source.find("String").unwrap();
            let report = runner
                .run_source(&format!(
                    "let fields = {{:path -> {source_path:?}, :byte -> {byte:?}}}\n\
                     let handled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_select_symbol\", fields)\n\
                     let symbol = one source/SelectedSymbol(endpoint(), ?symbol)\n\
                     let revision = sync_view_revision(31)\n\
                     return [handled, symbol == nothing, revision]",
                    source_path = source_path,
                    byte = offset.to_string()
                ))
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(false));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2].as_int(), Some(1));
                })
                .expect("expected unknown-symbol noop tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_open_directory_sync_event_updates_session_state() {
        let root_path = env::current_dir().unwrap().join(".cache").join(format!(
            "source-app-dir-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let src_dir = root_path.join("src").join("deep");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(root_path.join("src").join("lib.rs"), "mod deep;\n").unwrap();
        fs::write(src_dir.join("leaf.rs"), "pub fn leaf() {}\n").unwrap();
        let root = root_path.display().to_string();
        let index_path = temp_index_path("source-app-dir");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/shared/retrieval.mica"),
                include_str!("../../../apps/shared/openai.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/retrieval.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-policy.mica"),
                include_str!("../../../apps/source/ui-state.mica"),
                include_str!("../../../apps/source/ui-actions.mica"),
                include_str!("../../../apps/source/ui-sync.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/ui-navigator.mica"),
                include_str!("../../../apps/source/ui-retrieval-panel.mica"),
                include_str!("../../../apps/source/ui-agent-panel.mica"),
                include_str!("../../../apps/source/ui-code-panel.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let report = runner
                .run_source(
                    "let file_fields = {:path -> \"src/lib.rs\"}\n\
                     let opened_file = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_file\", file_fields)\n\
                     let src_fields = {:path -> \"src\"}\n\
                     let opened_src = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_directory\", src_fields)\n\
                     let dir_fields = {:path -> \"src/deep\"}\n\
                     let opened_dir = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_directory\", dir_fields)\n\
                     let src_expanded = source/ExpandedDirectory(endpoint(), \"src\")\n\
                     let deep_expanded = source/ExpandedDirectory(endpoint(), \"src/deep\")\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     return [opened_file, opened_src, opened_dir, src_expanded, deep_expanded, string_contains(payload, \"leaf.rs\"), string_contains(payload, \"Collapse\")]",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::bool(true));
                    assert_eq!(values[3], Value::bool(true));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(true));
                    assert_eq!(values[6], Value::bool(true));
                })
                .expect("expected open-directory state tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_hides_dot_entries_until_toggled() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-hidden-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join(".secret")).unwrap();
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::write(root_path.join(".env"), "TOKEN=secret\n").unwrap();
        fs::write(root_path.join(".secret").join("index.json"), "{}\n").unwrap();
        fs::write(
            root_path.join("src").join("lib.rs"),
            "pub fn visible() {}\n",
        )
        .unwrap();
        let root = root_path.display().to_string();
        let index_path = temp_index_path("source-app-hidden");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/shared/retrieval.mica"),
                include_str!("../../../apps/shared/openai.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/retrieval.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-policy.mica"),
                include_str!("../../../apps/source/ui-state.mica"),
                include_str!("../../../apps/source/ui-actions.mica"),
                include_str!("../../../apps/source/ui-sync.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/ui-navigator.mica"),
                include_str!("../../../apps/source/ui-retrieval-panel.mica"),
                include_str!("../../../apps/source/ui-agent-panel.mica"),
                include_str!("../../../apps/source/ui-code-panel.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let report = runner
                .run_source(
                    "let open_fields = {:path -> \"src/lib.rs\"}\n\
                     let opened = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_file\", open_fields)\n\
                     let initial_revision = sync_view_revision(31)\n\
                     let initial_payload = dom_snapshot_payload(31, initial_revision, sync_view_tree(31, initial_revision))\n\
                     let fields = {:show_hidden -> \"true\"}\n\
                     let toggled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_toggle_hidden\", fields)\n\
                     let next_revision = sync_view_revision(31)\n\
                     let next_payload = dom_snapshot_payload(31, next_revision, sync_view_tree(31, next_revision))\n\
                     return [opened, string_contains(initial_payload, \"src\"), string_contains(initial_payload, \".env\"), string_contains(initial_payload, \".secret\"), toggled, string_contains(next_payload, \".env\"), string_contains(next_payload, \".secret\")]",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::bool(false));
                    assert_eq!(values[3], Value::bool(false));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(true));
                    assert_eq!(values[6], Value::bool(true));
                })
                .expect("expected hidden-toggle state tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }

    #[test]
    fn source_app_toggles_inspector_sections() {
        let root_path = env::current_dir().unwrap().join("target").join(format!(
            "source-app-inspector-fixture-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(root_path.join("src")).unwrap();
        fs::write(
            root_path.join("src").join("lib.rs"),
            "pub fn inspector_fixture() {}\n",
        )
        .unwrap();
        let root = root_path.display().to_string();
        let index_path = temp_index_path("source-app-inspector");
        build_source_index_file(Path::new(&root), &index_path).unwrap();
        with_source_index_env(&index_path, Some(Path::new("/bin/false")), || {
            let mut runner = SourceRunner::new_empty();
            for filein in [
                include_str!("../../../apps/shared/sync-host.mica"),
                include_str!("../../../apps/shared/sync-dom.mica"),
                include_str!("../../../apps/shared/retrieval.mica"),
                include_str!("../../../apps/shared/openai.mica"),
                include_str!("../../../apps/source/core.mica"),
                include_str!("../../../apps/source/retrieval.mica"),
                include_str!("../../../apps/source/ui-session.mica"),
                include_str!("../../../apps/source/ui-policy.mica"),
                include_str!("../../../apps/source/ui-state.mica"),
                include_str!("../../../apps/source/ui-actions.mica"),
                include_str!("../../../apps/source/ui-sync.mica"),
                include_str!("../../../apps/source/ui-compose.mica"),
                include_str!("../../../apps/source/ui-navigator.mica"),
                include_str!("../../../apps/source/ui-retrieval-panel.mica"),
                include_str!("../../../apps/source/ui-agent-panel.mica"),
                include_str!("../../../apps/source/ui-code-panel.mica"),
                include_str!("../../../apps/source/http.mica"),
            ] {
                runner.run_filein(filein).unwrap();
            }
            runner
                .run_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {root:?})"
                ))
                .unwrap();

            let report = runner
                .run_source(
                    "let open_fields = {:path -> \"src/lib.rs\"}\n\
                     let opened = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_open_file\", open_fields)\n\
                     let outline_fields = {:section -> \"outline\", :collapsed -> \"true\"}\n\
                     let outline_toggled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_toggle_inspector_section\", outline_fields)\n\
                     let annotation_fields = {:section -> \"annotations\", :collapsed -> \"true\"}\n\
                     let annotations_toggled = sync_event(endpoint(), nothing, 31, \"submit\", \"\", \"source_toggle_inspector_section\", annotation_fields)\n\
                     let revision = sync_view_revision(31)\n\
                     let payload = dom_snapshot_payload(31, revision, sync_view_tree(31, revision))\n\
                     return [opened, outline_toggled, annotations_toggled, string_contains(payload, \"source-outline source-inspector-section collapsed\"), string_contains(payload, \"source-spans source-inspector-section collapsed\"), string_contains(payload, \"source-inspector-rail both-collapsed\")]",
                )
                .unwrap();
            let TaskOutcome::Complete { value, .. } = report.outcome else {
                panic!("expected complete outcome, got {:?}", report.outcome);
            };
            value
                .with_list(|values| {
                    assert_eq!(values[0], Value::bool(true));
                    assert_eq!(values[1], Value::bool(true));
                    assert_eq!(values[2], Value::bool(true));
                    assert_eq!(values[3], Value::bool(true));
                    assert_eq!(values[4], Value::bool(true));
                    assert_eq!(values[5], Value::bool(true));
                })
                .expect("expected inspector toggle state tuple");
        });
        let _ = fs::remove_file(index_path);
        let _ = fs::remove_dir_all(root_path);
    }
}
