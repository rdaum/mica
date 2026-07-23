;;; mica-mode.el --- Major mode for the Mica language -*- lexical-binding: t; -*-

;; Author: Chaz Straney (chaz.straney@gmail.com)
;; Keywords: languages mica
;; Version: 0.1.0
;; Package-Requires: ((emacs "26.1"))
;; URL: https://github.com/timbran-project/mica

;;; Commentary:

;; A major mode for authoring Mica source (.mica) files.
;;
;; Mica is a relational language/runtime built around facts, rules, objects,
;; and inference.  See https://codeberg.org/timbran/mica.
;;
;; Features:
;;   - Font-lock for keywords, value constants, relation/type names,
;;     identity (#id), symbol (:sym), query (?var) and role/splat (@name)
;;     sigils, error-code literals (E_FOO), and // line comments.
;;   - A syntax table covering // line comments and "..." strings.
;;   - Block-aware indentation (verb/method/if/for/while/begin/try ... end),
;;     Datalog rule bodies (Head(..) :-), and comma-continued argument lists.
;;   - imenu support for verbs, methods, lambda bindings, relation
;;     declarations, and rules.
;;
;; The token set is derived directly from the Mica compiler lexer
;; (crates/compiler/src/lexer.rs).
;;
;; Install: put this file on `load-path' and `(require 'mica-mode)'.
;;
;; The ERT test suite at the bottom of this file can be run with
;;   M-x ert RET mica- RET
;; or in batch:
;;   emacs -batch -l ert -l mica-mode.el -f ert-run-tests-batch-and-exit

;;; Code:

(defgroup mica nil
  "Major mode for editing Mica source."
  :group 'languages
  :prefix "mica-")

(defcustom mica-indent-offset 2
  "Number of spaces per indentation level in `mica-mode'."
  :type 'integer
  :group 'mica)

;;;; Keywords
;;
;; These mirror `keyword_kind' in crates/compiler/src/lexer.rs.  Note that
;; words such as `object' and `extends' are NOT keywords in Mica; they are
;; ordinary identifiers, so they are deliberately absent here.

(defconst mica--keywords
  '("let" "const" "if" "elseif" "else" "end" "begin" "for" "in" "while"
    "return" "raise" "recover" "one" "spawn" "after" "not" "break"
    "continue" "try" "catch" "as" "finally" "fn" "method" "verb" "do"
    "assert" "retract" "require")
  "Mica keywords (excludes the value constants true/false/nothing).")

(defconst mica--constants
  '("true" "false" "nothing")
  "Mica literal value constants.")

;;;; Font lock

(defconst mica-font-lock-keywords
  (let ((kw (regexp-opt mica--keywords 'symbols))
        (const (regexp-opt mica--constants 'symbols)))
    `(
      ;; Definition forms: highlight the name following verb/method/fn.
      ("\\_<\\(?:verb\\|method\\|fn\\)\\_>[ \t]+\\([a-zA-Z_][a-zA-Z0-9_]*\\)"
       1 font-lock-function-name-face)
      ;; Keywords.
      (,kw . font-lock-keyword-face)
      ;; Value constants.
      (,const . font-lock-constant-face)
      ;; Error code literals: E_FOO (precedes the TitleCase rule below).
      ("\\_<E_[A-Za-z0-9_]+\\_>" . font-lock-warning-face)
      ;; Identity values: #name.
      ("#[A-Za-z_][A-Za-z0-9_]*" . font-lock-variable-name-face)
      ;; Symbol literals: :name (the lexer distinguishes :- as a separate
      ;; token, so requiring an identifier char after `:' avoids matching it).
      (":\\([A-Za-z_][A-Za-z0-9_]*\\)" 0 font-lock-constant-face)
      ;; Query variables: ?name.
      ("\\?[A-Za-z_][A-Za-z0-9_]*" . font-lock-variable-name-face)
      ;; Role bindings / splats: @name.
      ("@[A-Za-z_][A-Za-z0-9_]*" . font-lock-builtin-face)
      ;; Relation / type names: TitleCase identifiers.
      ("\\_<[A-Z][A-Za-z0-9_]*\\_>" . font-lock-type-face)
      ;; Rule implication and arrows.
      ("\\(:-\\|->\\|=>\\)" 1 font-lock-keyword-face)
      ))
  "Font-lock rules for `mica-mode'.")

;;;; Syntax table

(defvar mica-mode-syntax-table
  (let ((table (make-syntax-table)))
    ;; // line comments: `/' is comment seq char 1 and 2; newline ends it.
    (modify-syntax-entry ?/ ". 12" table)
    (modify-syntax-entry ?\n ">" table)
    ;; Strings.
    (modify-syntax-entry ?\" "\"" table)
    (modify-syntax-entry ?\\ "\\" table)
    ;; Identifier constituents.
    (modify-syntax-entry ?_ "_" table)
    ;; Sigils and operators as punctuation.
    (dolist (ch '(?# ?: ?? ?@ ?! ?= ?< ?> ?+ ?- ?* ?% ?& ?|))
      (modify-syntax-entry ch "." table))
    table)
  "Syntax table for `mica-mode'.")

;;;; Indentation

(defconst mica--dedent-line-re
  "^[ \t]*\\(?:end\\|else\\|elseif\\|catch\\|finally\\)\\_>"
  "Lines that sit one level shallower than their enclosing block body.")

(defconst mica--block-token-re
  "\\_<\\(verb\\|method\\|if\\|for\\|while\\|begin\\|try\\|recover\\|fn\\|end\\)\\_>"
  "Block-opening keywords and the closing `end', for depth counting.
`fn' is included but treated conditionally: a block lambda
\(`fn(x) ... end') opens a block, while an expression lambda
\(`fn(x) => expr') does not — see `mica--token-block-delta'.  `do' is
excluded because it always follows an opener that has already been
counted (`for ... do', `while ... do', `method f() do'), so counting it
too would double-indent.")

(defun mica--token-block-delta (tok tok-end)
  "Block-depth contribution of block keyword TOK ending at TOK-END.
Returns -1 for `end', +1 for an opener, and 0 for an expression `fn'
lambda (one with a `=>' later on the same line)."
  (cond
   ((string= tok "end") -1)
   ((string= tok "fn")
    ;; `fn(x) => expr' is an expression with no matching `end'; only the
    ;; block form `fn(x) ... end' nests.  Distinguish by a same-line `=>'.
    (if (save-excursion
          (goto-char tok-end)
          (re-search-forward "=>" (line-end-position) t))
        0 1))
   (t 1)))

(defun mica--block-depth-before (limit)
  "Return net block nesting depth from `point-min' up to LIMIT.
Tokens inside strings or comments are ignored via `syntax-ppss'."
  (let ((depth 0))
    (save-excursion
      (goto-char (point-min))
      (while (re-search-forward mica--block-token-re limit t)
        ;; Capture token and end before `syntax-ppss', which clobbers match
        ;; data and (on a cold cache) moves point — hence `save-excursion'.
        (let* ((tok (match-string-no-properties 1))
               (tok-end (match-end 0))
               (state (save-excursion (syntax-ppss (match-beginning 0)))))
          (unless (or (nth 3 state) (nth 4 state))
            (setq depth (+ depth (mica--token-block-delta tok tok-end)))))))
    (max 0 depth)))

(defun mica--line-block-delta ()
  "Return the net block-depth change contributed by the current line.
Tokens inside strings or comments are ignored via `syntax-ppss'."
  (let ((delta 0)
        (eol (line-end-position)))
    (save-excursion
      (beginning-of-line)
      (while (re-search-forward mica--block-token-re eol t)
        (let* ((tok (match-string-no-properties 1))
               (tok-end (match-end 0))
               (state (save-excursion (syntax-ppss (match-beginning 0)))))
          (unless (or (nth 3 state) (nth 4 state))
            (setq delta (+ delta (mica--token-block-delta tok tok-end)))))))
    delta))

(defun mica--line-trailing ()
  "Return the trailing-token marker for the current line.
:rule when the code ends with `:-', the character ?, when it ends with a
comma, else nil.  A trailing // line comment is stripped, but a // that
sits inside a string literal is not mistaken for a comment."
  (save-excursion
    (let* ((bol (line-beginning-position))
           (eol (line-end-position))
           (comment-pos nil))
      (goto-char bol)
      ;; Find the first // that is not inside a string -> comment start.
      (while (and (not comment-pos) (search-forward "//" eol t))
        (unless (nth 3 (save-excursion (syntax-ppss (- (point) 2))))
          (setq comment-pos (- (point) 2))))
      (let ((code (string-trim-right
                   (buffer-substring-no-properties bol (or comment-pos eol)))))
        (cond ((string-suffix-p ":-" code) :rule)
              ((string-suffix-p "," code) ?,)
              (t nil))))))

(defun mica--prev-code-line ()
  "Return (INDENT . TRAILING) for the nearest prior code line, or nil.
TRAILING is as returned by `mica--line-trailing'.  Blank and
comment-only lines are skipped."
  (save-excursion
    (forward-line -1)
    (while (and (not (bobp))
                (looking-at "^[ \t]*\\(//.*\\)?$"))
      (forward-line -1))
    (unless (looking-at "^[ \t]*\\(//.*\\)?$")
      (cons (current-indentation) (mica--line-trailing)))))

(defun mica--indent-from (prev depth)
  "Return the indentation column for the line at point (assumed at BOL).
PREV is the (INDENT . TRAILING) of the previous code line (or nil), and
DEPTH is the net block nesting before this line."
  (cond
   ;; Inside a comma-continued list (call args, rule body): align with it.
   ((and prev (eq (cdr prev) ?,)) (car prev))
   ;; First line of a rule body, just after `Head(..) :-'.
   ((and prev (eq (cdr prev) :rule)) (+ (car prev) mica-indent-offset))
   ;; Otherwise indent by block-nesting depth.
   (t
    ;; `mica--dedent-line-re' is anchored with `^', so test it at BOL — not
    ;; after `back-to-indentation', which would leave point past the leading
    ;; whitespace and make `^' fail on an already-indented line.
    (when (save-excursion
            (beginning-of-line)
            (looking-at mica--dedent-line-re))
      (setq depth (1- depth)))
    (* (max 0 depth) mica-indent-offset))))

(defun mica-calculate-indent ()
  "Compute the indentation column for the current line."
  (save-excursion
    (beginning-of-line)
    (let ((prev (mica--prev-code-line)))
      (if (null prev)
          0
        (mica--indent-from prev (mica--block-depth-before (point)))))))

(defun mica-indent-region (start end)
  "Indent every line between START and END in a single forward pass.
This is O(n) for the region, where the per-line `mica-calculate-indent'
path is O(n) each (rescanning from `point-min'), making a naive
`indent-region' O(n^2).  Running block depth is carried across lines."
  (save-excursion
    (goto-char start)
    (beginning-of-line)
    (let* ((region-start (point))
           (end-marker (copy-marker end))
           ;; Block depth and previous code line as they stand entering the
           ;; region.  At buffer start there is no preceding code line.
           (depth (mica--block-depth-before region-start))
           (prev (and (> region-start (point-min)) (mica--prev-code-line))))
      (while (< (point) end-marker)
        (let ((empty (looking-at "^[ \t]*$"))
              (comment-only (looking-at "^[ \t]*//")))
          (unless empty
            (indent-line-to (mica--indent-from prev depth)))
          ;; Carry block depth forward using this line's own tokens.
          (setq depth (+ depth (mica--line-block-delta)))
          ;; Only genuine code lines become the `prev' for continuation.
          (unless (or empty comment-only)
            (setq prev (cons (current-indentation) (mica--line-trailing)))))
        (forward-line 1))
      (set-marker end-marker nil))))

(defun mica-indent-line ()
  "Indent the current line per `mica-mode' heuristics.
If point is within the leading whitespace it ends at the first
non-blank character; otherwise its position in the text is preserved."
  (interactive)
  (let ((target (mica-calculate-indent)))
    (if (<= (current-column) (current-indentation))
        ;; Point is in the indentation (or at BOL): safe to move it.
        (indent-line-to target)
      ;; Point is inside the line text: keep it there.
      (save-excursion (indent-line-to target)))))

;;;; Imenu

(defconst mica--imenu-generic-expression
  '(("Verbs"     "^[ \t]*verb[ \t]+\\([a-zA-Z_][a-zA-Z0-9_]*\\)" 1)
    ("Methods"   "^[ \t]*method[ \t]+\\([a-zA-Z_][a-zA-Z0-9_]*\\)" 1)
    ("Functions" "^[ \t]*let[ \t]+\\([a-zA-Z_][a-zA-Z0-9_]*\\)[ \t]*=[ \t]*fn\\_>" 1)
    ("Relations" "^[ \t]*make_\\(?:functional_\\)?relation(:\\([A-Za-z_][A-Za-z0-9_]*\\)" 1)
    ("Rules"     "^[ \t]*\\([A-Z][A-Za-z0-9_]*\\)(.*)[ \t]*:-" 1))
  "Imenu patterns for `mica-mode'.")

;;;; Mode

;;;###autoload
(define-derived-mode mica-mode prog-mode "Mica"
  "Major mode for editing Mica source files."
  :syntax-table mica-mode-syntax-table
  (setq-local font-lock-defaults '(mica-font-lock-keywords))
  (setq-local comment-start "// ")
  (setq-local comment-start-skip "//+[ \t]*")
  (setq-local comment-end "")
  (setq-local indent-line-function #'mica-indent-line)
  (setq-local indent-region-function #'mica-indent-region)
  (setq-local indent-tabs-mode nil)
  (setq-local imenu-generic-expression mica--imenu-generic-expression))

;;;###autoload
(add-to-list 'auto-mode-alist '("\\.mica\\'" . mica-mode))

;;;; Tests
;;
;; These are loaded unconditionally but only matter when ERT is present.
;; They do not run at load time.

(when (require 'ert nil t)

  (declare-function mica--test-reindent "mica-mode")
  (declare-function mica--test-depth "mica-mode")

  (defun mica--test-reindent (text)
    "Return TEXT reindented from scratch by `mica-mode'."
    (with-temp-buffer
      (insert text)
      (mica-mode)
      ;; Strip existing indentation first so the test exercises the
      ;; calculator rather than merely confirming the input.
      (goto-char (point-min))
      (while (not (eobp))
        (delete-horizontal-space)
        (forward-line 1))
      (indent-region (point-min) (point-max))
      (buffer-string)))

  (defun mica--test-reindent-line (text re)
    "Reindent only the line matching RE in TEXT, leaving the rest as-is.
This exercises the interactive `mica-indent-line' path against a line
that is already (mis-)indented, which `mica--test-reindent' cannot do
because it strips all indentation first."
    (with-temp-buffer
      (insert text)
      (mica-mode)
      (goto-char (point-min))
      (re-search-forward re)
      (mica-indent-line)
      (current-indentation)))

  (defun mica--test-depth (text)
    "Return block depth at end of TEXT in a `mica-mode' buffer."
    (with-temp-buffer
      (insert text)
      (mica-mode)
      (mica--block-depth-before (point-max))))

  (ert-deftest mica-test-keyword-list-matches-lexer ()
    "Sanity: the keyword list is non-empty and excludes non-keywords."
    (should (member "verb" mica--keywords))
    (should (member "assert" mica--keywords))
    (should-not (member "object" mica--keywords))
    (should-not (member "extends" mica--keywords))
    (should-not (member "true" mica--keywords)))

  (ert-deftest mica-test-depth-counts-blocks ()
    (should (= 0 (mica--test-depth "let x = 1\n")))
    (should (= 1 (mica--test-depth "verb f(x)\n")))
    (should (= 2 (mica--test-depth "verb f(x)\n  if y\n")))
    (should (= 1 (mica--test-depth "verb f(x)\n  if y\n  end\n")))
    (should (= 0 (mica--test-depth "verb f(x)\nend\n"))))

  (ert-deftest mica-test-depth-ignores-strings-and-comments ()
    (should (= 0 (mica--test-depth "let s = \"verb end if\"\n")))
    (should (= 0 (mica--test-depth "// verb if for\n"))))

  (ert-deftest mica-test-indent-verb-block ()
    (should (string=
             (concat "verb f(x)\n"
                     "  let y = 1\n"
                     "  if y\n"
                     "    return true\n"
                     "  else\n"
                     "    return false\n"
                     "  end\n"
                     "end\n")
             (mica--test-reindent
              (concat "verb f(x)\n"
                      "let y = 1\n"
                      "if y\n"
                      "return true\n"
                      "else\n"
                      "return false\n"
                      "end\n"
                      "end\n")))))

  (ert-deftest mica-test-indent-nested-blocks ()
    (should (string=
             (concat "verb f(x)\n"
                     "  for item in xs\n"
                     "    if item\n"
                     "      assert Seen(item)\n"
                     "    end\n"
                     "  end\n"
                     "end\n")
             (mica--test-reindent
              (concat "verb f(x)\n"
                      "for item in xs\n"
                      "if item\n"
                      "assert Seen(item)\n"
                      "end\n"
                      "end\n"
                      "end\n")))))

  (ert-deftest mica-test-indent-rule-body ()
    (should (string=
             (concat "CanRead(actor, relation) :-\n"
                     "  HasRole(actor, role),\n"
                     "  RoleCanRead(role, surface)\n")
             (mica--test-reindent
              (concat "CanRead(actor, relation) :-\n"
                      "HasRole(actor, role),\n"
                      "RoleCanRead(role, surface)\n")))))

  (ert-deftest mica-test-indent-top-level-facts ()
    "Bare assert/make_* lines stay at column zero."
    (should (string=
             (concat "make_identity(:lamp)\n"
                     "assert Name(#lamp, \"brass lamp\")\n")
             (mica--test-reindent
              (concat "make_identity(:lamp)\n"
                      "assert Name(#lamp, \"brass lamp\")\n")))))

  (ert-deftest mica-test-dedent-already-indented ()
    "Dedent keywords reindent correctly when the line is already indented.
Regression: `mica--dedent-line-re' is `^'-anchored, so the check must
run at BOL, not after `back-to-indentation'."
    (let ((text (concat "verb f(x)\n"
                        "  if r == 1\n"
                        "    return a\n"
                        "    elseif r == 2\n"   ;; already over-indented
                        "    return b\n"
                        "    else\n"
                        "    return c\n"
                        "    end\n")))
      (should (= 2 (mica--test-reindent-line text "^ *elseif")))
      (should (= 2 (mica--test-reindent-line text "^ *else\\_>")))
      (should (= 2 (mica--test-reindent-line text "^ *end")))))

  (ert-deftest mica-test-depth-recover-block ()
    "`recover ... end' nests; its `end' must not underflow."
    (should (= 1 (mica--test-depth "recover risky()\n")))
    (should (= 0 (mica--test-depth "recover risky()\ncatch E_X => 0\nend\n"))))

  (ert-deftest mica-test-depth-fn-forms ()
    "Block `fn ... end' nests; expression `fn(x) => ...' does not."
    (should (= 1 (mica--test-depth "let f = fn(x)\n")))
    (should (= 0 (mica--test-depth "let f = fn(x)\nend\n")))
    (should (= 0 (mica--test-depth "let f = fn(x) => x\n"))))

  (ert-deftest mica-test-indent-fn-block ()
    (should (string=
             (concat "let f = fn(x)\n"
                     "  return x\n"
                     "end\n")
             (mica--test-reindent
              (concat "let f = fn(x)\n"
                      "return x\n"
                      "end\n")))))

  (ert-deftest mica-test-indent-recover-block ()
    (should (string=
             (concat "verb f(x)\n"
                     "  recover risky()\n"
                     "  catch E_X => 0\n"
                     "  end\n"
                     "end\n")
             (mica--test-reindent
              (concat "verb f(x)\n"
                      "recover risky()\n"
                      "catch E_X => 0\n"
                      "end\n"
                      "end\n")))))

  (ert-deftest mica-test-trailing-comment-in-string ()
    "A // inside a string is not treated as a comment for continuation."
    (with-temp-buffer
      (insert "let u = \"http://x\"\n")
      (mica-mode)
      (goto-char (point-min))
      (should (eq nil (mica--line-trailing)))))

  (ert-deftest mica-test-fontify-identity-and-symbol ()
    ;; `font-lock-ensure' can wedge `kill-emacs' in some batch builds
    ;; (reproducible with builtin modes too), so only run when interactive.
    (skip-unless (not noninteractive))
    (with-temp-buffer
      (insert "assert Name(#lamp, :brass)\n")
      (mica-mode)
      (font-lock-ensure)
      (goto-char (point-min))
      (search-forward "#lamp")
      (should (eq 'font-lock-variable-name-face
                  (get-text-property (- (point) 3) 'face)))
      (goto-char (point-min))
      (search-forward "Name")
      (should (eq 'font-lock-type-face
                  (get-text-property (- (point) 2) 'face)))
      (goto-char (point-min))
      (search-forward "verb" nil t)))

  (ert-deftest mica-test-fontify-keyword ()
    (skip-unless (not noninteractive))
    (with-temp-buffer
      (insert "verb polish(actor @ #user)\n")
      (mica-mode)
      (font-lock-ensure)
      (goto-char (point-min))
      (should (eq 'font-lock-keyword-face (get-text-property 1 'face))))))

(provide 'mica-mode)
;;; mica-mode.el ends here
