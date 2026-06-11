//! Bounded, **correct** subset of PostgreSQL full-text search (FTS).
//!
//! # What is IN scope (implemented correctly)
//!
//! - **Types** `TSVECTOR` / `TSQUERY` — stored as `Utf8` holding the
//!   *canonical text form* (lexemes sorted with positions for a tsvector;
//!   `&`/`|`/`!`/`<->` boolean tree for a tsquery).  Type mapping lives in
//!   `types.rs` / `ddl.rs`.
//! - **`to_tsvector([config,] text)`** — tokenises on non-alphanumeric runs
//!   (preserving digit-hyphen-digit sequences), lowercases, strips English
//!   possessives (`'s`), drops the Basin English stopword set (see
//!   [`STOPWORDS`]), **stems with Snowball English** (Phase 5.20.C), assigns
//!   1-based positions, and emits the PG canonical sorted form.
//! - **`to_tsquery([config,] text)`** — parses `&` `|` `!` and the phrase
//!   operator `<->` (and `<N>` distance) into a boolean tree, **stems each
//!   lexeme** through the config's dictionary (Phase 5.20.E — same Snowball
//!   pipeline as `to_tsvector`, so `to_tsquery('english','runs')` matches a
//!   document containing `running`); emits the PG canonical form with
//!   minimal parentheses.  Raw `::tsquery` casts are NOT stemmed (PG parity).
//! - **`plainto_tsquery([config,] text)`** — tokenises like `to_tsvector`
//!   (stopwords dropped) and AND-joins the remaining lexemes.
//! - **`phraseto_tsquery([config,] text)`** — like `plainto_tsquery` but
//!   joins with the phrase operator `<->`.
//! - **`@@` match** (lowered to `tsvector_match_udf(tsvector, tsquery)` by
//!   `pg_ast::rewrite_tsvector_at_at`) — a **correct** boolean evaluator:
//!   `&` = AND, `|` = OR, `!` = NOT, `<->`/`<N>` = positional adjacency
//!   checked against the tsvector's stored positions.  The tsvector operand
//!   is parsed flexibly: canonical (`'fox':2`) OR a bare token string
//!   (`a fat cat` — the PG `text::tsvector` cast shape).
//! - **`tsvector_to_array(tsvector)`** — `text[]` of distinct lexemes,
//!   sorted, positions stripped.
//! - **`tsquery_phrase(a, b [, distance])`** — phrase-concatenates two
//!   tsqueries with `<->` (or `<N>`).
//! - **`ts_rank` / `ts_rank_cd`** — *simplified deterministic* score:
//!   matched-distinct-lexemes / vector-length.  This is **NOT** PG's
//!   cover-density algorithm; it is documented as a simplification.
//!
//! # What is OUT of scope (honestly unsupported — NOT faked)
//!
//! - **`ts_headline`** (returns the body with matched terms wrapped in
//!   `<b>…</b>` — not PG's cover-density fragment selection).
//! - **Weighted vectors** (`setweight`, A–D weight classes / `:1A`).
//! - **`ts_rank_cd` cover density** (we reuse the simplified `ts_rank`).
//! - **Language configs** beyond `english` / `simple` (the config arg is
//!   accepted; only `simple` changes behaviour — no stemming, no stopwords).
//! - **`websearch_to_tsquery`** (best-effort: treated like `plainto`).
//! - `ts_delete` / `ts_filter` / `numnode` precision / `querytree`.
//!
//! GIN index acceleration for `@@` is no longer out of scope: Phase 5.20.E
//! wires `CREATE INDEX … USING gin (tsvector_col)` into a posting-list
//! registry (`basin_storage::index::gin_tsvector`) consumed by the executor's
//! Empty short-circuit and the session file/row-group pruning paths.  The
//! `@@` UDF here remains the row-level re-evaluation oracle for every
//! candidate row the index admits.
//!
//! The boundary is deliberate: a *correct narrow* FTS beats a broad fake
//! one.  Where a case cannot be made correct it fails honestly rather than
//! returning a plausible-but-wrong match.

use std::any::Any;
use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Int32Array, ListArray, StringArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use rust_stemmers::{Algorithm, Stemmer};

// ===========================================================================
// Stopwords
// ===========================================================================

/// English stopword list (~127 words).  Mirrors the Snowball project's
/// English stopword list, which is what PostgreSQL's `english` text search
/// configuration ships with.  Only `english` (default / unknown configs)
/// drops stopwords; `simple` keeps every token.
///
/// **Must remain sorted** (ASCII lexicographic order, `binary_search` used
/// for O(log n) lookup).  No contractions (apostrophe tokens are never
/// produced by `tokenize()`, so contractions would be unreachable anyway).
pub(crate) const STOPWORDS: &[&str] = &[
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
    "cannot", "could",
    "did", "do", "does", "doing", "down", "during",
    "each",
    "few", "for", "from", "further",
    "get", "got",
    "had", "has", "have", "having", "he", "her", "here", "hers", "herself", "him",
    "himself", "his", "how",
    "i", "if", "in", "into", "is", "it", "its", "itself",
    "just",
    "me", "more", "most", "my", "myself",
    "no", "nor", "not",
    "of", "off", "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own",
    "s", "same", "she", "should", "so", "some", "such",
    "than", "that", "the", "their", "theirs", "them", "themselves", "then", "there", "these",
    "they", "this", "those", "through", "to", "too",
    "under", "until", "up",
    "very",
    "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why",
    "will", "with", "would",
    "you", "your", "yours", "yourself", "yourselves",
];

fn is_stopword(w: &str) -> bool {
    STOPWORDS.binary_search(&w).is_ok()
}

/// Does this text-search config drop stopwords?  `simple` keeps everything;
/// everything else (including `english` and unknown) uses the stopword set.
/// Documented limitation: configs other than `english`/`simple` are NOT
/// distinguished beyond this on/off switch.
fn config_drops_stopwords(config: Option<&str>) -> bool {
    match config {
        Some(c) if c.eq_ignore_ascii_case("simple") => false,
        _ => true,
    }
}

// ===========================================================================
// Language registry + stemming (Phase 5.20.C)
// ===========================================================================

/// Language-specific stemming algorithm. Currently only `English` (Snowball)
/// is implemented; the registry is pluggable for future additions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FtsLanguage {
    /// Snowball English (Porter2) — the default for `to_tsvector('english', …)`.
    English,
    /// Simple: no stemming, no stopword removal.
    Simple,
}

impl FtsLanguage {
    /// Select the language from a PG text-search config name. Unknown configs
    /// fall back to `English` (matching PG's behaviour of using the default
    /// dictionary for unrecognised configs).
    pub(crate) fn from_config(config: Option<&str>) -> Self {
        match config {
            Some(c) if c.eq_ignore_ascii_case("simple") => FtsLanguage::Simple,
            _ => FtsLanguage::English,
        }
    }
}

/// Apply the Snowball English (Porter2) stemmer to a single lowercase token.
/// Returns the stemmed form (which may be identical to the input).
///
/// The stemmer is constructed on each call; the Stemmer struct from
/// `rust-stemmers` is `!Send` so we can't cache a thread-local cheaply
/// across different call sites. Performance-wise construction is O(1)
/// (just a vtable pointer).
fn stem_english(token: &str) -> String {
    let stemmer = Stemmer::create(Algorithm::English);
    stemmer.stem(token).into_owned()
}

/// Stem a token using the given language. For `Simple`, stemming is a no-op.
fn stem_token(token: &str, lang: FtsLanguage) -> String {
    match lang {
        FtsLanguage::English => stem_english(token),
        FtsLanguage::Simple => token.to_owned(),
    }
}

// ===========================================================================
// Tokenisation
// ===========================================================================

/// Tokenise `text` into lowercase tokens for the tsvector document builder.
///
/// Rules (matching Basin's documented FTS tokenization — a practical subset
/// of PG's parser behaviour):
///
/// 1. Split on any non-alphanumeric character, **except** a hyphen that sits
///    between two digit characters (e.g. `2024-01-15` stays as one token).
/// 2. Strip the English possessive suffix `'s` from word-final position
///    (e.g. `John's` → `John`).  The stripped `s` does NOT emit a separate
///    token (Basin documents this as a minor divergence from PG's possessive
///    handling; the position counter is not advanced for the elided suffix).
/// 3. Lowercase every token via Unicode rules.
/// 4. Discard tokens that are empty after normalization.
///
/// Stemming is applied separately (in `from_text_document`) after stopword
/// filtering, so this function returns the post-possessive-strip lowercase
/// tokens without stemming.
fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len();
    let mut i = 0usize;

    while i < len {
        // Skip non-alphanumeric chars (token separators).
        if !chars[i].is_alphanumeric() {
            i += 1;
            continue;
        }

        // Collect an alphanumeric run, but allow hyphen between digits.
        let start = i;
        let mut buf = String::new();
        while i < len {
            let c = chars[i];
            if c.is_alphanumeric() {
                buf.push(c);
                i += 1;
            } else if c == '-'
                && i + 1 < len
                && chars[i - 1].is_ascii_digit()
                && chars[i + 1].is_ascii_digit()
            {
                // Digit-hyphen-digit: keep hyphen as part of the token
                // (e.g. date literals like 2024-01-15).
                buf.push(c);
                i += 1;
            } else {
                break;
            }
        }

        if buf.is_empty() {
            // Shouldn't happen given the outer guard, but be safe.
            i = i.max(start + 1);
            continue;
        }

        // Strip English possessive suffix `'s` (case-insensitive).
        // We look at the character immediately after `buf` ends: if the
        // source has `'s` (apostrophe + s + non-alphanumeric or EOS) right
        // after our token, consume those chars and don't add them to the
        // token. The `s` is silently elided (no separate token).
        if i < len && chars[i] == '\'' {
            // Peek: is the next char 's' or 'S' and then non-alpha-or-EOS?
            if i + 1 < len && (chars[i + 1] == 's' || chars[i + 1] == 'S') {
                let after_s = i + 2;
                let end_of_word = after_s >= len || !chars[after_s].is_alphanumeric();
                if end_of_word {
                    // Consume `'s` and skip the `s` token entirely.
                    i += 2;
                }
            }
        }

        // Lowercase.
        let lower = buf.to_lowercase();
        if !lower.is_empty() {
            tokens.push(lower);
        }
    }
    tokens
}

// ===========================================================================
// tsvector model
// ===========================================================================

/// A parsed tsvector: lexeme -> sorted distinct 1-based positions.
/// Positions may be empty (e.g. for the bare `text::tsvector` cast form
/// `'a fat cat'` PG assigns no positions either).
#[derive(Debug, Clone, Default)]
struct TsVector {
    /// Sorted by lexeme; positions sorted ascending and de-duplicated.
    entries: Vec<(String, Vec<u32>)>,
}

impl TsVector {
    fn from_text_document(text: &str, config: Option<&str>) -> Self {
        let lang = FtsLanguage::from_config(config);
        let drop_sw = lang != FtsLanguage::Simple;
        let mut map: std::collections::BTreeMap<String, Vec<u32>> =
            std::collections::BTreeMap::new();
        let mut pos: u32 = 0;
        for tok in tokenize(text) {
            pos += 1; // Position counter advances for every token (including stopwords).
            if drop_sw && is_stopword(&tok) {
                continue;
            }
            // Apply Snowball English stemming (Phase 5.20.C).
            let lexeme = stem_token(&tok, lang);
            if lexeme.is_empty() {
                continue;
            }
            map.entry(lexeme).or_default().push(pos);
        }
        let entries = map
            .into_iter()
            .map(|(lex, mut ps)| {
                ps.sort_unstable();
                ps.dedup();
                (lex, ps)
            })
            .collect();
        TsVector { entries }
    }

    /// Parse a tsvector from its stored text form.  Accepts BOTH:
    ///   * canonical form  `'fox':2 'quick':1,3`
    ///   * bare token form `a fat cat`  (the `text::tsvector` cast shape —
    ///     whitespace-split, no positions, NOT stopword-filtered, lowercased
    ///     to match canonical lexemes)
    fn parse(s: &str) -> Self {
        let s = s.trim();
        if s.is_empty() {
            return TsVector::default();
        }
        // Heuristic: canonical form always quotes lexemes with `'`.
        if s.contains('\'') {
            return Self::parse_canonical(s);
        }
        // Bare token form: split on whitespace, lowercase, no positions.
        let mut map: std::collections::BTreeMap<String, Vec<u32>> =
            std::collections::BTreeMap::new();
        for tok in s.split_whitespace() {
            map.entry(tok.to_lowercase()).or_default();
        }
        TsVector {
            entries: map.into_iter().collect(),
        }
    }

    /// Parse the canonical `'lex':p,p 'lex2':p` form.  Tolerant of missing
    /// positions (`'lex' 'lex2'`).
    fn parse_canonical(s: &str) -> Self {
        let bytes = s.as_bytes();
        let mut i = 0usize;
        let mut map: std::collections::BTreeMap<String, BTreeSet<u32>> =
            std::collections::BTreeMap::new();
        while i < bytes.len() {
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i >= bytes.len() {
                break;
            }
            // Lexeme: quoted '...' (escaped '' inside) or bare word.
            let lexeme = if bytes[i] == b'\'' {
                i += 1;
                let mut buf = String::new();
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            buf.push('\'');
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        buf.push(bytes[i] as char);
                        i += 1;
                    }
                }
                buf
            } else {
                let start = i;
                while i < bytes.len() && !bytes[i].is_ascii_whitespace() && bytes[i] != b':' {
                    i += 1;
                }
                s[start..i].to_string()
            };
            let mut positions: Vec<u32> = Vec::new();
            if i < bytes.len() && bytes[i] == b':' {
                i += 1;
                loop {
                    let start = i;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                    if start != i {
                        if let Ok(p) = s[start..i].parse::<u32>() {
                            positions.push(p);
                        }
                    }
                    // Skip an optional weight letter (A-D) — weights are OUT
                    // of scope; we parse past them so the position is read.
                    if i < bytes.len() && bytes[i].is_ascii_alphabetic() {
                        i += 1;
                    }
                    if i < bytes.len() && bytes[i] == b',' {
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            if !lexeme.is_empty() {
                let e = map.entry(lexeme).or_default();
                for p in positions {
                    e.insert(p);
                }
            }
        }
        TsVector {
            entries: map
                .into_iter()
                .map(|(l, ps)| (l, ps.into_iter().collect()))
                .collect(),
        }
    }

    /// PG canonical text form: lexemes sorted, each `'lex'` or
    /// `'lex':p1,p2`, space-separated.
    fn to_canonical(&self) -> String {
        let mut parts = Vec::with_capacity(self.entries.len());
        for (lex, ps) in &self.entries {
            let q = quote_lexeme(lex);
            if ps.is_empty() {
                parts.push(q);
            } else {
                let posstr = ps
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                parts.push(format!("{q}:{posstr}"));
            }
        }
        parts.join(" ")
    }

    fn contains(&self, lexeme: &str) -> bool {
        self.entries
            .binary_search_by(|(l, _)| l.as_str().cmp(lexeme))
            .is_ok()
    }

    fn positions(&self, lexeme: &str) -> &[u32] {
        match self
            .entries
            .binary_search_by(|(l, _)| l.as_str().cmp(lexeme))
        {
            Ok(idx) => &self.entries[idx].1,
            Err(_) => &[],
        }
    }

    fn lexemes_sorted(&self) -> Vec<String> {
        self.entries.iter().map(|(l, _)| l.clone()).collect()
    }
}

/// Quote a lexeme for canonical output.  PG always single-quotes lexemes in
/// the text form, doubling embedded quotes.
fn quote_lexeme(lex: &str) -> String {
    format!("'{}'", lex.replace('\'', "''"))
}

// ===========================================================================
// tsquery model
// ===========================================================================

#[derive(Debug, Clone, PartialEq)]
enum TsQuery {
    /// A single lexeme term.
    Term(String),
    /// `!q`
    Not(Box<TsQuery>),
    /// `a & b`
    And(Box<TsQuery>, Box<TsQuery>),
    /// `a | b`
    Or(Box<TsQuery>, Box<TsQuery>),
    /// `a <N> b` — phrase with distance N (N>=1; `<->` == `<1>`).
    Phrase(Box<TsQuery>, Box<TsQuery>, u32),
}

impl TsQuery {
    /// Parse a `to_tsquery`-style expression: lexemes joined by `&` `|`,
    /// `!` prefix negation, `<->` / `<N>` phrase, parentheses.  Tokens are
    /// lowercased; **no stemming at this layer** — the `to_tsquery` UDF /
    /// canonicalisation applies [`TsQuery::stem_terms`] afterwards so query
    /// lexemes go through the SAME Snowball pipeline as `to_tsvector`
    /// (PG stems `to_tsquery` terms; a raw `::tsquery` cast does not, which
    /// is why stemming lives outside this parser).  Stopwords inside an
    /// explicit `to_tsquery` are kept (PG drops them with a notice — we keep
    /// the boolean structure intact which is the safe/correct-for-match
    /// choice and is documented).
    fn parse_to_tsquery(input: &str) -> Result<Option<TsQuery>, String> {
        let toks = lex_query(input)?;
        if toks.is_empty() {
            return Ok(None);
        }
        let mut p = QParser { toks, pos: 0 };
        let q = p.parse_or()?;
        if p.pos != p.toks.len() {
            return Err(format!(
                "unexpected token in tsquery near {:?}",
                p.toks.get(p.pos)
            ));
        }
        Ok(Some(q))
    }

    /// `plainto_tsquery`: AND of the (stopword-filtered) lexemes.
    fn plainto(text: &str, config: Option<&str>) -> Option<TsQuery> {
        Self::join_tokens(text, config, JoinKind::And)
    }

    /// `phraseto_tsquery`: `<->` chain of the (stopword-filtered) lexemes.
    fn phraseto(text: &str, config: Option<&str>) -> Option<TsQuery> {
        Self::join_tokens(text, config, JoinKind::Phrase)
    }

    fn join_tokens(text: &str, config: Option<&str>, kind: JoinKind) -> Option<TsQuery> {
        let lang = FtsLanguage::from_config(config);
        let drop_sw = lang != FtsLanguage::Simple;
        let lexemes: Vec<String> = tokenize(text)
            .into_iter()
            .filter(|t| !(drop_sw && is_stopword(t)))
            .map(|t| stem_token(&t, lang))
            .filter(|t| !t.is_empty())
            .collect();
        let mut iter = lexemes.into_iter();
        let mut acc = TsQuery::Term(iter.next()?);
        for t in iter {
            acc = match kind {
                JoinKind::And => TsQuery::And(Box::new(acc), Box::new(TsQuery::Term(t))),
                JoinKind::Phrase => TsQuery::Phrase(Box::new(acc), Box::new(TsQuery::Term(t)), 1),
            };
        }
        Some(acc)
    }

    /// Stem every lexeme term through the language's dictionary, preserving
    /// the boolean structure.  This is the query-side half of the stemming
    /// contract: `to_tsvector('english', 'running')` stores `'run'`, so
    /// `to_tsquery('english', 'runs')` must probe/match `'run'` too — a stem
    /// mismatch silently breaks both `@@` matching and GIN-prune soundness.
    /// `Simple` is a no-op.  Applied by the `to_tsquery` UDF and the
    /// `to_tsquery_text` canonicalisation, NOT by raw `::tsquery` casts
    /// (PG does not stem direct casts either).
    fn stem_terms(self, lang: FtsLanguage) -> TsQuery {
        match self {
            TsQuery::Term(t) => {
                let stemmed = stem_token(&t, lang);
                // A term that stems to nothing keeps its original form —
                // never silently drop a boolean operand.
                if stemmed.is_empty() {
                    TsQuery::Term(t)
                } else {
                    TsQuery::Term(stemmed)
                }
            }
            TsQuery::Not(q) => TsQuery::Not(Box::new(q.stem_terms(lang))),
            TsQuery::And(a, b) => TsQuery::And(
                Box::new(a.stem_terms(lang)),
                Box::new(b.stem_terms(lang)),
            ),
            TsQuery::Or(a, b) => TsQuery::Or(
                Box::new(a.stem_terms(lang)),
                Box::new(b.stem_terms(lang)),
            ),
            TsQuery::Phrase(a, b, n) => TsQuery::Phrase(
                Box::new(a.stem_terms(lang)),
                Box::new(b.stem_terms(lang)),
                n,
            ),
        }
    }

    /// PG canonical text form with minimal parentheses.
    fn to_canonical(&self) -> String {
        self.fmt(0)
    }

    /// Precedence (higher binds tighter): `|`=1, `&`=2, `<->`=3, `!`=4.
    fn prec(&self) -> u8 {
        match self {
            TsQuery::Or(..) => 1,
            TsQuery::And(..) => 2,
            TsQuery::Phrase(..) => 3,
            TsQuery::Not(_) => 4,
            TsQuery::Term(_) => 5,
        }
    }

    fn fmt(&self, parent_prec: u8) -> String {
        let s = match self {
            TsQuery::Term(t) => quote_lexeme(t),
            TsQuery::Not(q) => format!("!{}", q.fmt(4)),
            TsQuery::And(a, b) => format!("{} & {}", a.fmt(2), b.fmt(2)),
            TsQuery::Or(a, b) => format!("{} | {}", a.fmt(1), b.fmt(1)),
            TsQuery::Phrase(a, b, n) => {
                let op = if *n == 1 {
                    "<->".to_string()
                } else {
                    format!("<{n}>")
                };
                format!("{} {} {}", a.fmt(3), op, b.fmt(3))
            }
        };
        if self.prec() < parent_prec {
            format!("({s})")
        } else {
            s
        }
    }

    /// Evaluate this query against `tv` for the `@@` operator.  Returns
    /// `true` iff the tsvector satisfies the boolean tree.
    ///
    /// Boolean operators are evaluated set-theoretically over the tsvector's
    /// lexeme set.  The phrase operator `<N>` is evaluated **positionally**:
    /// the right operand must occur at a position exactly `N` greater than a
    /// matching position of the left operand.  A phrase therefore returns,
    /// in addition to a boolean, the set of end-positions at which it matched
    /// so that chained phrases (`a <-> b <-> c`) compose correctly.
    fn matches(&self, tv: &TsVector) -> bool {
        !self.eval_positions(tv).is_empty_match()
    }

    fn eval_positions(&self, tv: &TsVector) -> PhraseEval {
        match self {
            TsQuery::Term(t) => {
                if tv.contains(t) {
                    let ps = tv.positions(t);
                    if ps.is_empty() {
                        // Lexeme present but position-less (bare cast form):
                        // matches as a plain boolean term; phrase adjacency
                        // against it is impossible (no positions).
                        PhraseEval::matched_no_positions()
                    } else {
                        PhraseEval::matched(ps.iter().copied().collect())
                    }
                } else {
                    PhraseEval::no_match()
                }
            }
            TsQuery::Not(q) => {
                // NOT is purely boolean (positions are meaningless for it;
                // PG also forbids `!` directly inside a phrase, which we do
                // not special-case beyond returning a boolean here).
                if q.matches(tv) {
                    PhraseEval::no_match()
                } else {
                    PhraseEval::matched_no_positions()
                }
            }
            TsQuery::And(a, b) => {
                if a.matches(tv) && b.matches(tv) {
                    PhraseEval::matched_no_positions()
                } else {
                    PhraseEval::no_match()
                }
            }
            TsQuery::Or(a, b) => {
                if a.matches(tv) || b.matches(tv) {
                    PhraseEval::matched_no_positions()
                } else {
                    PhraseEval::no_match()
                }
            }
            TsQuery::Phrase(a, b, n) => {
                let left = a.eval_positions(tv);
                let right = b.eval_positions(tv);
                if !left.matched || !right.matched {
                    return PhraseEval::no_match();
                }
                // Positional adjacency: need a left end-pos `p` and a right
                // end-pos `q` with `q - p == n`.  If either side has no
                // positions (bare cast / NOT / boolean sub-tree), positional
                // adjacency is undefined → no phrase match (honest: we will
                // NOT fake adjacency we cannot verify).
                if left.positions.is_empty() || right.positions.is_empty() {
                    return PhraseEval::no_match();
                }
                let mut out = BTreeSet::new();
                for &p in &left.positions {
                    if right.positions.contains(&(p + *n)) {
                        out.insert(p + *n);
                    }
                }
                if out.is_empty() {
                    PhraseEval::no_match()
                } else {
                    PhraseEval {
                        matched: true,
                        positions: out,
                    }
                }
            }
        }
    }
}

/// Result of evaluating a (sub-)query for phrase composition.
struct PhraseEval {
    matched: bool,
    /// End-positions at which this sub-query matched.  Empty + `matched`
    /// means "matched as a boolean but carries no positional information"
    /// (e.g. a NOT/AND/OR sub-tree, or a position-less bare-cast lexeme).
    positions: BTreeSet<u32>,
}

impl PhraseEval {
    fn no_match() -> Self {
        PhraseEval {
            matched: false,
            positions: BTreeSet::new(),
        }
    }
    fn matched(positions: BTreeSet<u32>) -> Self {
        PhraseEval {
            matched: true,
            positions,
        }
    }
    fn matched_no_positions() -> Self {
        PhraseEval {
            matched: true,
            positions: BTreeSet::new(),
        }
    }
    fn is_empty_match(&self) -> bool {
        !self.matched
    }
}

enum JoinKind {
    And,
    Phrase,
}

// --- tsquery lexer + recursive-descent parser ------------------------------

#[derive(Debug, Clone, PartialEq)]
enum QTok {
    Term(String),
    And,
    Or,
    Not,
    Phrase(u32),
    LParen,
    RParen,
}

/// Lex a `to_tsquery` string into tokens.  Terms are quoted (`'foo bar'`)
/// or bare alphanumeric runs (lowercased).  Operators: `&` `|` `!` `(` `)`
/// `<->` `<N>`.
fn lex_query(input: &str) -> Result<Vec<QTok>, String> {
    let b = input.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        match c {
            b'&' => {
                out.push(QTok::And);
                i += 1;
            }
            b'|' => {
                out.push(QTok::Or);
                i += 1;
            }
            b'!' => {
                out.push(QTok::Not);
                i += 1;
            }
            b'(' => {
                out.push(QTok::LParen);
                i += 1;
            }
            b')' => {
                out.push(QTok::RParen);
                i += 1;
            }
            b'<' => {
                // <-> or <N>
                i += 1;
                if i < b.len() && b[i] == b'-' {
                    i += 1;
                    if i < b.len() && b[i] == b'>' {
                        i += 1;
                        out.push(QTok::Phrase(1));
                    } else {
                        return Err("malformed phrase operator (expected <->)".into());
                    }
                } else {
                    let start = i;
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                    if start == i || i >= b.len() || b[i] != b'>' {
                        return Err("malformed phrase operator (expected <N>)".into());
                    }
                    let n: u32 = input[start..i]
                        .parse()
                        .map_err(|_| "invalid phrase distance".to_string())?;
                    i += 1;
                    out.push(QTok::Phrase(n.max(1)));
                }
            }
            b'\'' => {
                // Quoted term: '' is an embedded quote.
                i += 1;
                let mut buf = String::new();
                loop {
                    if i >= b.len() {
                        return Err("unterminated quoted lexeme in tsquery".into());
                    }
                    if b[i] == b'\'' {
                        if i + 1 < b.len() && b[i + 1] == b'\'' {
                            buf.push('\'');
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        buf.push(b[i] as char);
                        i += 1;
                    }
                }
                let lx = buf.to_lowercase();
                if !lx.is_empty() {
                    out.push(QTok::Term(lx));
                }
            }
            _ => {
                // Bare term: alphanumeric run (any non-alnum ends it).
                let start = i;
                while i < b.len() {
                    let ch = input[i..].chars().next().unwrap();
                    if ch.is_alphanumeric() {
                        i += ch.len_utf8();
                    } else {
                        break;
                    }
                }
                if start == i {
                    return Err(format!("unexpected character {:?} in tsquery", c as char));
                }
                out.push(QTok::Term(input[start..i].to_lowercase()));
            }
        }
    }
    Ok(out)
}

struct QParser {
    toks: Vec<QTok>,
    pos: usize,
}

impl QParser {
    fn peek(&self) -> Option<&QTok> {
        self.toks.get(self.pos)
    }
    fn bump(&mut self) -> Option<QTok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    // or := and ('|' and)*
    fn parse_or(&mut self) -> Result<TsQuery, String> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Some(QTok::Or)) {
            self.bump();
            let right = self.parse_and()?;
            left = TsQuery::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // and := phrase ('&' phrase)*
    fn parse_and(&mut self) -> Result<TsQuery, String> {
        let mut left = self.parse_phrase()?;
        while matches!(self.peek(), Some(QTok::And)) {
            self.bump();
            let right = self.parse_phrase()?;
            left = TsQuery::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // phrase := unary ('<N>' unary)*
    fn parse_phrase(&mut self) -> Result<TsQuery, String> {
        let mut left = self.parse_unary()?;
        while let Some(QTok::Phrase(n)) = self.peek().cloned() {
            self.bump();
            let right = self.parse_unary()?;
            left = TsQuery::Phrase(Box::new(left), Box::new(right), n);
        }
        Ok(left)
    }

    // unary := '!' unary | '(' or ')' | term
    fn parse_unary(&mut self) -> Result<TsQuery, String> {
        match self.peek() {
            Some(QTok::Not) => {
                self.bump();
                Ok(TsQuery::Not(Box::new(self.parse_unary()?)))
            }
            Some(QTok::LParen) => {
                self.bump();
                let q = self.parse_or()?;
                match self.bump() {
                    Some(QTok::RParen) => Ok(q),
                    _ => Err("unbalanced parenthesis in tsquery".into()),
                }
            }
            Some(QTok::Term(_)) => {
                if let Some(QTok::Term(t)) = self.bump() {
                    Ok(TsQuery::Term(t))
                } else {
                    unreachable!()
                }
            }
            other => Err(format!("unexpected token in tsquery: {other:?}")),
        }
    }
}

// ===========================================================================
// Pure helpers reused by the INSERT path (dml.rs)
// ===========================================================================

/// Compute the canonical tsvector text for `to_tsvector([config,] body)`.
/// Used by the INSERT coercion so a `to_tsvector(...)` value expression
/// targeting a `TSVECTOR` column stores the canonical form.
pub(crate) fn to_tsvector_text(config: Option<&str>, body: &str) -> String {
    TsVector::from_text_document(body, config).to_canonical()
}

/// Canonicalise a raw tsvector text (the `text::tsvector` cast shape) into
/// the stored canonical form.
pub(crate) fn canonicalize_tsvector_text(raw: &str) -> String {
    TsVector::parse(raw).to_canonical()
}

/// Compute the canonical tsquery text for `to_tsquery`/`plainto_tsquery`/
/// `phraseto_tsquery`.  `func` selects the parser.  Returns `Err` for an
/// unparseable `to_tsquery` (honest — never a fake).
pub(crate) fn to_tsquery_text(
    func: &str,
    config: Option<&str>,
    body: &str,
) -> Result<String, String> {
    let q = match func {
        // PG stems explicit `to_tsquery` lexemes through the config's
        // dictionary (`to_tsquery('english','runs')` → `'run'`).  Stemming
        // here keeps query lexemes in the same Snowball universe as the
        // `to_tsvector` document side — required for `@@` matching AND for
        // GIN posting-list probe soundness (the probe consumes this
        // canonical form via `index_probe::detect_tsvector_match`).
        "to_tsquery" => TsQuery::parse_to_tsquery(body)?
            .map(|q| q.stem_terms(FtsLanguage::from_config(config))),
        "phraseto_tsquery" => TsQuery::phraseto(body, config),
        _ => TsQuery::plainto(body, config),
    };
    Ok(q.map(|q| q.to_canonical()).unwrap_or_default())
}

/// Canonicalise a raw tsquery text into the stored canonical form.
pub(crate) fn canonicalize_tsquery_text(raw: &str) -> Result<String, String> {
    Ok(TsQuery::parse_to_tsquery(raw)?
        .map(|q| q.to_canonical())
        .unwrap_or_default())
}

// ===========================================================================
// Registration
// ===========================================================================

/// Register all FTS UDFs on `ctx`.  Idempotent — DataFusion overwrites by
/// name, so calling repeatedly is safe.
pub(crate) fn register_fts_udfs(ctx: &SessionContext) {
    let ts1 = TypeSignature::Exact(vec![DataType::Utf8]);
    let ts2 = TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]);
    let ts3 = TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]);

    let one_or_two = || Signature::one_of(vec![ts1.clone(), ts2.clone()], Volatility::Immutable);

    ctx.register_udf(ScalarUDF::from(ToTsvectorUdf {
        signature: one_or_two(),
    }));
    ctx.register_udf(ScalarUDF::from(ToTsqueryUdf {
        name: "to_tsquery".into(),
        signature: one_or_two(),
    }));
    ctx.register_udf(ScalarUDF::from(ToTsqueryUdf {
        name: "plainto_tsquery".into(),
        signature: one_or_two(),
    }));
    ctx.register_udf(ScalarUDF::from(ToTsqueryUdf {
        name: "phraseto_tsquery".into(),
        signature: one_or_two(),
    }));
    // websearch_to_tsquery: OUT of scope as a real parser; best-effort
    // treated like plainto (documented).
    ctx.register_udf(ScalarUDF::from(ToTsqueryUdf {
        name: "websearch_to_tsquery".into(),
        signature: one_or_two(),
    }));

    // @@ match — `tsvector_match_udf` is the rewrite target of
    // `pg_ast::rewrite_tsvector_at_at`.  `tsvector_match` is the explicit
    // function-name alias (same correct semantics now — the old always-false
    // stub is gone).
    ctx.register_udf(ScalarUDF::from(TsMatchUdf {
        name: "tsvector_match_udf".into(),
        signature: Signature::one_of(vec![ts2.clone()], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(TsMatchUdf {
        name: "tsvector_match".into(),
        signature: Signature::one_of(vec![ts2.clone()], Volatility::Immutable),
    }));

    ctx.register_udf(ScalarUDF::from(TsvectorToArrayUdf {
        signature: Signature::one_of(vec![ts1.clone()], Volatility::Immutable),
    }));

    ctx.register_udf(ScalarUDF::from(TsqueryPhraseUdf {
        signature: Signature::one_of(vec![ts2.clone(), ts3.clone()], Volatility::Immutable),
    }));

    // Simplified deterministic ranking (NOT cover density).
    ctx.register_udf(ScalarUDF::from(TsRankUdf {
        name: "ts_rank".into(),
        signature: sig_rank(),
    }));
    ctx.register_udf(ScalarUDF::from(TsRankUdf {
        name: "ts_rank_cd".into(),
        signature: sig_rank(),
    }));

    // ts_headline — minimal highlighting: wraps matched query terms in <b>…</b>
    // (full body, not PG cover-density fragments). See `headline_highlight`.
    ctx.register_udf(ScalarUDF::from(TsHeadlineUdf {
        signature: Signature::one_of(
            vec![
                ts2.clone(),
                ts3.clone(),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ]),
            ],
            Volatility::Immutable,
        ),
    }));

    // strip(tsvector) -> tsvector with positions removed (correct).
    ctx.register_udf(ScalarUDF::from(StripUdf {
        signature: Signature::one_of(vec![ts1.clone()], Volatility::Immutable),
    }));

    // tsvector_length(tsvector) -> distinct lexeme count (correct).
    ctx.register_udf(ScalarUDF::from(TsvectorLengthUdf {
        signature: Signature::one_of(vec![ts1.clone()], Volatility::Immutable),
    }));

    // numnode / querytree — minimal, documented as approximate.
    ctx.register_udf(ScalarUDF::from(NumnodeUdf {
        signature: Signature::one_of(vec![ts1.clone()], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(QuerytreeUdf {
        signature: Signature::one_of(vec![ts1.clone()], Volatility::Immutable),
    }));

    let _ = (ts1, ts2, ts3);
}

fn sig_rank() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
        ],
        Volatility::Immutable,
    )
}

// ===========================================================================
// Argument helpers
// ===========================================================================

fn num_rows(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

fn arg_strings(arg: &ColumnarValue, n: usize) -> DFResult<StringArray> {
    let arr = arg.clone().into_array(n)?;
    let arr = if arr.data_type() == &DataType::Utf8 {
        arr
    } else {
        datafusion::arrow::compute::cast(&arr, &DataType::Utf8)?
    };
    Ok(arr
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 yields StringArray")
        .clone())
}

// ===========================================================================
// to_tsvector
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ToTsvectorUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToTsvectorUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_tsvector"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        // 1-arg: (text); 2-arg: (config, text).
        let (config_arr, body_arr) = if args.len() == 2 {
            (Some(arg_strings(&args[0], n)?), arg_strings(&args[1], n)?)
        } else {
            (None, arg_strings(&args[0], n)?)
        };
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if body_arr.is_null(i) {
                out.push(None);
                continue;
            }
            let cfg =
                config_arr
                    .as_ref()
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) });
            let tv = TsVector::from_text_document(body_arr.value(i), cfg);
            out.push(Some(tv.to_canonical()));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ===========================================================================
// to_tsquery / plainto_tsquery / phraseto_tsquery / websearch_to_tsquery
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ToTsqueryUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for ToTsqueryUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let (config_arr, body_arr) = if args.len() == 2 {
            (Some(arg_strings(&args[0], n)?), arg_strings(&args[1], n)?)
        } else {
            (None, arg_strings(&args[0], n)?)
        };
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if body_arr.is_null(i) {
                out.push(None);
                continue;
            }
            let cfg =
                config_arr
                    .as_ref()
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) });
            let text = body_arr.value(i);
            let q: Option<TsQuery> = match self.name.as_str() {
                // Stem explicit-query lexemes through the config dictionary
                // (same pipeline as to_tsvector) — see `to_tsquery_text`.
                "to_tsquery" => TsQuery::parse_to_tsquery(text)
                    .map_err(|e| {
                        datafusion::common::DataFusionError::Execution(format!(
                            "to_tsquery: {e}"
                        ))
                    })?
                    .map(|q| q.stem_terms(FtsLanguage::from_config(cfg))),
                "phraseto_tsquery" => TsQuery::phraseto(text, cfg),
                // plainto / websearch (best-effort) → implicit AND.
                _ => TsQuery::plainto(text, cfg),
            };
            out.push(Some(q.map(|q| q.to_canonical()).unwrap_or_default()));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ===========================================================================
// @@ match
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TsMatchUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for TsMatchUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let lhs = arg_strings(&args[0], n)?;
        let rhs = arg_strings(&args[1], n)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if lhs.is_null(i) || rhs.is_null(i) {
                out.push(None);
                continue;
            }
            let tv = TsVector::parse(lhs.value(i));
            // The query side may be a canonical tsquery (`'a' & 'b'`) OR a
            // bare word string (`'cat'::tsquery` → `cat`).  Both parse with
            // the same to_tsquery grammar (bare words are valid terms).
            let qres = TsQuery::parse_to_tsquery(rhs.value(i));
            let m = match qres {
                Ok(Some(q)) => q.matches(&tv),
                Ok(None) => false, // empty query matches nothing (PG: warns + no rows)
                Err(_) => false,   // unparseable query → no match (honest)
            };
            out.push(Some(m));
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out))))
    }
}

// ===========================================================================
// tsvector_to_array
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TsvectorToArrayUdf {
    signature: Signature,
}

impl ScalarUDFImpl for TsvectorToArrayUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "tsvector_to_array"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let input = arg_strings(&args[0], n)?;
        let mut values: Vec<Option<String>> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(n + 1);
        offsets.push(0);
        let mut nulls = Vec::with_capacity(n);
        for i in 0..n {
            if input.is_null(i) {
                nulls.push(false);
                offsets.push(values.len() as i32);
                continue;
            }
            nulls.push(true);
            let tv = TsVector::parse(input.value(i));
            for lex in tv.lexemes_sorted() {
                values.push(Some(lex));
            }
            offsets.push(values.len() as i32);
        }
        let value_arr: ArrayRef = Arc::new(StringArray::from(values));
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list = ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            value_arr,
            Some(nulls.into()),
        )
        .map_err(|e| {
            datafusion::common::DataFusionError::Execution(format!("tsvector_to_array: {e}"))
        })?;
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

// ===========================================================================
// tsquery_phrase
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TsqueryPhraseUdf {
    signature: Signature,
}

impl ScalarUDFImpl for TsqueryPhraseUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "tsquery_phrase"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let a = arg_strings(&args[0], n)?;
        let b = arg_strings(&args[1], n)?;
        let dist = if args.len() == 3 {
            Some(arg_strings(&args[2], n)?)
        } else {
            None
        };
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if a.is_null(i) || b.is_null(i) {
                out.push(None);
                continue;
            }
            let qa = TsQuery::parse_to_tsquery(a.value(i)).ok().flatten();
            let qb = TsQuery::parse_to_tsquery(b.value(i)).ok().flatten();
            let dn: u32 = match &dist {
                Some(d) if !d.is_null(i) => d.value(i).trim().parse().unwrap_or(1).max(1),
                _ => 1,
            };
            match (qa, qb) {
                (Some(qa), Some(qb)) => {
                    let q = TsQuery::Phrase(Box::new(qa), Box::new(qb), dn);
                    out.push(Some(q.to_canonical()));
                }
                _ => out.push(Some(String::new())),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ===========================================================================
// ts_rank / ts_rank_cd  (simplified deterministic — NOT cover density)
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TsRankUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for TsRankUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float32)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        // 2-arg: (tsvector, tsquery); 3-arg: (weights, tsvector, tsquery) —
        // weights ignored (weights are OUT of scope).
        let (tv_arg, tq_arg) = if args.len() == 3 {
            (&args[1], &args[2])
        } else {
            (&args[0], &args[1])
        };
        let tv = arg_strings(tv_arg, n)?;
        let tq = arg_strings(tq_arg, n)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if tv.is_null(i) || tq.is_null(i) {
                out.push(0.0f32);
                continue;
            }
            let v = TsVector::parse(tv.value(i));
            let q = TsQuery::parse_to_tsquery(tq.value(i)).ok().flatten();
            let score = match q {
                Some(q) if q.matches(&v) => {
                    let total = v.entries.len().max(1) as f32;
                    let hits = query_terms(&q).iter().filter(|t| v.contains(t)).count() as f32;
                    hits / total
                }
                _ => 0.0,
            };
            out.push(score);
        }
        Ok(ColumnarValue::Array(Arc::new(Float32Array::from(out))))
    }
}

/// Distinct lexemes referenced anywhere in a query tree.
fn query_terms(q: &TsQuery) -> BTreeSet<String> {
    let mut s = BTreeSet::new();
    fn walk(q: &TsQuery, s: &mut BTreeSet<String>) {
        match q {
            TsQuery::Term(t) => {
                s.insert(t.clone());
            }
            TsQuery::Not(a) => walk(a, s),
            TsQuery::And(a, b) | TsQuery::Or(a, b) | TsQuery::Phrase(a, b, _) => {
                walk(a, s);
                walk(b, s);
            }
        }
    }
    walk(q, &mut s);
    s
}

// ===========================================================================
// strip(tsvector) -> tsvector without positions  (correct)
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StripUdf {
    signature: Signature,
}

impl ScalarUDFImpl for StripUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "strip"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let input = arg_strings(&args[0], n)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if input.is_null(i) {
                out.push(None);
                continue;
            }
            let mut tv = TsVector::parse(input.value(i));
            for (_, ps) in tv.entries.iter_mut() {
                ps.clear();
            }
            out.push(Some(tv.to_canonical()));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ===========================================================================
// tsvector_length(tsvector) -> distinct lexeme count  (correct)
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TsvectorLengthUdf {
    signature: Signature,
}

impl ScalarUDFImpl for TsvectorLengthUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "tsvector_length"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int32)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let input = arg_strings(&args[0], n)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if input.is_null(i) {
                out.push(0i32);
            } else {
                out.push(TsVector::parse(input.value(i)).entries.len() as i32);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(Int32Array::from(out))))
    }
}

// ===========================================================================
// numnode / querytree  (minimal — documented approximations)
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NumnodeUdf {
    signature: Signature,
}

impl ScalarUDFImpl for NumnodeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "numnode"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int32)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let input = arg_strings(&args[0], n)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if input.is_null(i) {
                out.push(0i32);
                continue;
            }
            let cnt = match TsQuery::parse_to_tsquery(input.value(i)) {
                Ok(Some(q)) => count_nodes(&q),
                _ => 0,
            };
            out.push(cnt);
        }
        Ok(ColumnarValue::Array(Arc::new(Int32Array::from(out))))
    }
}

fn count_nodes(q: &TsQuery) -> i32 {
    match q {
        TsQuery::Term(_) => 1,
        TsQuery::Not(a) => 1 + count_nodes(a),
        TsQuery::And(a, b) | TsQuery::Or(a, b) | TsQuery::Phrase(a, b, _) => {
            1 + count_nodes(a) + count_nodes(b)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QuerytreeUdf {
    signature: Signature,
}

impl ScalarUDFImpl for QuerytreeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "querytree"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let input = arg_strings(&args[0], n)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if input.is_null(i) {
                out.push(None);
                continue;
            }
            let s = match TsQuery::parse_to_tsquery(input.value(i)) {
                Ok(Some(q)) => q.to_canonical(),
                _ => "T".to_string(),
            };
            out.push(Some(s));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ===========================================================================
// ts_headline — STUB (out of scope): returns the document body unchanged.
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TsHeadlineUdf {
    signature: Signature,
}

impl ScalarUDFImpl for TsHeadlineUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ts_headline"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        // 2-arg: (body, tsquery)            → body @0, query @1
        // 3/4-arg: (config, body, tsquery[, opts]) → body @1, query @2
        let (body_idx, query_idx) = if args.len() == 2 { (0, 1) } else { (1, 2) };
        let bodies = arg_strings(&args[body_idx], n)?;
        let queries = arg_strings(&args[query_idx], n)?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if bodies.is_null(i) {
                out.push(None);
                continue;
            }
            let body = bodies.value(i);
            let query = if queries.is_null(i) { "" } else { queries.value(i) };
            out.push(Some(headline_highlight(body, query)));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

/// Minimal `ts_headline` highlighting: wrap every body word whose lowercased
/// token form matches a positive (non-negated) query term in `<b>…</b>`.
///
/// This is *not* PG's cover-density fragment selection — it returns the full
/// body with matched terms bolded, which is the common, honest behavior most
/// callers expect (and far better than the previous no-op stub). Stemming is
/// not applied (consistent with Basin's no-stemming tsvector), so matching is
/// exact-token on the lowercased word.
fn headline_highlight(body: &str, query: &str) -> String {
    // Collect positive query terms (skip negated `!term` branches).
    let terms: std::collections::HashSet<String> = match TsQuery::parse_to_tsquery(query) {
        Ok(Some(q)) => {
            let mut acc = Vec::new();
            collect_positive_terms(&q, &mut acc);
            acc.into_iter().collect()
        }
        // Fall back to a plain tokenisation of the raw text if it is not a
        // valid tsquery (e.g. a plain phrase was passed in).
        _ => tokenize(query).into_iter().collect(),
    };

    if terms.is_empty() {
        return body.to_string();
    }

    // Walk the body splitting on word/non-word boundaries so punctuation and
    // whitespace are preserved verbatim and only the word runs are wrapped.
    let mut out = String::with_capacity(body.len() + 16);
    let mut word = String::new();
    let flush_word = |word: &mut String, out: &mut String| {
        if word.is_empty() {
            return;
        }
        if terms.contains(&word.to_lowercase()) {
            out.push_str("<b>");
            out.push_str(word);
            out.push_str("</b>");
        } else {
            out.push_str(word);
        }
        word.clear();
    };
    for ch in body.chars() {
        if ch.is_alphanumeric() {
            word.push(ch);
        } else {
            flush_word(&mut word, &mut out);
            out.push(ch);
        }
    }
    flush_word(&mut word, &mut out);
    out
}

/// Collect the lexemes of every non-negated `Term` in a tsquery tree.
fn collect_positive_terms(q: &TsQuery, acc: &mut Vec<String>) {
    match q {
        TsQuery::Term(t) => acc.push(t.to_lowercase()),
        // A `!q` branch is a negative term — do not highlight its lexemes.
        TsQuery::Not(_) => {}
        TsQuery::And(a, b) | TsQuery::Or(a, b) | TsQuery::Phrase(a, b, _) => {
            collect_positive_terms(a, acc);
            collect_positive_terms(b, acc);
        }
    }
}

// ===========================================================================
// Unit tests — tokenisation, canonical forms, the full @@ truth table,
// phrase adjacency (positive AND negative), tsvector_to_array, tsquery_phrase
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn tsv(text: &str) -> String {
        TsVector::from_text_document(text, Some("english")).to_canonical()
    }
    fn tsq(text: &str) -> String {
        TsQuery::parse_to_tsquery(text)
            .unwrap()
            .unwrap()
            .to_canonical()
    }
    /// `vec @@ query` where `vec` is a *document* run through to_tsvector.
    fn mtch(doc: &str, query: &str) -> bool {
        let v = TsVector::from_text_document(doc, Some("english"));
        let q = TsQuery::parse_to_tsquery(query).unwrap().unwrap();
        q.matches(&v)
    }
    /// `raw_vec @@ query` where `raw_vec` is a bare cast string.
    fn mtch_raw(raw: &str, query: &str) -> bool {
        let v = TsVector::parse(raw);
        let q = TsQuery::parse_to_tsquery(query).unwrap().unwrap();
        q.matches(&v)
    }

    #[test]
    fn tokenization_and_stopwords() {
        assert_eq!(
            tokenize("A Quick, Brown-Fox!"),
            vec!["a", "quick", "brown", "fox"]
        );
        assert!(is_stopword("the"));
        assert!(is_stopword("a"));
        assert!(!is_stopword("fox"));
        // Digit-hyphen-digit stays as one token.
        assert_eq!(tokenize("version 2024-01-15"), vec!["version", "2024-01-15"]);
        // Possessive stripping.
        assert_eq!(tokenize("John's"), vec!["john"]);
    }

    #[test]
    fn headline_wraps_matched_terms() {
        // Single term, preserves punctuation/whitespace verbatim.
        assert_eq!(
            headline_highlight("The quick brown fox.", "fox"),
            "The quick brown <b>fox</b>."
        );
    }

    #[test]
    fn headline_handles_and_query_and_case_insensitive() {
        assert_eq!(
            headline_highlight("Quick brown FOX jumps", "quick & fox"),
            "<b>Quick</b> brown <b>FOX</b> jumps"
        );
    }

    #[test]
    fn headline_skips_negated_terms() {
        // `!brown` is a negative term and must NOT be highlighted; `fox` is.
        assert_eq!(
            headline_highlight("quick brown fox", "fox & !brown"),
            "quick brown <b>fox</b>"
        );
    }

    #[test]
    fn headline_no_match_returns_body_unchanged() {
        assert_eq!(
            headline_highlight("the quick brown fox", "dog"),
            "the quick brown fox"
        );
        // Empty query → unchanged body (no terms to wrap).
        assert_eq!(headline_highlight("hello world", ""), "hello world");
    }

    #[test]
    fn to_tsvector_canonical_drops_stopwords_keeps_positions() {
        // 'a' is a stopword and dropped; positions advance over it so
        // quick=2, fox=3.  Lexemes sorted alphabetically: fox before quick.
        assert_eq!(tsv("a quick fox"), "'fox':3 'quick':2");
        // duplicate lexeme accumulates positions, sorted.
        // 'fox' is not stemmed; 'quick' is not stemmed by Snowball English.
        assert_eq!(tsv("fox fox quick"), "'fox':1,2 'quick':3");
        // simple config keeps stopwords and does NOT stem.
        assert_eq!(
            TsVector::from_text_document("a quick fox", Some("simple")).to_canonical(),
            "'a':1 'fox':3 'quick':2"
        );
        // Phase 5.20.C: Snowball English stemming is now applied.
        // 'running' stems to 'run'.
        assert_eq!(tsv("running"), "'run':1");
        assert_eq!(tsv("running runs"), "'run':1,2");
    }

    #[test]
    fn tsvector_to_array_sorted_distinct_no_positions() {
        let v = TsVector::from_text_document("a quick fox", Some("english"));
        assert_eq!(v.lexemes_sorted(), vec!["fox", "quick"]); // no 'a', sorted
        let v2 = TsVector::parse("'fox':2 'quick':1 'fox':4");
        assert_eq!(v2.lexemes_sorted(), vec!["fox", "quick"]);
    }

    #[test]
    fn to_tsquery_canonical() {
        assert_eq!(tsq("quick & fox"), "'quick' & 'fox'");
        assert_eq!(tsq("quick | fox"), "'quick' | 'fox'");
        assert_eq!(tsq("!dog"), "!'dog'");
        assert_eq!(tsq("quick <-> brown"), "'quick' <-> 'brown'");
        assert_eq!(tsq("quick <2> fox"), "'quick' <2> 'fox'");
        // precedence: & binds tighter than | ; ! tighter than & ;
        assert_eq!(tsq("a | b & c"), "'a' | 'b' & 'c'");
        assert_eq!(tsq("(a | b) & c"), "('a' | 'b') & 'c'");
        assert_eq!(tsq("!a & b"), "!'a' & 'b'");
    }

    #[test]
    fn at_at_boolean_truth_table() {
        // 'a fat cat'::tsvector @@ ...  (bare cast form, lexemes a,cat,fat)
        assert!(mtch_raw("a fat cat", "cat"));
        assert!(!mtch_raw("a fat cat", "dog"));
        assert!(!mtch_raw("a fat cat", "cat & dog"));
        assert!(mtch_raw("a fat cat", "cat | dog"));
        assert!(mtch_raw("a fat cat", "!dog"));
        assert!(!mtch_raw("a fat cat", "!cat"));
        assert!(mtch_raw("a fat cat", "cat & fat"));
        assert!(mtch_raw("a fat cat", "cat & !dog"));

        // document form (stopwords dropped from the vector)
        assert!(mtch("a quick fox", "quick"));
        assert!(!mtch("a quick fox", "slow"));
        assert!(mtch("a quick fox", "quick | slow"));
        assert!(mtch("a quick fox", "quick & fox"));
        assert!(!mtch("a quick fox", "quick & slow"));
    }

    #[test]
    fn phrase_adjacency_positive_and_negative() {
        // the quick brown fox -> quick:2 brown:3 fox:4 (the dropped @1)
        assert!(mtch("the quick brown fox", "quick <-> brown")); // adjacent
        assert!(!mtch("the quick brown fox", "quick <-> fox")); // 2 apart
        assert!(mtch("the quick brown fox", "quick <2> fox")); // exactly 2 apart
        assert!(mtch("the quick brown fox", "brown <-> fox")); // adjacent
        assert!(!mtch("the quick brown fox", "fox <-> brown")); // wrong order
                                                                // chained phrase composes via end-positions
        assert!(mtch("the quick brown fox", "quick <-> brown <-> fox"));
        assert!(!mtch("the quick brown fox", "quick <-> fox <-> brown"));
        // phrase against a position-less bare cast cannot be verified → false
        assert!(!mtch_raw("quick brown fox", "quick <-> brown"));
    }

    #[test]
    fn empty_and_nomatch_semantics() {
        // empty query string → no query → no match (no panic)
        assert!(TsQuery::parse_to_tsquery("").unwrap().is_none());
        assert!(TsQuery::parse_to_tsquery("   ").unwrap().is_none());
        // empty vector matches nothing
        let empty = TsVector::parse("");
        let q = TsQuery::parse_to_tsquery("cat").unwrap().unwrap();
        assert!(!q.matches(&empty));
    }

    #[test]
    fn tsquery_phrase_concatenation() {
        let a = TsQuery::parse_to_tsquery("quick").unwrap().unwrap();
        let b = TsQuery::parse_to_tsquery("fox").unwrap().unwrap();
        let p = TsQuery::Phrase(Box::new(a.clone()), Box::new(b.clone()), 1);
        assert_eq!(p.to_canonical(), "'quick' <-> 'fox'");
        let p2 = TsQuery::Phrase(Box::new(a), Box::new(b), 3);
        assert_eq!(p2.to_canonical(), "'quick' <3> 'fox'");
    }

    #[test]
    fn plainto_and_phraseto() {
        assert_eq!(
            TsQuery::plainto("the quick fox", Some("english"))
                .unwrap()
                .to_canonical(),
            "'quick' & 'fox'"
        );
        assert_eq!(
            TsQuery::phraseto("the quick fox", Some("english"))
                .unwrap()
                .to_canonical(),
            "'quick' <-> 'fox'"
        );
    }

    #[test]
    fn parse_canonical_roundtrip() {
        let v = TsVector::from_text_document("the quick brown fox jumps", Some("english"));
        let canon = v.to_canonical();
        let reparsed = TsVector::parse(&canon);
        assert_eq!(reparsed.to_canonical(), canon);
        // phrase still works after a canonical round-trip
        let q = TsQuery::parse_to_tsquery("quick <-> brown")
            .unwrap()
            .unwrap();
        assert!(q.matches(&reparsed));
    }

    #[test]
    fn strip_removes_positions() {
        let v = TsVector::parse("'fox':2 'quick':1");
        let mut s = v.clone();
        for (_, ps) in s.entries.iter_mut() {
            ps.clear();
        }
        assert_eq!(s.to_canonical(), "'fox' 'quick'");
    }

    #[test]
    fn unparseable_query_is_honest_no_match() {
        // Unbalanced paren → parse error → @@ returns false (never a fake hit)
        assert!(TsQuery::parse_to_tsquery("a & (b").is_err());
    }

    #[test]
    fn to_tsquery_stems_terms_like_to_tsvector() {
        // Phase 5.20.E stemming-consistency contract: to_tsquery lexemes go
        // through the SAME Snowball pipeline as to_tsvector, so a document
        // indexed as 'run' (from "running") matches to_tsquery('runs').
        assert_eq!(
            to_tsquery_text("to_tsquery", None, "runs").unwrap(),
            "'run'"
        );
        assert_eq!(
            to_tsquery_text("to_tsquery", Some("english"), "running & dogs").unwrap(),
            "'run' & 'dog'"
        );
        // Boolean structure is preserved through stemming.
        assert_eq!(
            to_tsquery_text("to_tsquery", None, "running | !jumping").unwrap(),
            "'run' | !'jump'"
        );
        assert_eq!(
            to_tsquery_text("to_tsquery", None, "running <-> dogs").unwrap(),
            "'run' <-> 'dog'"
        );
        // 'simple' config: no stemming.
        assert_eq!(
            to_tsquery_text("to_tsquery", Some("simple"), "running").unwrap(),
            "'running'"
        );
        // Raw ::tsquery casts are NOT stemmed (PG parity).
        assert_eq!(canonicalize_tsquery_text("running").unwrap(), "'running'");
    }

    #[test]
    fn stemmed_query_matches_stemmed_document() {
        // End-to-end stem consistency at the @@ evaluator level: the document
        // "running fast" stems to 'run' + 'fast'; to_tsquery('runs') stems to
        // 'run' and must match.
        let v = TsVector::from_text_document("running fast", Some("english"));
        let canon = to_tsquery_text("to_tsquery", Some("english"), "runs").unwrap();
        let q = TsQuery::parse_to_tsquery(&canon).unwrap().unwrap();
        assert!(q.matches(&v), "to_tsquery('runs') must match doc 'running'");
        // And the unstemmed cast form does NOT match — same divergence as PG.
        let raw = TsQuery::parse_to_tsquery("runs").unwrap().unwrap();
        assert!(!raw.matches(&v), "'runs'::tsquery (unstemmed) must not match");
    }
}
