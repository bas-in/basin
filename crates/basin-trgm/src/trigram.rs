//! Trigram extraction — the kernel of every `pg_trgm` operation.
//!
//! Postgres's `pg_trgm` (see `contrib/pg_trgm/trgm.h`) treats a string as a
//! sequence of whitespace-delimited "words", each padded with two leading
//! spaces and one trailing space, then slides a 3-byte window over every
//! word and dedupes the resulting set:
//!
//! ```text
//!   "cat"   ─► padded "  cat "
//!           ─► windows "  c", " ca", "cat", "at "
//! ```
//!
//! ## Normalisation
//!
//! We match Postgres's preprocessing exactly:
//!
//! 1. **Case-fold to ASCII lowercase**. `pg_trgm` uses `pg_mblower` which
//!    is locale-sensitive on the server; for the v0.1 ASCII-only subset,
//!    `char::to_ascii_lowercase` is byte-identical for the inputs that
//!    matter (English-letter typo matching).
//! 2. **Word splitting**. Any character that isn't an alphanumeric is a
//!    word separator — same rule `pg_trgm` uses (see `iswordchr` in
//!    `trgm.h`). This means `"alice-smith"` and `"alice smith"` produce
//!    identical trigram sets, which matches Postgres.
//! 3. **Padding**. Two leading spaces, one trailing space per word. This
//!    is what makes `similarity("cat", "scat")` non-trivial — without
//!    padding, "cat" ⊆ "scat" and the score would jump to 1.0; with
//!    padding the leading-space trigrams differ and you get a more
//!    discriminating ratio.
//!
//! ## Output shape
//!
//! [`extract`] returns a sorted, deduped `Vec<[u8; 3]>`. We use the byte
//! array (not `String`) because trigrams are always exactly 3 bytes after
//! ASCII case-folding and the surrounding code never needs to UTF-8-decode
//! them again. The `Vec` is sorted so set-intersection runs in O(n+m) via
//! a two-pointer merge instead of allocating a `HashSet`. Empirically the
//! sort dominates for short strings (n ≲ 20) but is still ~2× faster than
//! the hash-set path because of the lack of allocator traffic.
//!
//! ## Memory
//!
//! Per-input cost: an N-byte ASCII string yields at most N+1 trigrams per
//! word (most strings yield ≈ N trigrams since words are short). For a
//! 100-char string with average word length ~5, expect ≈ 100 trigrams ×
//! 3 bytes = ~300 B for the trigram bytes plus the `Vec` overhead.

/// A trigram. Exactly 3 ASCII bytes.
pub type Trigram = [u8; 3];

/// Extract the trigram set of `s`, sorted and deduplicated.
///
/// Returns an empty vector when `s` has no alphanumeric characters
/// (`pg_trgm` returns `{}` in the same case).
pub fn extract(s: &str) -> Vec<Trigram> {
    let mut out = Vec::with_capacity(s.len() + 4);
    push_word_trigrams(s, &mut out);
    out.sort_unstable();
    out.dedup();
    out
}

/// Push the trigrams of every word in `s` onto `out`. Caller must
/// sort+dedup afterwards. Split out so [`crate::similarity::word_similarity`]
/// can iterate per-word trigram sets without re-running the splitter.
pub(crate) fn push_word_trigrams(s: &str, out: &mut Vec<Trigram>) {
    // Walk bytes (we case-fold to ASCII anyway) and accumulate runs of
    // alphanumeric characters into "words". For each word, emit
    // "  <word> " trigrams.
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Skip non-word chars.
        while i < bytes.len() && !is_word_byte(bytes[i]) {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        // Accumulate one word.
        let start = i;
        while i < bytes.len() && is_word_byte(bytes[i]) {
            i += 1;
        }
        emit_word_trigrams(&bytes[start..i], out);
    }
}

/// Emit "  <word> "-padded 3-byte windows.
fn emit_word_trigrams(word: &[u8], out: &mut Vec<Trigram>) {
    if word.is_empty() {
        return;
    }
    // Build the padded buffer on the stack for the common case (word
    // length ≤ 60). Falls through to a heap allocation only for very
    // long words. The common pg_trgm input is human names / short
    // identifiers where this cap is comfortable.
    const STACK_CAP: usize = 64;
    let padded_len = word.len() + 3; // 2 leading + 1 trailing
    if padded_len <= STACK_CAP {
        let mut buf = [0u8; STACK_CAP];
        buf[0] = b' ';
        buf[1] = b' ';
        for (k, b) in word.iter().enumerate() {
            buf[2 + k] = b.to_ascii_lowercase();
        }
        buf[2 + word.len()] = b' ';
        for w in buf[..padded_len].windows(3) {
            out.push([w[0], w[1], w[2]]);
        }
    } else {
        let mut buf = Vec::with_capacity(padded_len);
        buf.push(b' ');
        buf.push(b' ');
        for b in word {
            buf.push(b.to_ascii_lowercase());
        }
        buf.push(b' ');
        for w in buf.windows(3) {
            out.push([w[0], w[1], w[2]]);
        }
    }
}

/// `pg_trgm` treats anything that isn't `[A-Za-z0-9]` as a word
/// separator. Match that exactly. Non-ASCII multibyte sequences fall
/// through as separators in v0.1 — we'll widen this when locale-aware
/// case-folding lands.
#[inline]
fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric()
}

/// Size of one trigram set for cost reporting. Always
/// `set.len() * std::mem::size_of::<Trigram>()` plus the `Vec` header.
#[inline]
pub fn set_bytes(set: &[Trigram]) -> usize {
    std::mem::size_of_val(set)
}

/// Cardinality of the intersection of two **sorted** trigram sets.
/// O(n + m). Both inputs must be the output of [`extract`] (or the
/// per-word slice from [`push_word_trigrams`] followed by sort+dedup);
/// passing un-sorted input silently yields wrong counts.
pub(crate) fn intersect_count(a: &[Trigram], b: &[Trigram]) -> usize {
    let (mut i, mut j) = (0usize, 0usize);
    let mut count = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => {
                count += 1;
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }
    count
}
