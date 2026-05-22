//! Server-side MIME-type sniffing from magic bytes (P2-4).
//!
//! # Security rationale
//!
//! Trusting a client-supplied `Content-Type` header is a content-type
//! confusion vector: an attacker can upload an HTML file as `image/png` and
//! a browser that later serves it will execute the HTML. To close this audit
//! finding (P2-4) the server sniffs the actual magic bytes of every uploaded
//! payload and uses the sniffed type instead of the client header.
//!
//! # Implementation strategy
//!
//! Pure Rust magic-byte matching with no extra crate dependencies.  The set
//! of magic signatures covers the common web-hostile types (HTML, SVG, JS,
//! XML, PDF) as well as the popular image/video/audio/archive formats that
//! buckets are commonly used for.  Unknown content falls back to
//! `application/octet-stream` — a safe, non-executable MIME type that
//! browsers will not render.
//!
//! # Algorithm
//!
//! 1. Strip a BOM if present (UTF-8/UTF-16/UTF-32).
//! 2. Try each magic signature in priority order (longest / most-specific
//!    first within each group).
//! 3. For text that passes the ASCII-printable + whitespace check, attempt
//!    a heuristic tag-sniff for HTML, SVG, XML, and JavaScript.
//! 4. Return `application/octet-stream` for anything else.

use bytes::Bytes;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Sniff the MIME type of `data` from its content (magic bytes).
///
/// Always returns a valid, non-empty MIME-type string.  Falls back to
/// `"application/octet-stream"` for unknown or ambiguous content.
///
/// The returned string should be stored as the canonical `mime_type` for the
/// object, overriding whatever the client supplied.
pub fn sniff(data: &Bytes) -> &'static str {
    sniff_bytes(data.as_ref())
}

// ---------------------------------------------------------------------------
// Magic-byte table
// ---------------------------------------------------------------------------

/// A magic signature entry: `(offset, magic, mime_type)`.
///
/// `offset` is the byte offset at which `magic` must appear.
struct Magic {
    offset: usize,
    magic: &'static [u8],
    mime: &'static str,
}

impl Magic {
    const fn new(offset: usize, magic: &'static [u8], mime: &'static str) -> Self {
        Self { offset, magic, mime }
    }
}

/// Magic signatures checked in order.  Longer / more-specific prefixes come
/// first within each format family so they shadow shorter overlapping ones.
static MAGIC: &[Magic] = &[
    // ── Images ─────────────────────────────────────────────────────────────
    Magic::new(0, b"\x89PNG\r\n\x1a\n", "image/png"),
    Magic::new(0, b"\xff\xd8\xff", "image/jpeg"),
    Magic::new(0, b"GIF87a", "image/gif"),
    Magic::new(0, b"GIF89a", "image/gif"),
    Magic::new(0, b"RIFF", "image/webp"),   // checked further below
    Magic::new(0, b"BM", "image/bmp"),
    Magic::new(0, b"\x00\x00\x01\x00", "image/x-icon"),
    Magic::new(0, b"\x00\x00\x02\x00", "image/x-icon"),
    Magic::new(4, b"ftyp", "video/mp4"),    // ISO base media; refined below
    Magic::new(0, b"\x1f\x8b", "application/gzip"),
    Magic::new(0, b"PK\x03\x04", "application/zip"),
    Magic::new(0, b"PK\x05\x06", "application/zip"),
    Magic::new(0, b"PK\x07\x08", "application/zip"),
    Magic::new(0, b"\x42\x5a\x68", "application/x-bzip2"),
    Magic::new(0, b"\xfd7zXZ\x00", "application/x-xz"),
    Magic::new(0, b"7z\xbc\xaf\x27\x1c", "application/x-7z-compressed"),
    Magic::new(0, b"Rar!\x1a\x07\x00", "application/x-rar-compressed"),
    Magic::new(0, b"Rar!\x1a\x07\x01", "application/x-rar-compressed"),
    // ── PDF ────────────────────────────────────────────────────────────────
    Magic::new(0, b"%PDF-", "application/pdf"),
    // ── Audio / Video ───────────────────────────────────────────────────────
    Magic::new(0, b"fLaC", "audio/flac"),
    Magic::new(0, b"OggS", "audio/ogg"),
    Magic::new(0, b"ID3", "audio/mpeg"),
    Magic::new(0, b"\xff\xfb", "audio/mpeg"),
    Magic::new(0, b"\xff\xf3", "audio/mpeg"),
    Magic::new(0, b"\xff\xf2", "audio/mpeg"),
    // ── Fonts ───────────────────────────────────────────────────────────────
    Magic::new(0, b"wOFF", "font/woff"),
    Magic::new(0, b"wOF2", "font/woff2"),
    // ── Executable / wasm ───────────────────────────────────────────────────
    Magic::new(0, b"\x00asm", "application/wasm"),
    Magic::new(0, b"\x7fELF", "application/octet-stream"),
    Magic::new(0, b"MZ", "application/octet-stream"),
    // ── TIFF ────────────────────────────────────────────────────────────────
    Magic::new(0, b"II*\x00", "image/tiff"),
    Magic::new(0, b"MM\x00*", "image/tiff"),
    // ── Avif / HEIC (need ftyp brand check) ────────────────────────────────
    // handled in refinement step below
];

// ---------------------------------------------------------------------------
// Core sniff implementation
// ---------------------------------------------------------------------------

fn sniff_bytes(data: &[u8]) -> &'static str {
    if data.is_empty() {
        return "application/octet-stream";
    }

    // Step 1: strip BOM so text-sniffing works on BOM-prefixed UTF-8 files.
    let text_data = strip_bom(data);

    // Step 2: try magic table.
    for m in MAGIC {
        if data.len() < m.offset + m.magic.len() {
            continue;
        }
        if &data[m.offset..m.offset + m.magic.len()] == m.magic {
            // Special-case refinement for RIFF containers.
            if m.magic == b"RIFF" {
                return sniff_riff(data);
            }
            // Special-case refinement for ISO base media containers (ftyp).
            if m.magic == b"ftyp" {
                return sniff_ftyp(data);
            }
            return m.mime;
        }
    }

    // Step 3: text heuristics for markup / script types.
    if looks_like_text(text_data) {
        return sniff_text(text_data);
    }

    // Step 4: unknown binary.
    "application/octet-stream"
}

// ---------------------------------------------------------------------------
// Helper: BOM stripping
// ---------------------------------------------------------------------------

fn strip_bom(data: &[u8]) -> &[u8] {
    if data.starts_with(b"\xef\xbb\xbf") {
        return &data[3..]; // UTF-8 BOM
    }
    if data.starts_with(b"\xff\xfe\x00\x00") || data.starts_with(b"\x00\x00\xfe\xff") {
        return &data[4..]; // UTF-32 BOM
    }
    if data.starts_with(b"\xff\xfe") || data.starts_with(b"\xfe\xff") {
        return &data[2..]; // UTF-16 BOM
    }
    data
}

// ---------------------------------------------------------------------------
// Helper: RIFF container disambiguation
// ---------------------------------------------------------------------------

fn sniff_riff(data: &[u8]) -> &'static str {
    // RIFF layout: "RIFF" <4-byte size LE> <4-byte brand>
    if data.len() >= 12 {
        let brand = &data[8..12];
        if brand == b"WEBP" {
            return "image/webp";
        }
        if brand == b"AVI " {
            return "video/x-msvideo";
        }
        if brand == b"WAVE" {
            return "audio/wav";
        }
    }
    "application/octet-stream"
}

// ---------------------------------------------------------------------------
// Helper: ISO base media (ftyp) brand disambiguation
// ---------------------------------------------------------------------------

fn sniff_ftyp(data: &[u8]) -> &'static str {
    // ftyp box layout: <4-byte size> "ftyp" <4-byte major-brand> <4-byte version>
    // major-brand is at offset 8 (size field is 4 bytes before ftyp).
    // Our Magic match hits at offset 4 (the "ftyp" literal), so the box
    // actually starts 4 bytes earlier; major brand is at offset 8.
    if data.len() >= 12 {
        let brand = &data[8..12];
        // AVIF brands.
        if brand == b"avif" || brand == b"avis" {
            return "image/avif";
        }
        // HEIC/HEIF brands.
        if brand == b"heic" || brand == b"heif" || brand == b"heix" || brand == b"mif1" {
            return "image/heic";
        }
        // QuickTime.
        if brand == b"qt  " {
            return "video/quicktime";
        }
    }
    "video/mp4"
}

// ---------------------------------------------------------------------------
// Helper: is this data plausibly text?
// ---------------------------------------------------------------------------

/// Returns `true` if the first 512 bytes are all ASCII printable or common
/// whitespace (tab, LF, CR).  This is a conservative check: high bytes →
/// binary, low-control bytes → binary.
fn looks_like_text(data: &[u8]) -> bool {
    let probe = &data[..data.len().min(512)];
    for &b in probe {
        match b {
            0x09 | 0x0a | 0x0d => {} // tab, LF, CR — OK
            0x20..=0x7e => {}         // printable ASCII — OK
            _ => return false,
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Helper: text-content sniffing
// ---------------------------------------------------------------------------

/// Sniff a text payload for HTML, SVG, XML, JSON, and JavaScript.
///
/// Uses byte-level scanning rather than a full parser: we look for the first
/// non-whitespace run of bytes and check if it looks like a tag or a known
/// text-format marker.
fn sniff_text(data: &[u8]) -> &'static str {
    // Skip leading whitespace to find the first content byte.
    let trimmed = trim_ascii_whitespace(data);

    // HTML tag heuristic: starts with `<!` or `<html` or `<head` or `<body`
    // (case-insensitive for the first ~16 bytes).
    if trimmed.starts_with(b"<!") {
        // `<!DOCTYPE html` or `<!-- comment -->` → HTML
        let lower16 = to_ascii_lower_prefix(trimmed, 16);
        if lower16.starts_with(b"<!doctype html") || lower16.starts_with(b"<!--") {
            return "text/html";
        }
    }
    if starts_with_ci(trimmed, b"<html") {
        return "text/html";
    }
    if starts_with_ci(trimmed, b"<head") {
        return "text/html";
    }
    if starts_with_ci(trimmed, b"<body") {
        return "text/html";
    }

    // SVG: XML namespace declaration or `<svg`
    if starts_with_ci(trimmed, b"<svg") {
        return "image/svg+xml";
    }
    // XML processing instruction or a namespace-qualified element.
    if trimmed.starts_with(b"<?xml") || trimmed.starts_with(b"<?XML") {
        // Peek further to see if it's SVG.
        if memmem(trimmed, b"<svg") || memmem(trimmed, b"<SVG") {
            return "image/svg+xml";
        }
        return "application/xml";
    }
    if trimmed.starts_with(b"<") {
        // Generic XML / HTML — check for `<svg` anywhere in the first 1 KiB.
        let head = &trimmed[..trimmed.len().min(1024)];
        if memmem_ci(head, b"<svg") {
            return "image/svg+xml";
        }
        // Any other `<tag` → treat as HTML to be conservative.
        return "text/html";
    }

    // JSON: starts with `{` or `[`
    if trimmed.first() == Some(&b'{') || trimmed.first() == Some(&b'[') {
        return "application/json";
    }

    // JavaScript heuristics: common module/script keywords at line start.
    {
        let head = &trimmed[..trimmed.len().min(256)];
        if starts_with_ci(head, b"import ")
            || starts_with_ci(head, b"export ")
            || starts_with_ci(head, b"function ")
            || starts_with_ci(head, b"const ")
            || starts_with_ci(head, b"var ")
            || starts_with_ci(head, b"let ")
            || starts_with_ci(head, b"class ")
            || starts_with_ci(head, b"(function")
            || starts_with_ci(head, b"!function")
        {
            return "text/javascript";
        }
    }

    // CSS heuristic: starts with a selector or `@` rule.
    if trimmed.first() == Some(&b'@') || trimmed.starts_with(b"/*") || trimmed.starts_with(b"//") {
        // Very rough: `@charset`, `@import`, or `/*` comments.
        let head = &trimmed[..trimmed.len().min(64)];
        if starts_with_ci(head, b"@charset")
            || starts_with_ci(head, b"@import")
            || starts_with_ci(head, b"@media")
        {
            return "text/css";
        }
    }

    // Default text type.
    "text/plain"
}

// ---------------------------------------------------------------------------
// Tiny text utilities (no alloc)
// ---------------------------------------------------------------------------

fn trim_ascii_whitespace(data: &[u8]) -> &[u8] {
    let start = data
        .iter()
        .position(|&b| !matches!(b, b'\t' | b'\n' | b'\r' | b' '))
        .unwrap_or(data.len());
    &data[start..]
}

/// Return `true` if `haystack` starts with `needle` (ASCII case-insensitive).
fn starts_with_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if haystack.len() < needle.len() {
        return false;
    }
    haystack[..needle.len()]
        .iter()
        .zip(needle.iter())
        .all(|(&h, &n)| h.to_ascii_lowercase() == n.to_ascii_lowercase())
}

/// Simple substring search (no alloc, O(n*m)).
fn memmem(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Case-insensitive substring search.
fn memmem_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    let needle_lower: Vec<u8> = needle.iter().map(|b| b.to_ascii_lowercase()).collect();
    haystack.windows(needle.len()).any(|w| {
        w.iter()
            .zip(needle_lower.iter())
            .all(|(&h, &n)| h.to_ascii_lowercase() == n)
    })
}

/// Lowercase the first `n` bytes of `data` (returning a `Vec`).
fn to_ascii_lower_prefix(data: &[u8], n: usize) -> Vec<u8> {
    data[..data.len().min(n)]
        .iter()
        .map(|b| b.to_ascii_lowercase())
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn s(data: &[u8]) -> &'static str {
        sniff_bytes(data)
    }

    // ── Binary formats ────────────────────────────────────────────────────

    #[test]
    fn detects_png() {
        assert_eq!(s(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"), "image/png");
    }

    #[test]
    fn detects_jpeg() {
        assert_eq!(s(b"\xff\xd8\xff\xe0\x00\x10JFIF"), "image/jpeg");
    }

    #[test]
    fn detects_gif() {
        assert_eq!(s(b"GIF89a\x01\x00\x01\x00"), "image/gif");
        assert_eq!(s(b"GIF87a\x01\x00\x01\x00"), "image/gif");
    }

    #[test]
    fn detects_webp() {
        let mut data = b"RIFF\x00\x00\x00\x00WEBP".to_vec();
        data.extend_from_slice(b"VP8 ");
        assert_eq!(s(&data), "image/webp");
    }

    #[test]
    fn detects_pdf() {
        assert_eq!(s(b"%PDF-1.5\n%\xe2\xe3\xcf\xd3"), "application/pdf");
    }

    #[test]
    fn detects_zip() {
        assert_eq!(s(b"PK\x03\x04\x14\x00"), "application/zip");
    }

    #[test]
    fn detects_gzip() {
        assert_eq!(s(b"\x1f\x8b\x08\x00"), "application/gzip");
    }

    // ── Text / markup ─────────────────────────────────────────────────────

    #[test]
    fn detects_html_doctype() {
        assert_eq!(
            s(b"<!DOCTYPE html>\n<html>"),
            "text/html"
        );
    }

    #[test]
    fn detects_html_doctype_uppercase() {
        assert_eq!(
            s(b"<!DOCTYPE HTML>\n<HTML>"),
            "text/html"
        );
    }

    #[test]
    fn detects_html_tag() {
        assert_eq!(s(b"<html lang=\"en\">"), "text/html");
    }

    #[test]
    fn detects_html_via_head_tag() {
        assert_eq!(s(b"<head><title>T</title></head>"), "text/html");
    }

    #[test]
    fn detects_svg() {
        assert_eq!(
            s(b"<?xml version=\"1.0\"?><svg xmlns=\"http://www.w3.org/2000/svg\">"),
            "image/svg+xml"
        );
    }

    #[test]
    fn detects_svg_direct_tag() {
        assert_eq!(
            s(b"<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 100 100\">"),
            "image/svg+xml"
        );
    }

    #[test]
    fn detects_xml() {
        assert_eq!(s(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><root>"), "application/xml");
    }

    #[test]
    fn detects_json_object() {
        assert_eq!(s(b"{\"key\": \"value\"}"), "application/json");
    }

    #[test]
    fn detects_json_array() {
        assert_eq!(s(b"[1, 2, 3]"), "application/json");
    }

    #[test]
    fn detects_javascript() {
        assert_eq!(s(b"function foo() { return 1; }"), "text/javascript");
    }

    #[test]
    fn detects_javascript_const() {
        assert_eq!(s(b"const x = 42;"), "text/javascript");
    }

    #[test]
    fn detects_javascript_import() {
        assert_eq!(s(b"import React from 'react';"), "text/javascript");
    }

    #[test]
    fn plain_text_falls_through() {
        assert_eq!(s(b"Hello, world!\n"), "text/plain");
    }

    #[test]
    fn empty_is_octet_stream() {
        assert_eq!(s(b""), "application/octet-stream");
    }

    #[test]
    fn binary_noise_is_octet_stream() {
        assert_eq!(s(b"\x00\x01\x02\x03\xfe\xff"), "application/octet-stream");
    }

    // ── Security: PNG bytes uploaded as text/html must sniff as image/png ─

    #[test]
    fn png_uploaded_as_html_sniffs_as_png() {
        let png = Bytes::from_static(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR extra");
        assert_eq!(sniff(&png), "image/png");
    }

    // ── BOM handling ──────────────────────────────────────────────────────

    #[test]
    fn utf8_bom_html_sniffs_as_html() {
        let data = b"\xef\xbb\xbf<!DOCTYPE html><html>";
        assert_eq!(s(data), "text/html");
    }
}
