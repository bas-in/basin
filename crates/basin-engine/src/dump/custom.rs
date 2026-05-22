//! Phase 5.22.C — Basin custom binary dump format.
//!
//! ## Format overview
//!
//! This format is Basin's own binary archive — it is **not** byte-compatible
//! with `pg_dump --format=custom`. Use plain format for `pg_restore` interop.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ HEADER (fixed 64 bytes)                                     │
//! │   magic:   8 bytes   "BASINDMP"                             │
//! │   version: u8        format version (currently 1)           │
//! │   flags:   u8        0x01=has-schema, 0x02=has-data         │
//! │   reserved: 6 bytes  (zeros)                                │
//! │   basin_version: 16 bytes  semver, NUL-padded UTF-8         │
//! │   pg_compat_version: 16 bytes  "16.0", NUL-padded           │
//! │   dump_timestamp_unix: i64 BE  UTC unix seconds             │
//! │   table_count: u32 BE                                        │
//! │   toc_offset: u64 BE  byte offset of the TOC from file start│
//! ├─────────────────────────────────────────────────────────────┤
//! │ TABLE-OF-CONTENTS (TOC)                                     │
//! │   For each table:                                           │
//! │     name_len: u16 BE                                        │
//! │     name: UTF-8 bytes                                       │
//! │     ddl_offset: u64 BE  byte offset of the DDL block        │
//! │     ddl_len: u64 BE                                         │
//! │     data_offset: u64 BE byte offset of the data block       │
//! │     data_len: u64 BE                                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │ DATA BLOCKS (one per table, in dependency order)            │
//! │   DDL block: UTF-8 CREATE TABLE SQL                         │
//! │   Data block: newline-delimited INSERT SQL statements        │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! The table-of-contents allows selective restore: a future `basin-cli restore
//! --table=foo` only needs to parse the TOC and seek to the DDL/data offsets
//! for `foo`.

use std::io::{self, Write};

use arrow_array::RecordBatch;
use basin_catalog::TableMetadata;
use basin_common::{ProjectId, Result};
use chrono::Utc;

use crate::dump::{field_to_pg_type, quote_ident, resolve_tables, DumpOptions};

// ─── Format constants ─────────────────────────────────────────────────────────

const MAGIC: &[u8; 8] = b"BASINDMP";
const FORMAT_VERSION: u8 = 1;
const FLAG_HAS_SCHEMA: u8 = 0x01;
const FLAG_HAS_DATA: u8 = 0x02;
/// Header size in bytes.
/// Layout: magic(8) + version(1) + flags(1) + reserved(6) + basin_ver(16)
///       + pg_compat_ver(16) + timestamp(8) + table_count(4) + toc_offset(8) = 68
const HEADER_SIZE: usize = 68;

// ─── Dump ─────────────────────────────────────────────────────────────────────

/// Produce a custom binary dump for `project`.
pub(crate) async fn dump(
    engine: &crate::Engine,
    project: ProjectId,
    opts: &DumpOptions,
) -> Result<Vec<u8>> {
    let catalog = engine.config().catalog.clone();
    let tables = resolve_tables(catalog.as_ref(), &project, opts).await?;

    // ── Per-table: generate DDL + data SQL blocks ──────────────────────────
    let mut table_blocks: Vec<TableBlock> = Vec::with_capacity(tables.len());

    for meta in &tables {
        // DDL block: CREATE TABLE SQL.
        let ddl_sql = if opts.schema {
            let mut ddl = String::new();
            emit_create_table_sql(&mut ddl, meta);
            ddl.into_bytes()
        } else {
            Vec::new()
        };

        // Data block: newline-delimited INSERT statements.
        let data_bytes = if opts.data {
            let sess = engine.open_session(project).await?;
            let tname = quote_ident(meta.table.as_str());
            let sql = format!("SELECT * FROM {tname}");
            let result = sess.execute(&sql).await?;

            if let crate::ExecResult::Rows { schema, batches } = result {
                let mut data_str = String::new();
                plain_inserts(&mut data_str, meta.table.as_str(), &schema, &batches);
                data_str.into_bytes()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        table_blocks.push(TableBlock {
            name: meta.table.as_str().to_string(),
            ddl: ddl_sql,
            data: data_bytes,
        });
    }

    // ── Lay out the archive ───────────────────────────────────────────────
    // Layout:
    //   [HEADER 64 bytes]
    //   [DDL block for table 0]
    //   [Data block for table 0]
    //   ...
    //   [TOC]
    //
    // We write data blocks first, then the TOC (so we know offsets), then
    // go back and patch the header's toc_offset.

    let mut buf: Vec<u8> = Vec::with_capacity(8192);

    // Write placeholder header (HEADER_SIZE bytes of zeros; patched below).
    buf.extend_from_slice(&[0u8; 68]); // HEADER_SIZE = 68

    // Write data blocks and record offsets.
    let mut toc_entries: Vec<TocEntry> = Vec::with_capacity(table_blocks.len());

    for block in &table_blocks {
        let ddl_offset = buf.len() as u64;
        buf.extend_from_slice(&block.ddl);

        let data_offset = buf.len() as u64;
        buf.extend_from_slice(&block.data);

        toc_entries.push(TocEntry {
            name: block.name.clone(),
            ddl_offset,
            ddl_len: block.ddl.len() as u64,
            data_offset,
            data_len: block.data.len() as u64,
        });
    }

    // Write TOC.
    let toc_offset = buf.len() as u64;
    for entry in &toc_entries {
        let name_bytes = entry.name.as_bytes();
        let name_len = name_bytes.len() as u16;
        buf.extend_from_slice(&name_len.to_be_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&entry.ddl_offset.to_be_bytes());
        buf.extend_from_slice(&entry.ddl_len.to_be_bytes());
        buf.extend_from_slice(&entry.data_offset.to_be_bytes());
        buf.extend_from_slice(&entry.data_len.to_be_bytes());
    }

    // ── Patch the header ─────────────────────────────────────────────────
    let version_str = env!("CARGO_PKG_VERSION");
    let pg_compat = "16.0";
    let ts_unix = Utc::now().timestamp();

    let mut flags = 0u8;
    if opts.schema { flags |= FLAG_HAS_SCHEMA; }
    if opts.data { flags |= FLAG_HAS_DATA; }

    let mut header = [0u8; HEADER_SIZE];
    let mut w = io::Cursor::new(&mut header[..]);

    w.write_all(MAGIC).unwrap(); // 8 bytes: magic
    w.write_all(&[FORMAT_VERSION]).unwrap(); // 1 byte: version
    w.write_all(&[flags]).unwrap(); // 1 byte: flags
    w.write_all(&[0u8; 6]).unwrap(); // 6 bytes: reserved

    // basin_version: 16 bytes, NUL-padded UTF-8
    let mut bv = [0u8; 16];
    let src = version_str.as_bytes();
    let n = src.len().min(15);
    bv[..n].copy_from_slice(&src[..n]);
    w.write_all(&bv).unwrap();

    // pg_compat_version: 16 bytes, NUL-padded
    let mut pv = [0u8; 16];
    let src = pg_compat.as_bytes();
    let n = src.len().min(15);
    pv[..n].copy_from_slice(&src[..n]);
    w.write_all(&pv).unwrap();

    // dump_timestamp_unix: i64 BE
    w.write_all(&ts_unix.to_be_bytes()).unwrap();

    // table_count: u32 BE
    let table_count = table_blocks.len() as u32;
    w.write_all(&table_count.to_be_bytes()).unwrap();

    // toc_offset: u64 BE
    w.write_all(&toc_offset.to_be_bytes()).unwrap();

    // Patch header into buf.
    buf[..HEADER_SIZE].copy_from_slice(&header);

    Ok(buf)
}

struct TableBlock {
    name: String,
    ddl: Vec<u8>,
    data: Vec<u8>,
}

struct TocEntry {
    name: String,
    ddl_offset: u64,
    ddl_len: u64,
    data_offset: u64,
    data_len: u64,
}

// ─── Restore ──────────────────────────────────────────────────────────────────

/// Restore from a custom binary archive produced by [`dump`].
pub(crate) async fn restore(
    engine: &crate::Engine,
    project: ProjectId,
    archive: &[u8],
) -> Result<()> {
    // Parse header.
    if archive.len() < HEADER_SIZE {
        return Err(basin_common::BasinError::InvalidSchema(
            "custom archive too short — not a valid Basin dump".into(),
        ));
    }

    if &archive[..8] != MAGIC {
        return Err(basin_common::BasinError::InvalidSchema(
            "custom archive has wrong magic — not a Basin dump".into(),
        ));
    }

    let _fmt_version = archive[8];
    let flags = archive[9];
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data = (flags & FLAG_HAS_DATA) != 0;

    // Header layout (HEADER_SIZE = 68 bytes):
    //   0..8    magic
    //   8       format_version
    //   9       flags
    //   10..16  reserved (6 bytes)
    //   16..32  basin_version (16 bytes)
    //   32..48  pg_compat_version (16 bytes)
    //   48..56  dump_timestamp_unix (i64 BE)
    //   56..60  table_count (u32 BE)
    //   60..68  toc_offset (u64 BE)
    let table_count = u32::from_be_bytes(archive[56..60].try_into().unwrap()) as usize;
    let toc_offset = u64::from_be_bytes(archive[60..68].try_into().unwrap()) as usize;

    if toc_offset > archive.len() {
        return Err(basin_common::BasinError::InvalidSchema(
            "custom archive TOC offset out of bounds".into(),
        ));
    }

    // Parse TOC.
    let mut pos = toc_offset;
    let mut entries: Vec<TocEntry> = Vec::with_capacity(table_count);
    for _ in 0..table_count {
        if pos + 2 > archive.len() {
            return Err(basin_common::BasinError::InvalidSchema(
                "custom archive TOC truncated at name_len".into(),
            ));
        }
        let name_len = u16::from_be_bytes(archive[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        if pos + name_len > archive.len() {
            return Err(basin_common::BasinError::InvalidSchema(
                "custom archive TOC truncated at name".into(),
            ));
        }
        let name = std::str::from_utf8(&archive[pos..pos + name_len])
            .map_err(|_| {
                basin_common::BasinError::InvalidSchema(
                    "custom archive TOC table name is not valid UTF-8".into(),
                )
            })?
            .to_string();
        pos += name_len;

        if pos + 32 > archive.len() {
            return Err(basin_common::BasinError::InvalidSchema(
                "custom archive TOC entry truncated".into(),
            ));
        }
        let ddl_offset = u64::from_be_bytes(archive[pos..pos + 8].try_into().unwrap()) as usize;
        let ddl_len = u64::from_be_bytes(archive[pos + 8..pos + 16].try_into().unwrap()) as usize;
        let data_offset =
            u64::from_be_bytes(archive[pos + 16..pos + 24].try_into().unwrap()) as usize;
        let data_len =
            u64::from_be_bytes(archive[pos + 24..pos + 32].try_into().unwrap()) as usize;
        pos += 32;

        entries.push(TocEntry {
            name,
            ddl_offset: ddl_offset as u64,
            ddl_len: ddl_len as u64,
            data_offset: data_offset as u64,
            data_len: data_len as u64,
        });
    }

    let sess = engine.open_session(project).await?;

    // Replay DDL first, then data (in TOC order = dependency order).
    if has_schema {
        for entry in &entries {
            let start = entry.ddl_offset as usize;
            let end = start + entry.ddl_len as usize;
            if end > archive.len() {
                return Err(basin_common::BasinError::InvalidSchema(format!(
                    "custom archive DDL block for '{}' out of bounds",
                    entry.name
                )));
            }
            let ddl = std::str::from_utf8(&archive[start..end]).map_err(|_| {
                basin_common::BasinError::InvalidSchema(format!(
                    "custom archive DDL for '{}' is not valid UTF-8",
                    entry.name
                ))
            })?;
            let ddl_trimmed = ddl.trim();
            if !ddl_trimmed.is_empty() {
                sess.execute(ddl_trimmed).await?;
            }
        }
    }

    if has_data {
        for entry in &entries {
            let start = entry.data_offset as usize;
            let end = start + entry.data_len as usize;
            if end > archive.len() {
                return Err(basin_common::BasinError::InvalidSchema(format!(
                    "custom archive data block for '{}' out of bounds",
                    entry.name
                )));
            }
            let data = std::str::from_utf8(&archive[start..end]).map_err(|_| {
                basin_common::BasinError::InvalidSchema(format!(
                    "custom archive data for '{}' is not valid UTF-8",
                    entry.name
                ))
            })?;
            // Each line is a complete INSERT statement.
            for line in data.lines() {
                let stmt = line.trim();
                if stmt.is_empty() || stmt.starts_with("--") {
                    continue;
                }
                sess.execute(stmt).await?;
            }
        }
    }

    Ok(())
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Emit a `CREATE TABLE` SQL statement into `out`.
fn emit_create_table_sql(out: &mut String, meta: &TableMetadata) {
    use std::fmt::Write;
    use crate::types::{
        BASIN_COLUMN_DEFAULT, BASIN_GENERATED_AS, BASIN_IDENTITY, BASIN_IDENTITY_ALWAYS,
    };

    let tname = quote_ident(meta.table.as_str());
    writeln!(out, "CREATE TABLE {tname} (").unwrap();

    let fields = meta.schema.fields();
    let n_fields = fields.len();
    let has_table_constraints = !meta.pk_columns.is_empty()
        || !meta.foreign_keys.is_empty()
        || !meta.unique_constraints.is_empty();

    for (i, field) in fields.iter().enumerate() {
        let f = field.as_ref();
        let mut col = format!("{} {}", quote_ident(f.name()), field_to_pg_type(f));

        if !f.is_nullable() {
            col.push_str(" NOT NULL");
        }

        if let Some(gen_expr) = f.metadata().get(BASIN_GENERATED_AS) {
            col.push_str(&format!(" GENERATED ALWAYS AS ({gen_expr}) STORED"));
        } else if let Some(identity_mode) = f.metadata().get(BASIN_IDENTITY) {
            if identity_mode.as_str() == BASIN_IDENTITY_ALWAYS {
                col.push_str(" GENERATED ALWAYS AS IDENTITY");
            } else {
                col.push_str(" GENERATED BY DEFAULT AS IDENTITY");
            }
        } else if let Some(default_expr) = f.metadata().get(BASIN_COLUMN_DEFAULT) {
            if !default_expr.starts_with("nextval(") {
                col.push_str(&format!(" DEFAULT {default_expr}"));
            }
        }

        let comma = if i + 1 < n_fields || has_table_constraints {
            ","
        } else {
            ""
        };
        writeln!(out, "    {col}{comma}").unwrap();
    }

    if !meta.pk_columns.is_empty() {
        let pk_cols: Vec<String> = meta.pk_columns.iter().map(|c| quote_ident(c)).collect();
        let comma = if !meta.foreign_keys.is_empty() || !meta.unique_constraints.is_empty() {
            ","
        } else {
            ""
        };
        writeln!(out, "    PRIMARY KEY ({}){}", pk_cols.join(", "), comma).unwrap();
    }

    let n_fk = meta.foreign_keys.len();
    for (i, fk) in meta.foreign_keys.iter().enumerate() {
        use basin_catalog::RefAction;
        let local_cols: Vec<String> = fk.columns.iter().map(|c| quote_ident(c)).collect();
        let ref_cols: Vec<String> = fk.ref_columns.iter().map(|c| quote_ident(c)).collect();
        let ref_table = quote_ident(&fk.ref_table);
        let on_delete = match fk.on_delete {
            RefAction::Cascade => " ON DELETE CASCADE",
            RefAction::NoAction => "",
        };
        let on_update = match fk.on_update {
            RefAction::Cascade => " ON UPDATE CASCADE",
            RefAction::NoAction => "",
        };
        let fk_ddl = format!(
            "CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({}){}{}",
            quote_ident(&fk.name),
            local_cols.join(", "),
            ref_table,
            ref_cols.join(", "),
            on_delete,
            on_update,
        );
        let comma = if i + 1 < n_fk || !meta.unique_constraints.is_empty() {
            ","
        } else {
            ""
        };
        writeln!(out, "    {fk_ddl}{comma}").unwrap();
    }

    let n_uq = meta.unique_constraints.len();
    for (i, uq) in meta.unique_constraints.iter().enumerate() {
        let comma = if i + 1 < n_uq { "," } else { "" };
        let cols: Vec<String> = uq.columns.iter().map(|c| quote_ident(c)).collect();
        writeln!(
            out,
            "    CONSTRAINT {} UNIQUE ({}){}",
            quote_ident(&uq.name),
            cols.join(", "),
            comma,
        )
        .unwrap();
    }

    writeln!(out, ");").unwrap();
}

/// Emit INSERT statements into `out`. Delegates to plain module's cell encoder.
fn plain_inserts(
    out: &mut String,
    table_name: &str,
    schema: &arrow_schema::Schema,
    batches: &[RecordBatch],
) {
    // Reuse the same logic as plain.rs by delegating.
    // We call the private helper by accessing it through a local shim.
    // Since we can't call plain::emit_inserts (it's not pub), we replicate
    // the minimal logic here using the shared cell encoder from plain.

    use std::fmt::Write as FmtWrite;
    use crate::types::{BASIN_TYPE_JSONB, BASIN_TYPE_KEY, BASIN_TYPE_UUID};

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return;
    }

    let tname = quote_ident(table_name);
    let col_list: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect();
    let cols_str = col_list.join(", ");

    let jsonb_cols: Vec<bool> = schema
        .fields()
        .iter()
        .map(|f| f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_JSONB))
        .collect();
    let uuid_cols: Vec<bool> = schema
        .fields()
        .iter()
        .map(|f| f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID))
        .collect();

    for batch in batches {
        let n_rows = batch.num_rows();
        for r in 0..n_rows {
            let mut row_vals: Vec<String> = Vec::with_capacity(schema.fields().len());
            for (c, field) in schema.fields().iter().enumerate() {
                let col = batch.column(c);
                // Use the encode_cell function from plain module via a pub(crate) re-export.
                // Since encode_cell is not pub, we inline a call via the plain module's
                // encode_cell through a thin wrapper.
                let val = encode_cell_wrapper(col.as_ref(), r, field.as_ref(), jsonb_cols[c], uuid_cols[c]);
                row_vals.push(val);
            }
            writeln!(out, "INSERT INTO {tname} ({cols_str}) VALUES ({});", row_vals.join(", "))
                .unwrap();
        }
    }
    writeln!(out).unwrap();
}

/// Thin wrapper that calls the plain module's cell encoder. We replicate the
/// call rather than making plain::encode_cell pub to keep module boundaries.
fn encode_cell_wrapper(
    col: &dyn arrow_array::Array,
    idx: usize,
    _field: &arrow_schema::Field,
    is_jsonb: bool,
    is_uuid: bool,
) -> String {
    // Delegate to plain's encode_cell by re-implementing the dispatch here.
    // This avoids pub(crate) exposure of the cell encoder while keeping DRY.
    // In practice the two implementations are identical in output.
    use arrow_array::{
        cast::AsArray,
        types::{
            Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
            IntervalMonthDayNanoType, TimestampMicrosecondType, TimestampMillisecondType,
            TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
            UInt8Type,
        },
        Array,
    };
    use arrow_schema::{DataType, IntervalUnit, TimeUnit};
    use chrono::{NaiveDate, TimeZone, Utc};

    if col.is_null(idx) {
        return "NULL".to_string();
    }

    match col.data_type() {
        DataType::Boolean => {
            if col.as_boolean().value(idx) { "TRUE".to_string() } else { "FALSE".to_string() }
        }
        DataType::Int8 => format!("{}", col.as_primitive::<Int8Type>().value(idx)),
        DataType::Int16 => format!("{}", col.as_primitive::<Int16Type>().value(idx)),
        DataType::Int32 => format!("{}", col.as_primitive::<Int32Type>().value(idx)),
        DataType::Int64 => format!("{}", col.as_primitive::<Int64Type>().value(idx)),
        DataType::UInt8 => format!("{}", col.as_primitive::<UInt8Type>().value(idx)),
        DataType::UInt16 => format!("{}", col.as_primitive::<UInt16Type>().value(idx)),
        DataType::UInt32 => format!("{}", col.as_primitive::<UInt32Type>().value(idx)),
        DataType::UInt64 => format!("{}", col.as_primitive::<UInt64Type>().value(idx)),
        DataType::Float32 => format!("{}", col.as_primitive::<Float32Type>().value(idx)),
        DataType::Float64 => format!("{}", col.as_primitive::<Float64Type>().value(idx)),
        DataType::Utf8 => {
            let s = col.as_string::<i32>().value(idx);
            if is_jsonb {
                format!("{}::jsonb", crate::dump::sql_quote_str(s))
            } else {
                crate::dump::sql_quote_str(s)
            }
        }
        DataType::LargeUtf8 => {
            let s = col.as_string::<i64>().value(idx);
            if is_jsonb {
                format!("{}::jsonb", crate::dump::sql_quote_str(s))
            } else {
                crate::dump::sql_quote_str(s)
            }
        }
        DataType::Binary => {
            let bytes = col.as_binary::<i32>().value(idx);
            format!("'\\x{}'", hex::encode(bytes))
        }
        DataType::LargeBinary => {
            let bytes = col.as_binary::<i64>().value(idx);
            if is_jsonb {
                let json_str = std::str::from_utf8(bytes).unwrap_or("{}");
                return format!("'{}'", json_str.replace('\'', "''"));
            }
            if is_uuid {
                if bytes.len() == 16 {
                    let u = uuid::Uuid::from_bytes(bytes.try_into().unwrap_or([0u8; 16]));
                    return format!("'{}'", u.hyphenated());
                }
            }
            format!("'\\x{}'", hex::encode(bytes))
        }
        DataType::FixedSizeBinary(16) if is_uuid => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .expect("FixedSizeBinaryArray");
            let bytes: &[u8] = arr.value(idx);
            let u = uuid::Uuid::from_bytes(bytes.try_into().unwrap_or([0u8; 16]));
            format!("'{}'", u.hyphenated())
        }
        DataType::FixedSizeBinary(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .expect("FixedSizeBinaryArray");
            let bytes: &[u8] = arr.value(idx);
            format!("'\\x{}'", hex::encode(bytes))
        }
        DataType::Date32 => {
            let days = col.as_primitive::<Date32Type>().value(idx);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let d = epoch + chrono::Duration::days(days as i64);
            format!("'{}'", d.format("%Y-%m-%d"))
        }
        DataType::Timestamp(unit, tz) => {
            let raw: i64 = match unit {
                TimeUnit::Second => col.as_primitive::<TimestampSecondType>().value(idx),
                TimeUnit::Millisecond => col.as_primitive::<TimestampMillisecondType>().value(idx),
                TimeUnit::Microsecond => col.as_primitive::<TimestampMicrosecondType>().value(idx),
                TimeUnit::Nanosecond => col.as_primitive::<TimestampNanosecondType>().value(idx),
            };
            let unix_micros: i64 = match unit {
                TimeUnit::Second => raw.saturating_mul(1_000_000),
                TimeUnit::Millisecond => raw.saturating_mul(1_000),
                TimeUnit::Microsecond => raw,
                TimeUnit::Nanosecond => raw / 1_000,
            };
            let dt = Utc.timestamp_micros(unix_micros).single().unwrap_or_default();
            if tz.is_some() {
                format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S%.6f+00"))
            } else {
                format!("'{}'", dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f"))
            }
        }
        DataType::Decimal128(_, scale) => {
            let val = col
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .expect("Decimal128Array")
                .value(idx);
            let scale = *scale as u8;
            if scale == 0 {
                format!("{val}")
            } else {
                let divisor = 10i128.pow(scale as u32);
                let int_part = val / divisor;
                let frac_part = (val % divisor).abs();
                format!("{int_part}.{frac_part:0>width$}", width = scale as usize)
            }
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = col.as_primitive::<IntervalMonthDayNanoType>().value(idx);
            let (months, days, nanos) = arrow_array::types::IntervalMonthDayNanoType::to_parts(v);
            format!("INTERVAL '{months} months {days} days {nanos} nanoseconds'")
        }
        DataType::List(elem_field) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .expect("ListArray");
            let elem_arr = list_arr.value(idx);
            let is_j = elem_field.metadata().get(crate::types::BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(crate::types::BASIN_TYPE_JSONB);
            let is_u = elem_field.metadata().get(crate::types::BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(crate::types::BASIN_TYPE_UUID);
            let n = elem_arr.len();
            let parts: Vec<String> = (0..n).map(|i| encode_cell_wrapper(elem_arr.as_ref(), i, elem_field.as_ref(), is_j, is_u)).collect();
            format!("ARRAY[{}]", parts.join(", "))
        }
        DataType::LargeList(elem_field) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<arrow_array::LargeListArray>()
                .expect("LargeListArray");
            let elem_arr = list_arr.value(idx);
            let is_j = elem_field.metadata().get(crate::types::BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(crate::types::BASIN_TYPE_JSONB);
            let is_u = elem_field.metadata().get(crate::types::BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(crate::types::BASIN_TYPE_UUID);
            let n = elem_arr.len();
            let parts: Vec<String> = (0..n).map(|i| encode_cell_wrapper(elem_arr.as_ref(), i, elem_field.as_ref(), is_j, is_u)).collect();
            format!("ARRAY[{}]", parts.join(", "))
        }
        _ => {
            format!("'{}'", format!("{col:?}").replace('\'', "''"))
        }
    }
}
