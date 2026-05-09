// pgx (v5) smoke test against an in-process Basin server. Mirrors the
// Python asyncpg smoke: same operations, same PASS/FAIL contract on stdout.
// The Rust harness boots Basin on an ephemeral port and `go run`s this file
// with `--url postgres://...`.
//
// Format-code note (v0.1): pgx-v5 picks the result-row scan codec from the
// `Format` field on the server's RowDescription (rows.go:257 in v5.5.5).
// Basin's PoC RowDescription always advertises text (format=0) even when
// Bind requested binary, so we pass `pgx.QueryResultFormats{}` on every
// row-returning call to force text format end-to-end. A richer pgtype-based
// scan path (binary INT8 / JSONB unwrap / pgtype.UUID) is deferred until
// Basin's portal-describe surfaces per-column format codes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func run(ctx context.Context, url string) error {
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	// In-memory catalog is fresh per test boot; no DROP needed (and Basin
	// doesn't support DROP TABLE IF EXISTS today).
	if _, err := conn.Exec(ctx,
		"CREATE TABLE smoke_go ("+
			"id BIGINT NOT NULL, "+
			"name TEXT NOT NULL, "+
			"active BOOLEAN NOT NULL, "+
			"score DOUBLE PRECISION NOT NULL)"); err != nil {
		return fmt.Errorf("create: %w", err)
	}

	if _, err := conn.Exec(ctx,
		"INSERT INTO smoke_go (id, name, active, score) VALUES "+
			"($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)",
		int64(1), "alpha", true, 1.5,
		int64(2), "beta", false, 2.5,
		int64(3), "gamma", true, 3.5,
	); err != nil {
		return fmt.Errorf("multi-row insert: %w", err)
	}

	rows, err := conn.Query(ctx,
		"SELECT id, name, score FROM smoke_go WHERE active = $1 ORDER BY id LIMIT $2",
		pgx.QueryResultFormats{}, true, int64(5))
	if err != nil {
		return fmt.Errorf("select: %w", err)
	}
	type rec struct {
		id    int64
		name  string
		score float64
	}
	var got []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.id, &r.name, &r.score); err != nil {
			rows.Close()
			return fmt.Errorf("scan: %w", err)
		}
		got = append(got, r)
	}
	rows.Close()
	if rows.Err() != nil {
		return fmt.Errorf("rows: %w", rows.Err())
	}
	if len(got) != 2 || got[0].id != 1 || got[1].id != 3 {
		return fmt.Errorf("WHERE/ORDER/LIMIT got %+v", got)
	}

	// Prepared statement: real ORMs (gorm, ent, sqlc) cache by SQL text.
	if _, err := conn.Prepare(ctx, "by_id", "SELECT name FROM smoke_go WHERE id = $1"); err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	for _, c := range []struct {
		id   int64
		want string
	}{{1, "alpha"}, {2, "beta"}, {3, "gamma"}} {
		var name string
		if err := conn.QueryRow(ctx, "by_id", pgx.QueryResultFormats{}, c.id).Scan(&name); err != nil {
			return fmt.Errorf("prepared lookup id=%d: %w", c.id, err)
		}
		if name != c.want {
			return fmt.Errorf("prepared lookup id=%d got %q want %q", c.id, name, c.want)
		}
	}

	// JSONB + UUID round-trip.
	if _, err := conn.Exec(ctx,
		"CREATE TABLE smoke_go_types (id UUID NOT NULL, payload JSONB NOT NULL)"); err != nil {
		return fmt.Errorf("create types: %w", err)
	}
	rowUUID := uuid.New()
	payload := map[string]any{"k": "v", "n": float64(42), "nested": []any{float64(1), float64(2), float64(3)}}
	payloadBytes, _ := json.Marshal(payload)
	if _, err := conn.Exec(ctx,
		"INSERT INTO smoke_go_types (id, payload) VALUES ($1, $2)",
		rowUUID, string(payloadBytes)); err != nil {
		return fmt.Errorf("insert types: %w", err)
	}
	// Mirror the Python sibling: WHERE id = $1 over UUID is a deferred fast-select
	// limitation (v0.2); the unfiltered SELECT round-trips today.
	var gotID uuid.UUID
	var gotPayloadRaw []byte
	if err := conn.QueryRow(ctx,
		"SELECT id, payload FROM smoke_go_types", pgx.QueryResultFormats{},
	).Scan(&gotID, &gotPayloadRaw); err != nil {
		return fmt.Errorf("select types: %w", err)
	}
	if gotID != rowUUID {
		return fmt.Errorf("UUID mismatch: got %v want %v", gotID, rowUUID)
	}
	var gotPayload map[string]any
	if err := json.Unmarshal(gotPayloadRaw, &gotPayload); err != nil {
		return fmt.Errorf("JSONB decode: %w (raw=%q)", err, string(gotPayloadRaw))
	}
	if !reflect.DeepEqual(gotPayload, payload) {
		return fmt.Errorf("JSONB mismatch: got %v want %v", gotPayload, payload)
	}

	// Transaction shape — deferred per CAPABILITIES.md, so a rejection here is
	// reported as a known limitation rather than a hard failure.
	txStatus := "ok"
	tx, err := conn.Begin(ctx)
	if err != nil {
		txStatus = fmt.Sprintf("known-limitation: %v", err)
	} else {
		if _, err := tx.Exec(ctx,
			"INSERT INTO smoke_go (id, name, active, score) VALUES ($1, $2, $3, $4)",
			int64(99), "tx-row", true, 9.9); err != nil {
			_ = tx.Rollback(ctx)
			txStatus = fmt.Sprintf("known-limitation: insert in tx: %v", err)
		} else if err := tx.Commit(ctx); err != nil {
			txStatus = fmt.Sprintf("known-limitation: commit: %v", err)
		}
	}
	fmt.Printf("transaction: %s\n", txStatus)
	fmt.Println("PASS")
	return nil
}

func main() {
	url := flag.String("url", "", "postgres://user@host:port/")
	flag.Parse()
	if *url == "" {
		fmt.Println("FAIL: --url is required")
		os.Exit(2)
	}
	if err := run(context.Background(), *url); err != nil {
		// pgx wraps errors; surface the chain.
		fmt.Printf("FAIL: %v\n", err)
		var unwrapped error = err
		for {
			next := errors.Unwrap(unwrapped)
			if next == nil {
				break
			}
			fmt.Printf("    cause: %v\n", next)
			unwrapped = next
		}
		os.Exit(1)
	}
}
