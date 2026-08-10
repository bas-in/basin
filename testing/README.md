# testing/ — test configuration and live harnesses

Everything here supports the test suites; nothing here is compiled into a
release binary.

## Test-config profiles

Basin's integration tests read one TOML config. It is resolved in this order
(`tests/integration/src/test_config.rs`):

1. `$BASIN_TEST_CONFIG=<path>` — an explicit path, always wins
2. `./.basin-test.toml` — project-local, **gitignored, never committed**,
   because it holds real cloud credentials
3. `~/.basin/test-config.toml` — per-user, shared across checkouts
4. otherwise every gated test prints `[skip]` and returns `Ok`

The files below are the committed, credential-free profiles. None of them is
picked up implicitly — each is selected by pointing `BASIN_TEST_CONFIG` at it,
so adding one here can never change what an unconfigured `cargo test` does.

| File | What it configures |
|---|---|
| [`basin-test.example.toml`](./basin-test.example.toml) | Template for your own `.basin-test.toml`. Copy it to the repo root and fill in real credentials. |
| [`basin-test.seaweedfs.toml`](./basin-test.seaweedfs.toml) | Local SeaweedFS S3 gateway on loopback (~1 ms/op). The structural-bug detector. |
| [`basin-test.tigris-realistic.toml`](./basin-test.tigris-realistic.toml) | The same SeaweedFS gateway plus a 9 ms per-op inject (~10 ms/op), approximating same-region Tigris RTT. |
| [`basin-test.https.toml`](./basin-test.https.toml) | Local MinIO over HTTPS with HTTP/2, for exercising the TLS + h2 path. |
| [`basin-three-way.example.toml`](./basin-three-way.example.toml) | Template for `.basin-three-way.toml`, the Neon / Supabase / Basin cloud comparison harness config. Copy to the repo root and fill in the three DSNs. |
| [`seaweedfs-s3.json`](./seaweedfs-s3.json) | SeaweedFS's own `-s3.config` identity file, so the gateway's keys match the profiles above. |

Three of these form the benchmark realism ladder — see
[`benchmark/run/_setup.sh`](../benchmark/run/_setup.sh):

```
.basin-test.toml                         pure LocalFS   ~0 ms/op   fastest CI gate
testing/basin-test.seaweedfs.toml        SeaweedFS LB   ~1 ms/op   structural-bug detector
testing/basin-test.tigris-realistic.toml LB + 9 ms      ~10 ms/op  Tigris same-region proxy
```

Example:

```sh
BASIN_TEST_CONFIG=./testing/basin-test.seaweedfs.toml \
  cargo test -p basin-integration-tests --test s3_credentials_smoke -- --ignored --nocapture
```

The two live-credential files — `.basin-test.toml` and
`.basin-three-way.toml` — stay at the repo root, where the loader and the
benchmark harnesses look for them, and both are gitignored.

## Harnesses

- [`orm-live/`](./orm-live/) — six real ORM applications (Prisma, Drizzle,
  TypeORM, Django, SQLAlchemy, GORM) driven against a live `basin-server`
  over pgwire.
