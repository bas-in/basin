#!/usr/bin/env python3
"""Live SQLAlchemy 2.0 suite against BASIN_DSN.

Drives SQLAlchemy's OWN machinery: Alembic's migration runner (which creates
its alembic_version tracker table), the 2.0-style ORM (Session, relationships,
selectinload), and the session transaction manager incl. nested savepoints.

TAP-ish output: "ok - sqlalchemy.<test>" / "not ok - sqlalchemy.<test> # <reason>".
"""

import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
RUN = time.time_ns()


def ok(name):
    print(f"ok - sqlalchemy.{name}", flush=True)


def notok(name, reason):
    reason = " ".join(str(reason).split())[:300]
    print(f"not ok - sqlalchemy.{name} # {reason}", flush=True)


REMAINING = ["crud", "relationships", "selectinload", "session-tx-rollback", "savepoint",
             "aggregates-groupby", "with-for-update", "with-for-update-skip-locked",
             "jsonb-ops", "array-ops", "numeric-roundtrip", "date-trunc",
             "exists-scalar", "upsert-composite"]

DSN = os.environ.get("BASIN_DSN", "")
if not DSN:
    notok("connect", "BASIN_DSN not set")
    sys.exit(0)

from sqlalchemy import create_engine, func, select, text  # noqa: E402
from sqlalchemy.dialects.postgresql import insert as pg_insert  # noqa: E402
from sqlalchemy.orm import Session, selectinload  # noqa: E402

sys.path.insert(0, HERE)
from models import Author, Book  # noqa: E402

engine = create_engine(
    DSN.replace("postgres://", "postgresql://", 1),
    connect_args={"connect_timeout": 10},
)

# ── 1. connect ───────────────────────────────────────────────────────────────
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
except Exception as e:  # noqa: BLE001
    notok("connect", e)
    notok("alembic-upgrade", "SKIP-CASCADE: no connection")
    for t in REMAINING:
        notok(t, "SKIP-CASCADE: no connection")
    sys.exit(0)
else:
    ok("connect")

# ── 2. Alembic's migration runner ────────────────────────────────────────────
try:
    r = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        capture_output=True, text=True, timeout=180, cwd=HERE,
        env={**os.environ, "BASIN_DSN": DSN},
    )
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout).strip()[-300:])
    ok("alembic-upgrade")
except Exception as e:  # noqa: BLE001
    notok("alembic-upgrade", e)
    # Fallback (not a test): create tables directly so the ORM-path tests
    # still measure honestly.
    try:
        from models import Base

        Base.metadata.create_all(engine)
        print("[sqlalchemy] alembic failed - applied metadata.create_all fallback",
              file=sys.stderr)
    except Exception as e2:  # noqa: BLE001
        print(f"[sqlalchemy] fallback DDL failed: {e2}", file=sys.stderr)


def test(name, fn):
    try:
        fn()
        ok(name)
    except Exception as e:  # noqa: BLE001
        notok(name, e)


def check(cond, msg):
    if not cond:
        raise AssertionError(f"assertion failed: {msg}")


def t_crud():
    with Session(engine) as session, session.begin():
        a = Author(name=f"alice-{RUN}")
        session.add(a)
        session.flush()
        check(a.id is not None, "flush populated the PK (RETURNING/sequence)")
        session.add(Book(author_id=a.id, title="One", pages=100, meta={"genre": "db"}))
    with Session(engine) as session:
        book = session.scalars(select(Book).where(Book.title == "One",
                                                  Book.author_id == a.id)).one()
        check(book.meta == {"genre": "db"}, "JSON round-trips")
        book.pages = 110
        session.commit()
    with Session(engine) as session:
        check(session.scalars(select(Book.pages).where(Book.id == book.id)).one() == 110,
              "unit-of-work UPDATE persisted")


def t_relationships():
    with Session(engine) as session, session.begin():
        a = Author(name=f"rel-{RUN}")
        a.books = [Book(title="R1"), Book(title="R2", pages=7)]
        session.add(a)
    with Session(engine) as session:
        got = session.scalars(select(Author).where(Author.name == f"rel-{RUN}")).one()
        check(len(got.books) == 2, "relationship cascade-inserted + lazy-loaded both children")
        check(got.books[0].author.id == got.id, "back_populates wired")


def t_selectinload():
    with Session(engine) as session:
        # selectinload: one extra IN-list query, the canonical eager strategy.
        got = session.scalars(
            select(Author)
            .options(selectinload(Author.books))
            .where(Author.name == f"rel-{RUN}")
        ).one()
        check(len(got.books) == 2, "selectinload hydrated children eagerly")


def t_session_tx_rollback():
    with Session(engine) as session:
        session.add(Author(name=f"rb-{RUN}"))
        session.flush()
        session.rollback()
    with Session(engine) as session:
        check(
            session.scalars(select(Author).where(Author.name == f"rb-{RUN}")).first() is None,
            "rolled-back flush must not be visible",
        )


def t_savepoint():
    with Session(engine) as session, session.begin():
        session.add(Author(name=f"sp-outer-{RUN}"))
        try:
            with session.begin_nested():  # SAVEPOINT
                session.add(Author(name=f"sp-inner-{RUN}"))
                session.flush()
                raise RuntimeError("force savepoint rollback")
        except RuntimeError:
            pass
    with Session(engine) as session:
        check(session.scalars(select(Author).where(Author.name == f"sp-outer-{RUN}")).first()
              is not None, "outer write committed")
        check(session.scalars(select(Author).where(Author.name == f"sp-inner-{RUN}")).first()
              is None, "inner write rolled back to savepoint")


def t_aggregates_groupby():
    with Session(engine) as session, session.begin():
        a = Author(name=f"agg-{RUN}")
        a.books = [Book(title="AG1", pages=10), Book(title="AG2", pages=30)]
        session.add(a)
    with Session(engine) as session:
        aid = session.scalars(select(Author.id).where(Author.name == f"agg-{RUN}")).one()
        row = session.execute(
            select(
                Book.author_id,
                func.count(Book.id).label("n"),
                func.sum(Book.pages).label("total"),
                func.avg(Book.pages).label("avg"),
            )
            .where(Book.author_id == aid)
            .group_by(Book.author_id)
        ).one()
        check(row.n == 2 and row.total == 40, "group_by Count/Sum computed server-side")


def t_with_for_update():
    with Session(engine) as session:
        with session.begin():
            rows = session.scalars(
                select(Book).where(Book.title == "AG1").with_for_update()
            ).all()
            check(len(rows) >= 1, "with_for_update() returned the locked row")


def t_with_for_update_skip_locked():
    with Session(engine) as session:
        with session.begin():
            rows = session.scalars(
                select(Book).limit(1).with_for_update(skip_locked=True)
            ).all()
            check(isinstance(rows, list), "with_for_update(skip_locked=True) executed")


def t_jsonb_ops():
    with Session(engine) as session, session.begin():
        a = Author(name=f"json-{RUN}")
        a.books = [Book(title="J1", meta={"genre": "db", "n": 3})]
        session.add(a)
    with Session(engine) as session:
        # ->> astext extraction filter.
        got = session.scalars(
            select(Book).where(Book.title == "J1", Book.meta["genre"].astext == "db")
        ).first()
        check(got is not None, "JSONB ['key'].astext (->>) filter matched")
        # @> containment.
        got2 = session.scalars(
            select(Book).where(Book.title == "J1", Book.meta.contains({"genre": "db"}))
        ).first()
        check(got2 is not None, "JSONB .contains() (@>) filter matched")
        # ? has_key.
        got3 = session.scalars(
            select(Book).where(Book.title == "J1", Book.meta.has_key("genre"))
        ).first()
        check(got3 is not None, "JSONB .has_key() (?) filter matched")


def t_array_ops():
    with Session(engine) as session, session.begin():
        a = Author(name=f"arr-{RUN}")
        a.books = [Book(title="ARR1", tags=["rust", "db"]), Book(title="ARR2", tags=["go"])]
        session.add(a)
    with Session(engine) as session:
        aid = session.scalars(select(Author.id).where(Author.name == f"arr-{RUN}")).one()
        # ARRAY .contains() (@>).
        n = session.scalars(
            select(func.count(Book.id)).where(Book.author_id == aid, Book.tags.contains(["rust"]))
        ).one()
        check(n == 1, "ARRAY .contains() (@>) matched exactly one")
        # ARRAY .overlap() (&&).
        m = session.scalars(
            select(func.count(Book.id)).where(Book.author_id == aid, Book.tags.overlap(["go", "rust"]))
        ).one()
        check(m == 2, "ARRAY .overlap() (&&) matched both")


def t_numeric_roundtrip():
    with Session(engine) as session, session.begin():
        a = Author(name=f"num-{RUN}")
        a.books = [Book(title="N1", pages=42)]
        session.add(a)
    with Session(engine) as session:
        val = session.scalars(
            select(Book.pages + 1).where(Book.title == "N1")
        ).one()
        check(val == 43, "server-side integer arithmetic round-trips (42+1)")


def t_date_trunc():
    import datetime
    with Session(engine) as session, session.begin():
        a = Author(name=f"dt-{RUN}")
        a.books = [
            Book(title="DT1", published_at=datetime.datetime(2024, 3, 1, tzinfo=datetime.timezone.utc)),
            Book(title="DT2", published_at=datetime.datetime(2024, 3, 15, tzinfo=datetime.timezone.utc)),
        ]
        session.add(a)
    with Session(engine) as session:
        aid = session.scalars(select(Author.id).where(Author.name == f"dt-{RUN}")).one()
        rows = session.execute(
            select(func.date_trunc("month", Book.published_at).label("m"), func.count(Book.id))
            .where(Book.author_id == aid, Book.published_at.isnot(None))
            .group_by(func.date_trunc("month", Book.published_at))
        ).all()
        check(len(rows) == 1, "date_trunc('month') grouped both March rows together")


def t_exists_scalar():
    with Session(engine) as session:
        present = session.scalar(
            select(select(Book.id).where(Book.title == "AG1").exists())
        )
        check(present is True, "scalar EXISTS subquery returned True")
        n = session.scalar(select(func.count()).select_from(Book))
        check(isinstance(n, int) and n > 0, "func.count() scalar returned a positive int")


def t_upsert_composite():
    # PG-dialect insert().on_conflict_do_update over the (author_id, title)
    # unique constraint — the composite-target upsert.
    with Session(engine) as session, session.begin():
        a = Author(name=f"up-{RUN}")
        session.add(a)
        session.flush()
        aid = a.id
        session.execute(
            pg_insert(Book.__table__)
            .values(author_id=aid, title="UP1", pages=1)
            .on_conflict_do_update(
                index_elements=["author_id", "title"], set_={"pages": 2}
            )
        )
    with Session(engine) as session:
        aid = session.scalars(select(Author.id).where(Author.name == f"up-{RUN}")).one()
        pages = session.scalars(
            select(Book.pages).where(Book.author_id == aid, Book.title == "UP1")
        ).one()
        check(pages in (1, 2), "composite-target upsert inserted/updated the row")


test("crud", t_crud)
test("relationships", t_relationships)
test("selectinload", t_selectinload)
test("session-tx-rollback", t_session_tx_rollback)
test("savepoint", t_savepoint)
test("aggregates-groupby", t_aggregates_groupby)
test("with-for-update", t_with_for_update)
test("with-for-update-skip-locked", t_with_for_update_skip_locked)
test("jsonb-ops", t_jsonb_ops)
test("array-ops", t_array_ops)
test("numeric-roundtrip", t_numeric_roundtrip)
test("date-trunc", t_date_trunc)
test("exists-scalar", t_exists_scalar)
test("upsert-composite", t_upsert_composite)
