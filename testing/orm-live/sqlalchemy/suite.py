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


REMAINING = ["crud", "relationships", "selectinload", "session-tx-rollback", "savepoint"]

DSN = os.environ.get("BASIN_DSN", "")
if not DSN:
    notok("connect", "BASIN_DSN not set")
    sys.exit(0)

from sqlalchemy import create_engine, select, text  # noqa: E402
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


test("crud", t_crud)
test("relationships", t_relationships)
test("selectinload", t_selectinload)
test("session-tx-rollback", t_session_tx_rollback)
test("savepoint", t_savepoint)
