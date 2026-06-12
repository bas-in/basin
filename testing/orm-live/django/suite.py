#!/usr/bin/env python3
"""Live Django ORM suite against BASIN_DSN.

Drives Django's OWN machinery: makemigrations (offline) + migrate (its
migration executor and schema editor), then the ORM query compiler,
transaction.atomic with savepoints, F() expressions and bulk_create.

TAP-ish output: "ok - django.<test>" / "not ok - django.<test> # <reason>".
"""

import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
RUN = time.time_ns()


def ok(name):
    print(f"ok - django.{name}", flush=True)


def notok(name, reason):
    reason = " ".join(str(reason).split())[:300]
    print(f"not ok - django.{name} # {reason}", flush=True)


REMAINING = ["crud", "select-related", "atomic-commit", "savepoint-rollback",
             "f-update", "bulk-create"]

if not os.environ.get("BASIN_DSN"):
    notok("migrate", "BASIN_DSN not set")
    sys.exit(0)

# ── 1. Django's migration engine: makemigrations (offline) + migrate ────────
try:
    for args in (["makemigrations", "blog", "--noinput"], ["migrate", "--noinput"]):
        r = subprocess.run(
            [sys.executable, os.path.join(HERE, "manage.py"), *args],
            capture_output=True, text=True, timeout=180, cwd=HERE,
        )
        if r.returncode != 0:
            raise RuntimeError((r.stderr or r.stdout).strip()[-300:])
    ok("migrate")
    migrate_ok = True
except Exception as e:  # noqa: BLE001
    notok("migrate", e)
    migrate_ok = False

# ── ORM session ──────────────────────────────────────────────────────────────
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "basinsite.settings")
sys.path.insert(0, HERE)
import django  # noqa: E402

django.setup()

from django.db import connection, transaction  # noqa: E402
from django.db.models import F  # noqa: E402
from blog.models import Author, Book  # noqa: E402

try:
    with connection.cursor() as cur:
        cur.execute("SELECT 1")
except Exception as e:  # noqa: BLE001
    notok("connect", e)
    for t in REMAINING:
        notok(t, "SKIP-CASCADE: no connection")
    sys.exit(0)
else:
    ok("connect")

if not migrate_ok:
    # Tables may still exist from a previous run; probe before cascading.
    try:
        Author.objects.exists()
    except Exception:  # noqa: BLE001
        for t in REMAINING:
            notok(t, "SKIP-CASCADE: migrate failed and tables absent")
        sys.exit(0)


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
    a = Author.objects.create(name=f"alice-{RUN}")
    check(a.pk and a.created_at is not None, "create hydrates pk + auto_now_add")
    b = Book.objects.create(author=a, title="One", pages=100, meta={"genre": "db"})
    got = Book.objects.get(pk=b.pk)
    check(got.meta == {"genre": "db"}, "JSONField round-trips")
    got.pages = 110
    got.save()
    check(Book.objects.get(pk=b.pk).pages == 110, "update persisted")
    victim = Book.objects.create(author=a, title="Victim")
    victim.delete()
    check(not Book.objects.filter(title="Victim", author=a).exists(), "delete persisted")


def t_select_related():
    book = Book.objects.select_related("author").get(title="One", author__name=f"alice-{RUN}")
    check(book.author.name == f"alice-{RUN}", "select_related JOIN hydrates the FK")


def t_atomic_commit():
    with transaction.atomic():
        a = Author.objects.create(name=f"tx-{RUN}")
        Book.objects.create(author=a, title="InTx")
    check(Author.objects.filter(name=f"tx-{RUN}").exists(), "committed atomic block visible")


def t_savepoint_rollback():
    with transaction.atomic():
        Author.objects.create(name=f"outer-{RUN}")
        try:
            with transaction.atomic():  # inner block -> SAVEPOINT
                Author.objects.create(name=f"inner-{RUN}")
                raise RuntimeError("force inner rollback")
        except RuntimeError:
            pass
    check(Author.objects.filter(name=f"outer-{RUN}").exists(), "outer write committed")
    check(not Author.objects.filter(name=f"inner-{RUN}").exists(),
          "inner write rolled back to savepoint")


def t_f_update():
    n = Book.objects.filter(title="One", author__name=f"alice-{RUN}").update(pages=F("pages") + 5)
    check(n == 1, "F() update affected exactly one row")
    check(Book.objects.get(title="One", author__name=f"alice-{RUN}").pages == 115,
          "F() arithmetic applied server-side (110 + 5)")


def t_bulk_create():
    objs = Author.objects.bulk_create([Author(name=f"bulk-{RUN}-{i}") for i in range(5)])
    check(len(objs) == 5, "bulk_create returned all objects")
    check(Author.objects.filter(name__startswith=f"bulk-{RUN}-").count() == 5,
          "all bulk rows persisted")
    # ignore_conflicts path (INSERT ... ON CONFLICT DO NOTHING).
    Author.objects.bulk_create(
        [Author(name=f"bulk-{RUN}-0")], ignore_conflicts=True)
    check(Author.objects.filter(name=f"bulk-{RUN}-0").count() == 1,
          "ignore_conflicts did not duplicate")


test("crud", t_crud)
test("select-related", t_select_related)
test("atomic-commit", t_atomic_commit)
test("savepoint-rollback", t_savepoint_rollback)
test("f-update", t_f_update)
test("bulk-create", t_bulk_create)
