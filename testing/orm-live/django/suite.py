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
             "f-update", "bulk-create", "annotate-aggregate", "distinct-on",
             "json-lookups", "array-lookups", "date-lookups", "select-for-update",
             "select-for-update-skip-locked", "bulk-update", "values-list",
             "count-exists-latest"]

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
from django.db.models import Avg, Count, F, Q, Sum  # noqa: E402
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


def t_annotate_aggregate():
    a = Author.objects.create(name=f"agg-{RUN}")
    Book.objects.create(author=a, title="A1", pages=10)
    Book.objects.create(author=a, title="A2", pages=30)
    # annotate(Count/Sum/Avg) + values — the reporting groupBy shape.
    rows = list(
        Book.objects.filter(author=a)
        .values("author")
        .annotate(n=Count("id"), total=Sum("pages"), avg=Avg("pages"))
    )
    check(len(rows) == 1, "annotate+values grouped into one author row")
    check(rows[0]["n"] == 2 and rows[0]["total"] == 40 and rows[0]["avg"] == 20,
          "Count/Sum/Avg computed server-side")
    # aggregate() scalar form.
    agg = Book.objects.filter(author=a).aggregate(total=Sum("pages"))
    check(agg["total"] == 40, "aggregate() scalar sum")


def t_distinct_on():
    # Django distinct('col') compiles to Postgres DISTINCT ON (col).
    a = Author.objects.create(name=f"dist-{RUN}")
    Book.objects.create(author=a, title="D1", pages=1)
    Book.objects.create(author=a, title="D2", pages=2)
    rows = list(
        Book.objects.filter(author=a).order_by("author_id", "pages").distinct("author_id")
    )
    check(len(rows) == 1, "DISTINCT ON (author_id) collapsed to one row per author")


def t_json_lookups():
    a = Author.objects.create(name=f"json-{RUN}")
    Book.objects.create(author=a, title="J1", meta={"genre": "db", "tags": ["x"]})
    # __contains (JSONB @>).
    check(Book.objects.filter(author=a, meta__contains={"genre": "db"}).exists(),
          "JSONField __contains (@>) matched")
    # __has_key (jsonb ?).
    check(Book.objects.filter(author=a, meta__has_key="genre").exists(),
          "JSONField __has_key (?) matched")
    # key transform (->> 'genre').
    check(Book.objects.filter(author=a, meta__genre="db").exists(),
          "JSONField key transform (->>) matched")


def t_array_lookups():
    a = Author.objects.create(name=f"arr-{RUN}")
    Book.objects.create(author=a, title="Arr1", tags=["rust", "db"])
    Book.objects.create(author=a, title="Arr2", tags=["go"])
    # ArrayField __contains (@>).
    check(Book.objects.filter(author=a, tags__contains=["rust"]).count() == 1,
          "ArrayField __contains (@>) matched exactly one")
    # ArrayField __overlap (&&).
    check(Book.objects.filter(author=a, tags__overlap=["go", "rust"]).count() == 2,
          "ArrayField __overlap (&&) matched both")


def t_date_lookups():
    import datetime
    a = Author.objects.create(name=f"date-{RUN}")
    Book.objects.create(author=a, title="Dt1",
                        published_at=datetime.datetime(2024, 3, 1, 12, 0, tzinfo=datetime.timezone.utc))
    # __year filter (EXTRACT).
    check(Book.objects.filter(author=a, published_at__year=2024).exists(),
          "published_at__year filter (EXTRACT) matched")
    # __date filter (::date cast comparison).
    check(Book.objects.filter(author=a, published_at__date=datetime.date(2024, 3, 1)).exists(),
          "published_at__date filter matched")


def t_select_for_update():
    with transaction.atomic():
        rows = list(Book.objects.select_for_update().filter(title="A1", author__name=f"agg-{RUN}"))
        check(len(rows) >= 1, "select_for_update() returned the locked row inside atomic")


def t_select_for_update_skip_locked():
    with transaction.atomic():
        rows = list(
            Book.objects.select_for_update(skip_locked=True)
            .filter(author__name=f"agg-{RUN}").order_by("id")[:1]
        )
        check(isinstance(rows, list), "select_for_update(skip_locked=True) executed")


def t_bulk_update():
    a = Author.objects.create(name=f"bu-{RUN}")
    b1 = Book.objects.create(author=a, title="BU1", pages=1)
    b2 = Book.objects.create(author=a, title="BU2", pages=2)
    b1.pages, b2.pages = 100, 200
    Book.objects.bulk_update([b1, b2], ["pages"])
    check(Book.objects.get(pk=b1.pk).pages == 100 and Book.objects.get(pk=b2.pk).pages == 200,
          "bulk_update persisted both rows via the CASE UPDATE")


def t_values_list():
    a = Author.objects.create(name=f"vl-{RUN}")
    Book.objects.create(author=a, title="VL1", pages=7)
    flat = list(Book.objects.filter(author=a).values_list("pages", flat=True))
    check(flat == [7], "values_list(flat=True) projected the single scalar column")


def t_count_exists_latest():
    a = Author.objects.create(name=f"cel-{RUN}")
    Book.objects.create(author=a, title="CEL1", pages=1)
    Book.objects.create(author=a, title="CEL2", pages=2)
    check(Book.objects.filter(author=a).count() == 2, "count() over the author's books")
    check(Book.objects.filter(author=a).exists(), "exists() truthy")
    latest = Book.objects.filter(author=a).latest("id")
    check(latest.title == "CEL2", "latest('id') returns the highest-id row")
    first = Book.objects.filter(author=a).order_by("id").first()
    check(first.title == "CEL1", "first() returns the lowest-id row")
    # Q-object OR filter (the compound-WHERE shape).
    n = Book.objects.filter(Q(title="CEL1") | Q(title="CEL2"), author=a).count()
    check(n == 2, "Q() OR filter matched both")


test("crud", t_crud)
test("select-related", t_select_related)
test("atomic-commit", t_atomic_commit)
test("savepoint-rollback", t_savepoint_rollback)
test("f-update", t_f_update)
test("bulk-create", t_bulk_create)
test("annotate-aggregate", t_annotate_aggregate)
test("distinct-on", t_distinct_on)
test("json-lookups", t_json_lookups)
test("array-lookups", t_array_lookups)
test("date-lookups", t_date_lookups)
test("select-for-update", t_select_for_update)
test("select-for-update-skip-locked", t_select_for_update_skip_locked)
test("bulk-update", t_bulk_update)
test("values-list", t_values_list)
test("count-exists-latest", t_count_exists_latest)
