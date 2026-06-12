# The ALTER-heavy second migration: exercises Django's schema editor across
# the breadth of ALTER variants real apps emit after the initial CreateModel —
# AddField (incl. a PG ArrayField -> text[]), an index add, a column rename
# round-trip, and a column-type alter. Committed explicitly so `makemigrations`
# is a no-op on a fresh checkout (the model already matches this end state).

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("blog", "0001_initial"),
    ]

    operations = [
        # AddField — text[] ArrayField (ALTER TABLE … ADD COLUMN tags varchar(50)[]).
        migrations.AddField(
            model_name="book",
            name="tags",
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.CharField(max_length=50),
                blank=True,
                default=list,
                size=None,
            ),
        ),
        # AddField — optimistic-lock version counter (ALTER TABLE … ADD COLUMN version int).
        migrations.AddField(
            model_name="book",
            name="version",
            field=models.IntegerField(default=0),
        ),
        # AddIndex — Django emits CREATE INDEX on the column set.
        migrations.AddIndex(
            model_name="book",
            index=models.Index(fields=["pages"], name="dj_books_pages_idx"),
        ),
        # Raw ALTER breadth Django's schema editor also emits across migrations:
        # a rename round-trip and a column-type alter. RunSQL with reverse_sql so
        # the migration is reversible (matching real app migrations).
        migrations.RunSQL(
            sql='ALTER TABLE "dj_books" RENAME COLUMN "pages" TO "page_count"',
            reverse_sql='ALTER TABLE "dj_books" RENAME COLUMN "page_count" TO "pages"',
        ),
        migrations.RunSQL(
            sql='ALTER TABLE "dj_books" RENAME COLUMN "page_count" TO "pages"',
            reverse_sql='ALTER TABLE "dj_books" RENAME COLUMN "pages" TO "page_count"',
        ),
        migrations.RunSQL(
            sql='ALTER TABLE "dj_books" ALTER COLUMN "title" TYPE varchar(255)',
            reverse_sql='ALTER TABLE "dj_books" ALTER COLUMN "title" TYPE varchar(200)',
        ),
    ]
