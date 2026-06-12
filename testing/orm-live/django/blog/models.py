from django.contrib.postgres.fields import ArrayField
from django.db import models


class Author(models.Model):
    name = models.CharField(max_length=100, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "dj_authors"


class Book(models.Model):
    author = models.ForeignKey(Author, on_delete=models.CASCADE, related_name="books")
    title = models.CharField(max_length=200)
    pages = models.IntegerField(default=0)
    meta = models.JSONField(null=True)
    published_at = models.DateTimeField(null=True)
    # ArrayField (PG-only): exercises the text[] column kind plus the
    # __contains / __overlap lookups. Added via the 0002 ALTER-heavy migration.
    tags = ArrayField(models.CharField(max_length=50), default=list, blank=True)
    # Optimistic-locking version counter, also added in 0002.
    version = models.IntegerField(default=0)

    class Meta:
        db_table = "dj_books"
        unique_together = [("author", "title")]
