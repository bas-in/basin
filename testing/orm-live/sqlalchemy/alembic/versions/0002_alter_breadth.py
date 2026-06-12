"""0002 — ALTER-heavy migration: add ARRAY + version columns, index, type
alter, and a rename round-trip. Exercises Alembic's op.* schema-editor breadth
(the ALTER variants every real app emits after the initial create_table).

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-12

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ARRAY

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # op.add_column — PG ARRAY text[] column.
    op.add_column("sa_books", sa.Column("tags", ARRAY(sa.String()), nullable=True))
    # op.add_column — optimistic-lock version counter with a server default.
    op.add_column(
        "sa_books",
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
    )
    # op.create_index — index add breadth.
    op.create_index("ix_sa_books_pages", "sa_books", ["pages"])
    # op.alter_column(type_=...) — column-type widening.
    op.alter_column("sa_books", "title", type_=sa.String(length=255))
    # op.alter_column(new_column_name=...) — rename round-trip (rename, then
    # rename back so the model column name still matches).
    op.alter_column("sa_books", "pages", new_column_name="page_count")
    op.alter_column("sa_books", "page_count", new_column_name="pages")


def downgrade() -> None:
    op.drop_index("ix_sa_books_pages", table_name="sa_books")
    op.drop_column("sa_books", "version")
    op.drop_column("sa_books", "tags")
