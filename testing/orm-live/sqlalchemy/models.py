"""SQLAlchemy 2.0-style declarative models for the live harness."""

import datetime
from typing import Optional

from sqlalchemy import ForeignKey, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


class Base(DeclarativeBase):
    pass


class Author(Base):
    __tablename__ = "sa_authors"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    books: Mapped[list["Book"]] = relationship(back_populates="author")


class Book(Base):
    __tablename__ = "sa_books"
    __table_args__ = (UniqueConstraint("author_id", "title"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    author_id: Mapped[int] = mapped_column(ForeignKey("sa_authors.id"))
    title: Mapped[str]
    pages: Mapped[int] = mapped_column(default=0)
    # JSONB (was generic JSON) so the @> / ? / ->> operator round-trips can be
    # exercised. published_at + the new tags/version columns broaden coverage.
    meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    published_at: Mapped[Optional[datetime.datetime]] = mapped_column(nullable=True)
    # PG ARRAY column (added in the 0002 ALTER-heavy migration).
    tags: Mapped[Optional[list]] = mapped_column(ARRAY(String), nullable=True)
    # Optimistic-lock version counter (also 0002).
    version: Mapped[int] = mapped_column(default=0, server_default="0")

    author: Mapped[Author] = relationship(back_populates="books")
