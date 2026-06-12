"""SQLAlchemy 2.0-style declarative models for the live harness."""

import datetime
from typing import Optional

from sqlalchemy import ForeignKey, JSON, UniqueConstraint, func
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
    meta: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    published_at: Mapped[Optional[datetime.datetime]] = mapped_column(nullable=True)

    author: Mapped[Author] = relationship(back_populates="books")
