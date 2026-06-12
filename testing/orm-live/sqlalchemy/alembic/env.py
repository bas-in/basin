"""Alembic environment — url comes from BASIN_DSN."""

import os
import sys

from alembic import context
from sqlalchemy import create_engine

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from models import Base  # noqa: E402

target_metadata = Base.metadata


def _url() -> str:
    dsn = os.environ.get("BASIN_DSN", "")
    # psycopg2 dialect: postgres:// -> postgresql://
    return dsn.replace("postgres://", "postgresql://", 1)


def run_migrations_offline() -> None:
    context.configure(url=_url(), target_metadata=target_metadata,
                      literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_url(), connect_args={"connect_timeout": 10})
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
