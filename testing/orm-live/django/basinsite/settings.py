"""Minimal Django settings for the live-ORM harness.

The database comes from BASIN_DSN (postgres://user:pass@host:port/db).
Only the `blog` app is installed so `manage.py migrate` applies exactly the
harness models — no auth/contenttypes DDL noise.
"""

import os
from urllib.parse import urlparse

_dsn = urlparse(os.environ.get("BASIN_DSN", "postgres://basin:basin@127.0.0.1:54329/basin"))

SECRET_KEY = "basin-orm-live-harness"
DEBUG = True
USE_TZ = True
INSTALLED_APPS = ["blog"]
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": (_dsn.path or "/basin").lstrip("/") or "basin",
        "USER": _dsn.username or "",
        "PASSWORD": _dsn.password or "",
        "HOST": _dsn.hostname or "127.0.0.1",
        "PORT": str(_dsn.port or 5432),
        "OPTIONS": {"sslmode": "disable", "connect_timeout": 10},
    }
}
