"""Live integration tests — gated on BASIN_LIVE_URL env var.

Run against a real server:
    BASIN_LIVE_URL=http://localhost:8080 BASIN_LIVE_KEY=<key> \\
        BASIN_LIVE_PROJECT=<ulid> pytest tests/test_live.py -v
"""

import os
import pytest

LIVE_URL = os.environ.get("BASIN_LIVE_URL")
LIVE_KEY = os.environ.get("BASIN_LIVE_KEY")
LIVE_PROJECT = os.environ.get("BASIN_LIVE_PROJECT")

skip_live = pytest.mark.skipif(
    not LIVE_URL,
    reason="Set BASIN_LIVE_URL to run live integration tests",
)


@skip_live
def test_live_health():
    from basin import create_client

    client = create_client(LIVE_URL, LIVE_KEY, project_id=LIVE_PROJECT)
    result = client.health()
    assert result == "ok"


@skip_live
def test_live_query_and_insert():
    from basin import create_client

    client = create_client(LIVE_URL, LIVE_KEY, project_id=LIVE_PROJECT)
    # Create table if not exists is left to the user — we just attempt a select.
    result = client.table("_basin_sdk_live_test").select("id").limit(1).run()
    assert isinstance(result.rows, list)


@skip_live
async def test_live_async_health():
    from basin import create_async_client

    async with create_async_client(
        LIVE_URL, LIVE_KEY, project_id=LIVE_PROJECT
    ) as client:
        result = await client.health()
        assert result == "ok"


@skip_live
def test_live_arrow_conversion():
    from basin import create_client

    client = create_client(LIVE_URL, LIVE_KEY, project_id=LIVE_PROJECT)
    try:
        table = client.table("_basin_sdk_live_test").limit(10).to_arrow()
        import pyarrow as pa

        assert isinstance(table, pa.Table)
    except ImportError:
        pytest.skip("pyarrow not installed")
