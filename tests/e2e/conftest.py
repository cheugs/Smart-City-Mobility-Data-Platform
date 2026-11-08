"""Fixtures for the end-to-end tests (helpers live in e2e_helpers.py)."""

import uuid

import pytest

from e2e_helpers import KafkaSender, sql, wait_for


@pytest.fixture(scope="session")
def kafka():
    """One Kafka producer shared by all end-to-end tests."""
    return KafkaSender()


@pytest.fixture(scope="session", autouse=True)
def stack_is_running():
    """Wait until every Spark query has processed at least one batch (first start can take a few minutes)."""
    wait_for(lambda: sql("select count(distinct query_name) from monitoring.stream_progress")[0][0] >= 4,
             timeout=480, every=5, message="the four Spark streaming queries to start")


@pytest.fixture
def tag():
    """A unique prefix so each test only looks at its own events."""
    return f"E2E{uuid.uuid4().hex[:10]}"
