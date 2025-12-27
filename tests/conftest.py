"""Pytest configuration and fixtures for SQL MCP Server tests"""

import os
import pytest
import asyncpg
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture
def mock_database_url():
    """Set DATABASE_URL environment variable for tests"""
    original_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = "postgresql://test:test@localhost:5432/testdb"
    yield "postgresql://test:test@localhost:5432/testdb"
    if original_url:
        os.environ["DATABASE_URL"] = original_url
    else:
        os.environ.pop("DATABASE_URL", None)


@pytest.fixture
def mock_pool():
    """Create a mock asyncpg pool"""
    pool = AsyncMock(spec=asyncpg.Pool)
    return pool


@pytest.fixture
def mock_connection():
    """Create a mock database connection"""
    conn = AsyncMock(spec=asyncpg.Connection)
    return conn


@pytest.fixture
def mock_pool_with_connection(mock_pool, mock_connection):
    """Create a mock pool that returns a mock connection"""
    mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
    mock_pool.acquire.return_value.__aexit__.return_value = None
    return mock_pool


@pytest.fixture
def sample_tables_data():
    """Sample data for list_tables query"""
    return [
        {"table_name": "users", "table_type": "BASE TABLE"},
        {"table_name": "posts", "table_type": "BASE TABLE"},
        {"table_name": "comments", "table_type": "BASE TABLE"},
    ]


@pytest.fixture
def sample_schema_data():
    """Sample data for get_table_schema query"""
    return [
        {
            "column_name": "id",
            "data_type": "integer",
            "character_maximum_length": None,
            "is_nullable": "NO",
            "column_default": "nextval('users_id_seq'::regclass)",
        },
        {
            "column_name": "username",
            "data_type": "character varying",
            "character_maximum_length": 50,
            "is_nullable": "NO",
            "column_default": None,
        },
        {
            "column_name": "email",
            "data_type": "character varying",
            "character_maximum_length": 100,
            "is_nullable": "YES",
            "column_default": None,
        },
    ]


@pytest.fixture
def sample_query_results():
    """Sample data for query results"""
    return [
        {"id": 1, "username": "alice", "email": "alice@example.com"},
        {"id": 2, "username": "bob", "email": "bob@example.com"},
        {"id": 3, "username": "charlie", "email": "charlie@example.com"},
    ]


@pytest.fixture(autouse=True)
def reset_pool():
    """Reset the global pool before each test"""
    import src.fastmcp_sqltools.server as server
    server._connection_pool = None
    yield
    server._connection_pool = None


def create_mock_record(data: dict):
    """Create a mock asyncpg Record that behaves like the real thing"""
    mock_record = MagicMock()
    # Make it iterable (returns key-value pairs)
    mock_record.__iter__.return_value = iter(data.items())
    # Make dict() work on it
    mock_record.keys.return_value = data.keys()
    mock_record.__getitem__.side_effect = lambda key: data[key]
    # Make attribute access work
    for key, value in data.items():
        setattr(mock_record, key, value)
    return mock_record
