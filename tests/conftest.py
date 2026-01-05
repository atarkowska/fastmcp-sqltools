"""Pytest configuration and fixtures for SQL MCP Server tests"""

import os
import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_postgres_url():
    """Set PostgreSQL DATABASE_URL environment variable for tests"""
    original_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = "postgresql://test:test@localhost:5432/testdb"
    yield "postgresql://test:test@localhost:5432/testdb"
    if original_url:
        os.environ["DATABASE_URL"] = original_url
    else:
        os.environ.pop("DATABASE_URL", None)


@pytest.fixture
def mock_mysql_url():
    """Set MySQL DATABASE_URL environment variable for tests"""
    original_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = "mysql://test:test@localhost:3306/testdb"
    yield "mysql://test:test@localhost:3306/testdb"
    if original_url:
        os.environ["DATABASE_URL"] = original_url
    else:
        os.environ.pop("DATABASE_URL", None)


@pytest.fixture
def mock_sqlite_url():
    """Set SQLite DATABASE_URL environment variable for tests"""
    original_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = "sqlite:///test.db"
    yield "sqlite:///test.db"
    if original_url:
        os.environ["DATABASE_URL"] = original_url
    else:
        os.environ.pop("DATABASE_URL", None)


@pytest.fixture(autouse=True)
def reset_adapter():
    """Reset the global database adapter before each test"""
    from src.fastmcp_sqltools.server import db_manager
    db_manager._adapter = None
    yield
    db_manager._adapter = None
