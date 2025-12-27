"""Tests for SQL MCP Server functionality"""

import os
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import asyncpg

from src.fastmcp_sqltools import server
from tests.conftest import create_mock_record

# Get the actual functions from the decorated tool objects
get_pool = server.get_pool
list_tables = server.list_tables.fn
get_table_schema = server.get_table_schema.fn
execute_query = server.execute_query.fn
execute_safe_query = server.execute_safe_query.fn


class TestGetPool:
    """Tests for get_pool function"""

    @pytest.mark.asyncio
    async def test_get_pool_creates_pool_with_database_url(self, mock_database_url):
        """Test that get_pool creates a pool with the DATABASE_URL"""
        with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = AsyncMock(spec=asyncpg.Pool)
            mock_create_pool.return_value = mock_pool

            result = await get_pool()

            assert result == mock_pool
            mock_create_pool.assert_called_once_with(mock_database_url)

    @pytest.mark.asyncio
    async def test_get_pool_reuses_existing_pool(self, mock_database_url):
        """Test that get_pool reuses existing pool instead of creating new one"""
        with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = AsyncMock(spec=asyncpg.Pool)
            mock_create_pool.return_value = mock_pool

            # First call creates pool
            pool1 = await get_pool()
            # Second call should reuse pool
            pool2 = await get_pool()

            assert pool1 == pool2
            assert mock_create_pool.call_count == 1

    @pytest.mark.asyncio
    async def test_get_pool_raises_error_without_database_url(self):
        """Test that get_pool raises ValueError when DATABASE_URL is not set"""
        # Remove DATABASE_URL if it exists
        original_url = os.environ.get("DATABASE_URL")
        if "DATABASE_URL" in os.environ:
            del os.environ["DATABASE_URL"]

        try:
            with pytest.raises(ValueError, match="DATABASE_URL environment variable is required"):
                await get_pool()
        finally:
            # Restore original URL if it existed
            if original_url:
                os.environ["DATABASE_URL"] = original_url


class TestListTables:
    """Tests for list_tables function"""

    @pytest.mark.asyncio
    async def test_list_tables_default_schema(
        self, mock_database_url, mock_pool_with_connection, mock_connection, sample_tables_data
    ):
        """Test listing tables from default public schema"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            # Mock the fetch to return sample data as Record objects
            mock_records = [create_mock_record(table) for table in sample_tables_data]
            mock_connection.fetch.return_value = mock_records

            result = await list_tables()

            assert len(result) == 3
            assert result[0]["table_name"] == "users"
            assert result[1]["table_name"] == "posts"
            mock_connection.fetch.assert_called_once()
            call_args = mock_connection.fetch.call_args
            assert "information_schema.tables" in call_args[0][0]
            assert call_args[0][1] == "public"

    @pytest.mark.asyncio
    async def test_list_tables_custom_schema(
        self, mock_database_url, mock_pool_with_connection, mock_connection
    ):
        """Test listing tables from a custom schema"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_connection.fetch.return_value = []

            result = await list_tables(schema="custom_schema")

            assert result == []
            call_args = mock_connection.fetch.call_args
            assert call_args[0][1] == "custom_schema"

    @pytest.mark.asyncio
    async def test_list_tables_empty_result(
        self, mock_database_url, mock_pool_with_connection, mock_connection
    ):
        """Test listing tables when no tables exist"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_connection.fetch.return_value = []

            result = await list_tables()

            assert result == []


class TestGetTableSchema:
    """Tests for get_table_schema function"""

    @pytest.mark.asyncio
    async def test_get_table_schema_success(
        self, mock_database_url, mock_pool_with_connection, mock_connection, sample_schema_data
    ):
        """Test getting schema for a table"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_records = [create_mock_record(col) for col in sample_schema_data]
            mock_connection.fetch.return_value = mock_records

            result = await get_table_schema("users")

            assert len(result) == 3
            assert result[0]["column_name"] == "id"
            assert result[1]["column_name"] == "username"
            assert result[0]["is_nullable"] == "NO"
            mock_connection.fetch.assert_called_once()
            call_args = mock_connection.fetch.call_args
            assert "information_schema.columns" in call_args[0][0]
            assert call_args[0][1] == "public"
            assert call_args[0][2] == "users"

    @pytest.mark.asyncio
    async def test_get_table_schema_custom_schema(
        self, mock_database_url, mock_pool_with_connection, mock_connection
    ):
        """Test getting schema from custom schema"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_connection.fetch.return_value = []

            result = await get_table_schema("users", schema="custom_schema")

            assert result == []
            call_args = mock_connection.fetch.call_args
            assert call_args[0][1] == "custom_schema"
            assert call_args[0][2] == "users"

    @pytest.mark.asyncio
    async def test_get_table_schema_nonexistent_table(
        self, mock_database_url, mock_pool_with_connection, mock_connection
    ):
        """Test getting schema for non-existent table returns empty list"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_connection.fetch.return_value = []

            result = await get_table_schema("nonexistent_table")

            assert result == []


class TestExecuteQuery:
    """Tests for execute_query function"""

    @pytest.mark.asyncio
    async def test_execute_query_without_params(
        self, mock_database_url, mock_pool_with_connection, mock_connection, sample_query_results
    ):
        """Test executing query without parameters"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_records = [create_mock_record(row) for row in sample_query_results]
            mock_connection.fetch.return_value = mock_records

            result = await execute_query("SELECT * FROM users")

            assert len(result) == 3
            assert result[0]["username"] == "alice"
            assert result[1]["username"] == "bob"
            mock_connection.fetch.assert_called_once_with("SELECT * FROM users")

    @pytest.mark.asyncio
    async def test_execute_query_with_params(
        self, mock_database_url, mock_pool_with_connection, mock_connection
    ):
        """Test executing query with parameters"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_record = create_mock_record({"id": 1, "username": "alice"})
            mock_connection.fetch.return_value = [mock_record]

            result = await execute_query(
                "SELECT * FROM users WHERE id = $1", params=[1]
            )

            assert len(result) == 1
            mock_connection.fetch.assert_called_once_with(
                "SELECT * FROM users WHERE id = $1", 1
            )

    @pytest.mark.asyncio
    async def test_execute_query_multiple_params(
        self, mock_database_url, mock_pool_with_connection, mock_connection
    ):
        """Test executing query with multiple parameters"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_connection.fetch.return_value = []

            result = await execute_query(
                "SELECT * FROM users WHERE id = $1 AND username = $2",
                params=[1, "alice"]
            )

            assert result == []
            mock_connection.fetch.assert_called_once_with(
                "SELECT * FROM users WHERE id = $1 AND username = $2", 1, "alice"
            )

    @pytest.mark.asyncio
    async def test_execute_query_empty_result(
        self, mock_database_url, mock_pool_with_connection, mock_connection
    ):
        """Test executing query that returns no results"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_connection.fetch.return_value = []

            result = await execute_query("SELECT * FROM users WHERE id = -1")

            assert result == []


class TestExecuteSafeQuery:
    """Tests for execute_safe_query function"""

    @pytest.mark.asyncio
    async def test_execute_safe_query_select_only(
        self, mock_database_url, mock_pool_with_connection, mock_connection, sample_query_results
    ):
        """Test executing safe SELECT query"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            # Mock the transaction context manager
            mock_transaction = AsyncMock()
            mock_transaction.__aenter__.return_value = None
            mock_transaction.__aexit__.return_value = None
            mock_connection.transaction.return_value = mock_transaction

            mock_records = [create_mock_record(row) for row in sample_query_results]
            mock_connection.fetch.return_value = mock_records

            result = await execute_safe_query("SELECT * FROM users")

            assert len(result) == 3
            assert result[0]["username"] == "alice"
            mock_connection.transaction.assert_called_once_with(readonly=True)
            mock_connection.fetch.assert_called_once_with("SELECT * FROM users")

    @pytest.mark.asyncio
    async def test_execute_safe_query_with_params(
        self, mock_database_url, mock_pool_with_connection, mock_connection
    ):
        """Test executing safe query with parameters"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_transaction = AsyncMock()
            mock_transaction.__aenter__.return_value = None
            mock_transaction.__aexit__.return_value = None
            mock_connection.transaction.return_value = mock_transaction

            mock_connection.fetch.return_value = []

            result = await execute_safe_query(
                "SELECT * FROM users WHERE id = $1", params=[1]
            )

            assert result == []
            mock_connection.fetch.assert_called_once_with(
                "SELECT * FROM users WHERE id = $1", 1
            )

    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_insert(self, mock_database_url):
        """Test that execute_safe_query rejects INSERT statements"""
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            await execute_safe_query("INSERT INTO users (username) VALUES ('test')")

    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_update(self, mock_database_url):
        """Test that execute_safe_query rejects UPDATE statements"""
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            await execute_safe_query("UPDATE users SET username = 'test' WHERE id = 1")

    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_delete(self, mock_database_url):
        """Test that execute_safe_query rejects DELETE statements"""
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            await execute_safe_query("DELETE FROM users WHERE id = 1")

    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_drop(self, mock_database_url):
        """Test that execute_safe_query rejects DROP statements"""
        with pytest.raises(ValueError, match="Query contains disallowed keyword: DROP"):
            await execute_safe_query("SELECT * FROM users; DROP TABLE users;")

    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_create(self, mock_database_url):
        """Test that execute_safe_query rejects CREATE statements"""
        with pytest.raises(ValueError, match="Query contains disallowed keyword: CREATE"):
            await execute_safe_query("SELECT * FROM users; CREATE TABLE test (id int);")

    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_alter(self, mock_database_url):
        """Test that execute_safe_query rejects ALTER statements"""
        with pytest.raises(ValueError, match="Query contains disallowed keyword: ALTER"):
            await execute_safe_query("SELECT * FROM users; ALTER TABLE users ADD COLUMN test text;")

    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_truncate(self, mock_database_url):
        """Test that execute_safe_query rejects TRUNCATE statements"""
        with pytest.raises(ValueError, match="Query contains disallowed keyword: TRUNCATE"):
            await execute_safe_query("SELECT * FROM users; TRUNCATE TABLE users;")

    @pytest.mark.asyncio
    async def test_execute_safe_query_case_insensitive(self, mock_database_url):
        """Test that execute_safe_query validation is case-insensitive"""
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            await execute_safe_query("insert into users (username) values ('test')")

        with pytest.raises(ValueError, match="Query contains disallowed keyword: UPDATE"):
            await execute_safe_query("select * from users; uPdAtE users set username = 'x';")

    @pytest.mark.asyncio
    async def test_execute_safe_query_with_whitespace(
        self, mock_database_url, mock_pool_with_connection, mock_connection
    ):
        """Test that execute_safe_query handles queries with leading whitespace"""
        with patch("src.fastmcp_sqltools.server.get_pool", return_value=mock_pool_with_connection):
            mock_transaction = AsyncMock()
            mock_transaction.__aenter__.return_value = None
            mock_transaction.__aexit__.return_value = None
            mock_connection.transaction.return_value = mock_transaction

            mock_connection.fetch.return_value = []

            result = await execute_safe_query("  \n  SELECT * FROM users")

            assert result == []
            mock_connection.transaction.assert_called_once_with(readonly=True)
