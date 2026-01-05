"""Basic tests for fastmcp-sqltools"""

import os
import pytest
from unittest.mock import AsyncMock, patch

from src.fastmcp_sqltools.server import (
    db_manager,
    PostgresAdapter,
    MySQLAdapter,
    SQLiteAdapter,
)


class TestAdapterFactory:
    """Test database adapter factory"""

    def test_creates_postgres_adapter(self, mock_postgres_url):
        """Test PostgreSQL adapter is created for postgresql:// URL"""
        adapter = db_manager.get_adapter()
        assert isinstance(adapter, PostgresAdapter)

    def test_creates_mysql_adapter(self, mock_mysql_url):
        """Test MySQL adapter is created for mysql:// URL"""
        adapter = db_manager.get_adapter()
        assert isinstance(adapter, MySQLAdapter)

    def test_creates_sqlite_adapter(self, mock_sqlite_url):
        """Test SQLite adapter is created for sqlite:// URL"""
        adapter = db_manager.get_adapter()
        assert isinstance(adapter, SQLiteAdapter)


class TestPostgresAdapter:
    """Test PostgreSQL adapter"""

    def test_list_tables_query(self):
        """Test PostgreSQL list_tables query"""
        adapter = PostgresAdapter()
        query, params = adapter.get_list_tables_query("public")
        
        assert "information_schema.tables" in query
        assert "public" in params

    def test_table_schema_query(self):
        """Test PostgreSQL table_schema query"""
        adapter = PostgresAdapter()
        query, params = adapter.get_table_schema_query("users", "public")
        
        assert "information_schema.columns" in query
        assert params == ["public", "users"]


class TestMySQLAdapter:
    """Test MySQL adapter"""

    def test_list_tables_query_with_schema(self):
        """Test MySQL list_tables query with schema"""
        adapter = MySQLAdapter()
        query, params = adapter.get_list_tables_query("mydb")
        
        assert "information_schema.tables" in query
        assert params == ["mydb"]

    def test_list_tables_query_without_schema(self):
        """Test MySQL list_tables query without schema"""
        adapter = MySQLAdapter()
        query, params = adapter.get_list_tables_query(None)
        
        assert "DATABASE()" in query
        assert params == []


class TestSQLiteAdapter:
    """Test SQLite adapter"""

    def test_list_tables_query(self):
        """Test SQLite list_tables query"""
        adapter = SQLiteAdapter()
        query, params = adapter.get_list_tables_query()
        
        assert "sqlite_master" in query
        assert params == []

    def test_table_schema_query(self):
        """Test SQLite table_schema query"""
        adapter = SQLiteAdapter()
        query, params = adapter.get_table_schema_query("users")
        
        assert "PRAGMA table_info" in query
        assert "users" in query


class TestListTables:
    """Test list_tables function"""

    @pytest.mark.asyncio
    async def test_returns_table_list(self, mock_postgres_url):
        """Test list_tables returns table list"""
        from src.fastmcp_sqltools.server import list_tables
        
        with patch('src.fastmcp_sqltools.server.PostgresAdapter') as MockAdapter:
            mock_adapter = MockAdapter.return_value
            mock_adapter.get_list_tables_query.return_value = ("SELECT", [])
            mock_adapter.fetch = AsyncMock(return_value=[
                {"table_name": "users", "table_type": "BASE TABLE"},
                {"table_name": "posts", "table_type": "BASE TABLE"}
            ])
            
            with patch('src.fastmcp_sqltools.server.db_manager.get_adapter', return_value=mock_adapter):
                result = await list_tables.fn()
                
                assert len(result) == 2
                assert result[0]["table_name"] == "users"
                assert result[1]["table_name"] == "posts"


class TestGetTableSchema:
    """Test get_table_schema function"""

    @pytest.mark.asyncio
    async def test_returns_column_list(self, mock_postgres_url):
        """Test get_table_schema returns column list"""
        from src.fastmcp_sqltools.server import get_table_schema
        
        with patch('src.fastmcp_sqltools.server.PostgresAdapter') as MockAdapter:
            mock_adapter = MockAdapter.return_value
            mock_adapter.get_table_schema_query.return_value = ("SELECT", [])
            mock_adapter.fetch = AsyncMock(return_value=[
                {
                    "column_name": "id",
                    "data_type": "integer",
                    "is_nullable": "NO",
                    "character_maximum_length": None,
                    "column_default": None
                }
            ])
            
            with patch('src.fastmcp_sqltools.server.db_manager.get_adapter', return_value=mock_adapter):
                result = await get_table_schema.fn("users")
                
                assert len(result) == 1
                assert result[0]["column_name"] == "id"


class TestExecuteQuery:
    """Test execute_query function"""

    @pytest.mark.asyncio
    async def test_executes_query(self, mock_postgres_url):
        """Test execute_query executes SQL"""
        from src.fastmcp_sqltools.server import execute_query
        
        with patch('src.fastmcp_sqltools.server.PostgresAdapter') as MockAdapter:
            mock_adapter = MockAdapter.return_value
            mock_adapter.fetch = AsyncMock(return_value=[{"count": 5}])
            
            with patch('src.fastmcp_sqltools.server.db_manager.get_adapter', return_value=mock_adapter):
                result = await execute_query.fn("SELECT COUNT(*) as count FROM users")
                
                assert len(result) == 1
                assert result[0]["count"] == 5

    @pytest.mark.asyncio
    async def test_executes_with_params(self, mock_postgres_url):
        """Test execute_query with parameters"""
        from src.fastmcp_sqltools.server import execute_query
        
        with patch('src.fastmcp_sqltools.server.PostgresAdapter') as MockAdapter:
            mock_adapter = MockAdapter.return_value
            mock_adapter.fetch = AsyncMock(return_value=[])
            
            with patch('src.fastmcp_sqltools.server.db_manager.get_adapter', return_value=mock_adapter):
                result = await execute_query.fn("SELECT * FROM users WHERE id = $1", params=[1])
                
                assert result == []
                mock_adapter.fetch.assert_called_once_with("SELECT * FROM users WHERE id = $1", [1])


class TestExecuteSafeQuery:
    """Test execute_safe_query function"""

    @pytest.mark.asyncio
    async def test_allows_select(self, mock_postgres_url):
        """Test execute_safe_query allows SELECT"""
        from src.fastmcp_sqltools.server import execute_safe_query
        
        with patch('src.fastmcp_sqltools.server.PostgresAdapter') as MockAdapter:
            mock_adapter = MockAdapter.return_value
            mock_adapter.fetch = AsyncMock(return_value=[])
            
            with patch('src.fastmcp_sqltools.server.db_manager.get_adapter', return_value=mock_adapter):
                result = await execute_safe_query.fn("SELECT * FROM users")
                assert result == []

    @pytest.mark.asyncio
    async def test_rejects_insert(self):
        """Test execute_safe_query rejects INSERT"""
        from src.fastmcp_sqltools.server import execute_safe_query
        
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            await execute_safe_query.fn("INSERT INTO users VALUES (1, 'test')")

    @pytest.mark.asyncio
    async def test_rejects_update(self):
        """Test execute_safe_query rejects UPDATE"""
        from src.fastmcp_sqltools.server import execute_safe_query
        
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            await execute_safe_query.fn("UPDATE users SET name = 'test'")

    @pytest.mark.asyncio
    async def test_rejects_delete(self):
        """Test execute_safe_query rejects DELETE"""
        from src.fastmcp_sqltools.server import execute_safe_query
        
        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            await execute_safe_query.fn("DELETE FROM users")

    @pytest.mark.asyncio
    async def test_rejects_drop(self):
        """Test execute_safe_query rejects DROP"""
        from src.fastmcp_sqltools.server import execute_safe_query
        
        with pytest.raises(ValueError, match="disallowed keyword: DROP"):
            await execute_safe_query.fn("SELECT * FROM users; DROP TABLE users")
