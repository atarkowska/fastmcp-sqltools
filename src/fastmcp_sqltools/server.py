"""
FastMCP SQL Server with support for PostgreSQL, MySQL, and SQLite
Provides tools for database operations: list_tables, get_table_schema,
execute_query, execute_safe_query
"""

import logging
import os
from typing import Any, Protocol
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from fastmcp import FastMCP


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Initialize FastMCP server
mcp = FastMCP("SQL Server")


class DatabaseConnection(Protocol):
    """Protocol for database connection"""
    async def fetch(self, query: str, *args) -> list[Any]:
        """Fetch query results"""

    async def execute(self, query: str, *args) -> Any:
        """Execute a query"""


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters"""

    @abstractmethod
    async def get_connection(self) -> Any:
        """Get a database connection"""

    @abstractmethod
    async def fetch(
        self, query: str, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        """Fetch query results"""

    @abstractmethod
    async def execute(self, query: str, params: list[Any] | None = None) -> Any:
        """Execute a query"""

    @abstractmethod
    async def close(self) -> None:
        """Close the database connection"""

    @abstractmethod
    def get_list_tables_query(
        self, schema: str | None = None
    ) -> tuple[str, list[Any]]:
        """Get query to list tables"""

    @abstractmethod
    def get_table_schema_query(
        self, table_name: str, schema: str | None = None
    ) -> tuple[str, list[Any]]:
        """Get query to retrieve table schema"""


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL database adapter using asyncpg"""

    def __init__(self):
        import asyncpg  # pylint: disable=import-outside-toplevel
        self.asyncpg = asyncpg
        self.pool: asyncpg.Pool | None = None

    async def get_connection(self) -> Any:
        """Get or create PostgreSQL connection pool"""
        if self.pool is None:
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                raise ValueError("DATABASE_URL environment variable is required")
            self.pool = await self.asyncpg.create_pool(database_url)
            logger.info("PostgreSQL connection pool created successfully")
        return self.pool

    async def fetch(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        """Fetch query results from PostgreSQL"""
        pool = await self.get_connection()
        async with pool.acquire() as conn:
            if params:
                rows = await conn.fetch(query, *params)
            else:
                rows = await conn.fetch(query)
            return [dict(row) for row in rows]

    async def execute(self, query: str, params: list[Any] | None = None) -> Any:
        """Execute a query on PostgreSQL"""
        pool = await self.get_connection()
        async with pool.acquire() as conn:
            if params:
                return await conn.execute(query, *params)
            return await conn.execute(query)

    async def close(self) -> None:
        """Close PostgreSQL connection pool"""
        if self.pool:
            await self.pool.close()
            self.pool = None

    def get_list_tables_query(self, schema: str | None = None) -> tuple[str, list[Any]]:
        """Get query to list tables in PostgreSQL"""
        schema = schema or "public"
        query = """
            SELECT
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = $1
            ORDER BY table_name
        """
        return query, [schema]

    def get_table_schema_query(
        self, table_name: str, schema: str | None = None
    ) -> tuple[str, list[Any]]:
        """Get query to retrieve table schema in PostgreSQL"""
        schema = schema or "public"
        query = """
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        """
        return query, [schema, table_name]


class MySQLAdapter(DatabaseAdapter):
    """MySQL database adapter using aiomysql"""

    def __init__(self):
        import aiomysql  # pylint: disable=import-outside-toplevel
        self.aiomysql = aiomysql
        self.pool: aiomysql.Pool | None = None

    async def get_connection(self) -> Any:
        """Get or create MySQL connection pool"""
        if self.pool is None:
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                raise ValueError("DATABASE_URL environment variable is required")

            # Parse MySQL URL (mysql://user:password@host:port/database)
            parsed = urlparse(database_url)

            self.pool = await self.aiomysql.create_pool(
                host=parsed.hostname or 'localhost',
                port=parsed.port or 3306,
                user=parsed.username,
                password=parsed.password,
                db=parsed.path.lstrip('/') if parsed.path else None,
                autocommit=True
            )
            logger.info("MySQL connection pool created successfully")
        return self.pool

    async def fetch(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        """Fetch query results from MySQL"""
        pool = await self.get_connection()
        async with pool.acquire() as conn:
            async with conn.cursor(self.aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params or ())
                rows = await cursor.fetchall()
                return list(rows)

    async def execute(self, query: str, params: list[Any] | None = None) -> Any:
        """Execute a query on MySQL"""
        pool = await self.get_connection()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                result = await cursor.execute(query, params or ())
                await conn.commit()
                return result

    async def close(self) -> None:
        """Close MySQL connection pool"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    def get_list_tables_query(self, schema: str | None = None) -> tuple[str, list[Any]]:
        """Get query to list tables in MySQL"""
        if schema:
            query = """
                SELECT
                    table_name,
                    table_type
                FROM information_schema.tables
                WHERE table_schema = %s
                ORDER BY table_name
            """
            return query, [schema]
        query = """
            SELECT
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            ORDER BY table_name
        """
        return query, []

    def get_table_schema_query(
        self, table_name: str, schema: str | None = None
    ) -> tuple[str, list[Any]]:
        """Get query to retrieve table schema in MySQL"""
        if schema:
            query = """
                SELECT
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            return query, [schema, table_name]
        query = """
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = %s
            ORDER BY ordinal_position
        """
        return query, [table_name]


class SQLiteAdapter(DatabaseAdapter):
    """SQLite database adapter using aiosqlite"""

    def __init__(self):
        import aiosqlite  # pylint: disable=import-outside-toplevel
        self.aiosqlite = aiosqlite
        self.connection: aiosqlite.Connection | None = None

    async def get_connection(self) -> Any:
        """Get or create SQLite connection"""
        if self.connection is None:
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                raise ValueError("DATABASE_URL environment variable is required")

            # Parse SQLite URL (sqlite:///path/to/db.sqlite or sqlite://path/to/db.sqlite)
            db_path = database_url.replace("sqlite:///", "").replace("sqlite://", "")
            self.connection = await self.aiosqlite.connect(db_path)
            self.connection.row_factory = self.aiosqlite.Row
            logger.info("SQLite connection created successfully")
        return self.connection

    async def fetch(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        """Fetch query results from SQLite"""
        conn = await self.get_connection()
        async with conn.execute(query, params or ()) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def execute(self, query: str, params: list[Any] | None = None) -> Any:
        """Execute a query on SQLite"""
        conn = await self.get_connection()
        cursor = await conn.execute(query, params or ())
        await conn.commit()
        return cursor

    async def close(self) -> None:
        """Close SQLite connection"""
        if self.connection:
            await self.connection.close()
            self.connection = None

    def get_list_tables_query(self, schema: str | None = None) -> tuple[str, list[Any]]:
        """Get query to list tables in SQLite"""
        query = """
            SELECT
                name as table_name,
                type as table_type
            FROM sqlite_master
            WHERE type IN ('table', 'view')
            ORDER BY name
        """
        return query, []

    def get_table_schema_query(
        self, table_name: str, schema: str | None = None
    ) -> tuple[str, list[Any]]:
        """Get query to retrieve table schema in SQLite"""
        # SQLite uses PRAGMA table_info, but we'll convert it to a similar format
        query = f"PRAGMA table_info({table_name})"
        return query, []


class DatabaseManager:
    """Manages database adapter lifecycle"""

    def __init__(self):
        self._adapter:  DatabaseAdapter | None = None

    def get_adapter(self) -> DatabaseAdapter:
        """Get or create database adapter based on DATABASE_URL"""
        if self._adapter is None:
            database_url = os.getenv("DATABASE_URL", "")

            if (
                database_url.startswith("postgresql://")
                or database_url.startswith("postgres://")
            ):
                logger.info("Initializing PostgreSQL adapter")
                self._adapter = PostgresAdapter()
            elif database_url.startswith("mysql://"):
                logger.info("Initializing MySQL adapter")
                self._adapter = MySQLAdapter()
            elif database_url.startswith("sqlite://"):
                logger.info("Initializing SQLite adapter")
                self._adapter = SQLiteAdapter()
            else:
                raise ValueError(
                    "Unsupported database URL.  Must start with "
                    "postgresql://, mysql://, or sqlite://"
                )

        return self._adapter

    async def close(self):
        """Close database connection"""
        if self._adapter:
            await self._adapter.close()
            self._adapter = None


# Create database manager instance
db_manager = DatabaseManager()


@mcp.tool()
async def list_tables(schema: str | None = None) -> list[dict[str, Any]]:
    """
    List all tables in the specified schema.
    
    Args:
        schema: Database schema name (default: None, uses default schema for the database)
    
    Returns:
        List of tables with their details
    """
    logger.info("Listing tables in schema: %s", schema or "default")
    try:
        db = db_manager.get_adapter()
        query, params = db.get_list_tables_query(schema)
        result = await db.fetch(query, params)

        # For SQLite, normalize the column names
        if isinstance(db, SQLiteAdapter):
            result = [
                {
                    "table_name": row.get("table_name"),
                    "table_type": row.get("table_type")
                }
                for row in result
            ]

        logger.info("Found %d tables", len(result))
        return result
    except Exception as e:
        logger.error("Error listing tables: %s", e)
        raise


@mcp.tool()
async def get_table_schema(table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
    """
    Get the schema (column definitions) for a specific table.
    
    Args:
        table_name: Name of the table
        schema: Database schema name (default: None, uses default schema for the database)
    
    Returns:
        List of columns with their data types and constraints
    """
    logger.info("Getting schema for table: %s", table_name)
    try:
        db = db_manager.get_adapter()
        query, params = db.get_table_schema_query(table_name, schema)
        result = await db.fetch(query, params)

        # For SQLite, convert PRAGMA table_info output to standard format
        if isinstance(db, SQLiteAdapter):
            result = [
                {
                    "column_name": row.get("name"),
                    "data_type": row.get("type"),
                    "character_maximum_length": None,
                    "is_nullable": "YES" if not row.get("notnull") else "NO",
                    "column_default": row.get("dflt_value"),
                }
                for row in result
            ]

        logger.info("Found %d columns for table '%s'", len(result), table_name)
        return result
    except Exception as e:
        logger.error("Error getting schema for table '%s': %s", table_name, e)
        raise


@mcp.tool()
async def execute_query(query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    """
    Execute a SQL query and return results.
    WARNING: This can execute any SQL including DDL and DML statements.
    
    Args:
        query: SQL query to execute
        params: Optional list of query parameters for parameterized queries
    
    Returns:
        Query results as list of dictionaries
    """
    logger.info("Executing query: %s", query[:100] + ("..." if len(query) > 100 else ""))
    if params:
        logger.debug("Query parameters: %s", params)
    try:
        db = db_manager.get_adapter()
        result = await db.fetch(query, params)
        logger.info("Query returned %d rows", len(result))
        return result
    except Exception as e:
        logger.error("Error executing query: %s", e)
        raise


@mcp.tool()
async def execute_safe_query(query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    """
    Execute a read-only SQL query (SELECT only).
    This is safer than execute_query as it prevents data modification.
    
    Args:
        query: SQL SELECT query to execute
        params: Optional list of query parameters for parameterized queries
    
    Returns:
        Query results as list of dictionaries
    """
    logger.info("Executing safe query: %s", query[:100] + ("..." if len(query) > 100 else ""))
    if params:
        logger.debug("Query parameters: %s", params)

    # Basic validation to ensure only SELECT queries
    query_stripped = query.strip().upper()
    if not query_stripped.startswith("SELECT"):
        logger.warning("Rejected non-SELECT query in execute_safe_query")
        raise ValueError("Only SELECT queries are allowed in execute_safe_query")

    # Check for disallowed keywords that could modify data
    disallowed_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]
    for keyword in disallowed_keywords:
        if keyword in query_stripped:
            logger.warning("Rejected query with disallowed keyword '%s'", keyword)
            raise ValueError(f"Query contains disallowed keyword: {keyword}")

    try:
        db = db_manager.get_adapter()
        result = await db.fetch(query, params)
        logger.info("Safe query returned %d rows", len(result))
        return result
    except Exception as e:
        logger.error("Error executing safe query: %s", e)
        raise


def main():
    """
    Start the MCP server.
    """
    logger.info("Starting SQL MCP Server...")
    logger.info("Available tools:")
    logger.info(" - list_tables: List all tables in the specified schema")
    logger.info(" - get_table_schema: Get the schema (column definitions) for a specific table")
    logger.info(" - execute_query: Execute a SQL query and return results")
    logger.info(" - execute_safe_query: Execute a read-only SQL query (SELECT only)")
    mcp.run()


if __name__ == "__main__":
    # Run the server
    main()
