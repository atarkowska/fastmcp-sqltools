"""
FastMCP SQL Server with asyncpg
Provides tools for database operations: list_tables, get_table_schema,
execute_query, execute_safe_query
"""

import os
from typing import Any
import asyncpg
from fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("SQL Server")

# Database connection pool
_connection_pool: asyncpg.Pool | None = None  # pylint: disable=invalid-name


async def get_pool() -> asyncpg.Pool:
    """Get or create database connection pool"""
    global _connection_pool  # pylint: disable=global-statement
    if _connection_pool is None:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        _connection_pool = await asyncpg.create_pool(database_url)
    return _connection_pool


@mcp.tool()
async def list_tables(schema: str = "public") -> list[dict[str, Any]]:
    """
    List all tables in the specified schema.
    
    Args:
        schema: Database schema name (default: public)
    
    Returns:
        List of tables with their details
    """
    db_pool = await get_pool()
    async with db_pool.acquire() as conn:
        query = """
            SELECT 
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = $1
            ORDER BY table_name
        """
        rows = await conn.fetch(query, schema)
        return [dict(row) for row in rows]


@mcp.tool()
async def get_table_schema(table_name: str, schema: str = "public") -> list[dict[str, Any]]:
    """
    Get the schema (column definitions) for a specific table.
    
    Args:
        table_name: Name of the table
        schema: Database schema name (default: public)
    
    Returns:
        List of columns with their data types and constraints
    """
    db_pool = await get_pool()
    async with db_pool.acquire() as conn:
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
        rows = await conn.fetch(query, schema, table_name)
        return [dict(row) for row in rows]


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
    db_pool = await get_pool()
    async with db_pool.acquire() as conn:
        if params:
            rows = await conn.fetch(query, *params)
        else:
            rows = await conn.fetch(query)
        return [dict(row) for row in rows]


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
    # Basic validation to ensure only SELECT queries
    query_stripped = query.strip().upper()
    if not query_stripped.startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed in execute_safe_query")

    # Check for disallowed keywords that could modify data
    disallowed_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]
    for keyword in disallowed_keywords:
        if keyword in query_stripped:
            raise ValueError(f"Query contains disallowed keyword: {keyword}")

    db_pool = await get_pool()
    async with db_pool.acquire() as conn:
        # Use a read-only transaction
        async with conn.transaction(readonly=True):
            if params:
                rows = await conn.fetch(query, *params)
            else:
                rows = await conn.fetch(query)
            return [dict(row) for row in rows]


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
    mcp.run()
