# FastMCP SQL Tools

mcp-name: io.github.atarkowska/fastmcp-sqltools

A Model Context Protocol (MCP) server built with FastMCP that provides SQL database access with support for PostgreSQL, MySQL, and SQLite.

## Features

- **list_tables**: List all tables in the database
- **get_table_schema**: Get detailed schema information for a table including columns, types, constraints, and indexes
- **execute_query**: Execute any SQL query (INSERT, UPDATE, DELETE, DDL, etc.)
- **execute_safe_query**: Execute read-only SELECT queries with additional safety checks

## Supported Databases

- **PostgreSQL**: Full support via asyncpg
- **MySQL**: Full support via aiomysql
- **SQLite**: Full support via aiosqlite

The server automatically detects the database type from the `DATABASE_URL` environment variable.

## Configuration

[](https://github.com/atarkowska/fastmcp-sqltools/blob/main/README.md#configuration)

Add the following to your `claude_desktop_config.json`:

```json
{
    "mcpServers": {
        "sql-mcp-tools": {
            "command": "uvx",
            "args": [
                "fastmcp-sqltools"
            ],
            "env": {
                "DATABASE_URL": "<your-database-url>"
            }
        }
    }
}
```

### Database URL Format

The `DATABASE_URL` should be in one of the following formats:

- **PostgreSQL**: `postgresql://user:password@host:port/database` or `postgres://user:password@host:port/database`
- **MySQL**: `mysql://user:password@host:port/database`
- **SQLite**: `sqlite:///path/to/database.db` (use three slashes for absolute path)
```

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0).
See the LICENSE file for details.
