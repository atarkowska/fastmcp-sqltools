# FastMCP SQL Server

A Model Context Protocol (MCP) server built with FastMCP that provides universal SQL database access with support for PostgreSQL, MySQL, SQLite, and more.

## Features

- **list_tables**: List all tables in the database
- **get_table_schema**: Get detailed schema information for a table including columns, types, constraints, and indexes
- **execute_query**: Execute any SQL query (INSERT, UPDATE, DELETE, DDL, etc.)
- **execute_safe_query**: Execute read-only SELECT queries with additional safety checks

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
            ]
        },
        "env": {
  		  "DATABASE_URL": ...
        }
    }
}
```

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0).
See the LICENSE file for details.
