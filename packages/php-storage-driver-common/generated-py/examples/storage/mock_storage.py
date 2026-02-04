"""
Mock in-memory storage backend for demonstration purposes.

This module provides a simple in-memory storage implementation that mimics
a real database backend. It's used for educational purposes to demonstrate
how handlers interact with storage without requiring actual database connections.

TODO: In production, replace this with real backend implementations:
- BigQuery client for GCP
- Snowflake connector
- Redshift client for AWS
- Teradata connector
- Azure Synapse client
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import copy

# Add generated proto modules to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from proto import table_pb2, info_pb2


logger = logging.getLogger(__name__)


@dataclass
class ColumnSchema:
    """Schema definition for a table column."""
    name: str
    type: str
    length: str = ""
    nullable: bool = True
    default: Optional[str] = None


@dataclass
class TableSchema:
    """Schema definition for a table."""
    name: str
    path: List[str]
    columns: List[ColumnSchema] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    row_count: int = 0
    size_bytes: int = 0


@dataclass
class TableData:
    """Data storage for a table."""
    schema: TableSchema
    rows: List[Dict[str, Any]] = field(default_factory=list)


class MockStorageBackend:
    """
    In-memory storage backend for demonstration purposes.

    This class simulates a database backend with tables, schemas, and data.
    It's intentionally simple to focus on the gRPC/protobuf patterns rather
    than database complexity.

    Attributes:
        tables: Dictionary mapping table paths to TableData

    Example:
        >>> storage = MockStorageBackend()
        >>> storage.create_table(['project'], 'users', columns)
        >>> storage.insert_rows(['project'], 'users', rows)
        >>> data = storage.query_table(['project'], 'users', limit=10)
    """

    def __init__(self):
        """Initialize the mock storage backend."""
        self.tables: Dict[str, TableData] = {}
        logger.info("Mock storage backend initialized")

    def _get_table_key(self, path: List[str], table_name: str) -> str:
        """Generate a unique key for a table."""
        return '.'.join(list(path) + [table_name])

    def table_exists(self, path: List[str], table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            path: Table path components
            table_name: Table name

        Returns:
            True if table exists, False otherwise
        """
        key = self._get_table_key(path, table_name)
        return key in self.tables

    def create_table(
        self,
        path: List[str],
        table_name: str,
        columns: List[ColumnSchema],
        primary_keys: Optional[List[str]] = None
    ) -> None:
        """
        Create a new table.

        Args:
            path: Table path components (e.g., ['project', 'bucket'])
            table_name: Name of the table
            columns: List of column schemas
            primary_keys: Optional list of primary key column names

        Raises:
            ValueError: If table already exists or invalid parameters
        """
        key = self._get_table_key(path, table_name)

        if self.table_exists(path, table_name):
            raise ValueError(f"Table {key} already exists")

        if not columns:
            raise ValueError("Table must have at least one column")

        # Validate primary keys
        if primary_keys:
            column_names = {col.name for col in columns}
            for pk in primary_keys:
                if pk not in column_names:
                    raise ValueError(f"Primary key column '{pk}' not found in table columns")

        schema = TableSchema(
            name=table_name,
            path=path,
            columns=columns,
            primary_keys=primary_keys or []
        )

        self.tables[key] = TableData(schema=schema, rows=[])
        logger.info(f"Created table {key} with {len(columns)} columns")

    def drop_table(self, path: List[str], table_name: str) -> None:
        """
        Drop a table.

        Args:
            path: Table path components
            table_name: Table name

        Raises:
            KeyError: If table doesn't exist
        """
        key = self._get_table_key(path, table_name)

        if not self.table_exists(path, table_name):
            raise KeyError(f"Table {key} does not exist")

        del self.tables[key]
        logger.info(f"Dropped table {key}")

    def get_table_schema(self, path: List[str], table_name: str) -> TableSchema:
        """
        Get table schema.

        Args:
            path: Table path components
            table_name: Table name

        Returns:
            TableSchema

        Raises:
            KeyError: If table doesn't exist
        """
        key = self._get_table_key(path, table_name)

        if not self.table_exists(path, table_name):
            raise KeyError(f"Table {key} does not exist")

        return self.tables[key].schema

    def insert_rows(
        self,
        path: List[str],
        table_name: str,
        rows: List[Dict[str, Any]]
    ) -> int:
        """
        Insert rows into a table.

        Args:
            path: Table path components
            table_name: Table name
            rows: List of row dictionaries

        Returns:
            Number of rows inserted

        Raises:
            KeyError: If table doesn't exist
        """
        key = self._get_table_key(path, table_name)

        if not self.table_exists(path, table_name):
            raise KeyError(f"Table {key} does not exist")

        table_data = self.tables[key]

        # Validate row columns match table schema
        column_names = {col.name for col in table_data.schema.columns}
        for i, row in enumerate(rows):
            for col in row.keys():
                if col not in column_names:
                    raise ValueError(f"Row {i}: Column '{col}' not in table schema")

        # Insert rows
        table_data.rows.extend(copy.deepcopy(rows))
        table_data.schema.row_count = len(table_data.rows)

        # Estimate size (very rough)
        table_data.schema.size_bytes = len(str(table_data.rows).encode())

        logger.info(f"Inserted {len(rows)} rows into table {key}")
        return len(rows)

    def query_table(
        self,
        path: List[str],
        table_name: str,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        where: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[Dict[str, Any]], TableSchema]:
        """
        Query table data.

        Args:
            path: Table path components
            table_name: Table name
            columns: Optional list of columns to return (default: all)
            limit: Optional limit on number of rows
            where: Optional simple filter conditions

        Returns:
            Tuple of (rows, schema)

        Raises:
            KeyError: If table doesn't exist
        """
        key = self._get_table_key(path, table_name)

        if not self.table_exists(path, table_name):
            raise KeyError(f"Table {key} does not exist")

        table_data = self.tables[key]
        result_rows = copy.deepcopy(table_data.rows)

        # Apply simple where filters
        if where:
            filtered_rows = []
            for row in result_rows:
                match = True
                for col, value in where.items():
                    if col not in row or row[col] != value:
                        match = False
                        break
                if match:
                    filtered_rows.append(row)
            result_rows = filtered_rows

        # Apply column selection
        if columns:
            result_rows = [
                {col: row.get(col) for col in columns}
                for row in result_rows
            ]

        # Apply limit
        if limit and limit > 0:
            result_rows = result_rows[:limit]

        return result_rows, table_data.schema

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL-like query (very simplified).

        This is a mock implementation that only supports very basic queries.
        In a real implementation, this would use the actual database's SQL engine.

        Args:
            sql: SQL query string

        Returns:
            List of result rows

        Note:
            This mock implementation only supports basic SELECT statements
            for demonstration purposes.
        """
        # Very simple query parser for demonstration
        sql_lower = sql.lower().strip()

        if sql_lower.startswith('select'):
            # Mock: return empty result
            logger.info(f"Executing query: {sql}")
            return []
        else:
            raise ValueError(f"Unsupported query type: {sql}")

    def get_table_info(self, path: List[str], table_name: str) -> dict:
        """
        Get table metadata.

        Args:
            path: Table path components
            table_name: Table name

        Returns:
            Dictionary with table metadata

        Raises:
            KeyError: If table doesn't exist
        """
        schema = self.get_table_schema(path, table_name)

        return {
            'name': schema.name,
            'path': '.'.join(schema.path),
            'columns': [
                {
                    'name': col.name,
                    'type': col.type,
                    'nullable': col.nullable
                }
                for col in schema.columns
            ],
            'primary_keys': schema.primary_keys,
            'row_count': schema.row_count,
            'size_bytes': schema.size_bytes,
            'created_at': schema.created_at.isoformat()
        }

    def list_tables(self, path_prefix: Optional[List[str]] = None) -> List[str]:
        """
        List all tables, optionally filtered by path prefix.

        Args:
            path_prefix: Optional path prefix to filter tables

        Returns:
            List of table keys
        """
        if path_prefix:
            prefix = '.'.join(path_prefix)
            return [
                key for key in self.tables.keys()
                if key.startswith(prefix)
            ]
        return list(self.tables.keys())

    def clear_all(self) -> None:
        """Clear all tables from storage."""
        self.tables.clear()
        logger.info("Cleared all tables from storage")
