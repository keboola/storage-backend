"""
Table operation handlers.

This module implements handlers for table-related commands:
- CreateTableCommand
- PreviewTableCommand
- TableImportFromFileCommand
"""

import logging
from typing import List, Dict, Any

# Add generated proto modules to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from proto import table_pb2, common_pb2
from google.protobuf import struct_pb2
from examples.handlers.base_handler import BaseCommandHandler, TableCommandHandlerMixin
from examples.storage.mock_storage import MockStorageBackend, ColumnSchema


logger = logging.getLogger(__name__)


class CreateTableHandler(BaseCommandHandler, TableCommandHandlerMixin):
    """
    Handler for CreateTableCommand.

    Creates a new table with the specified schema in the storage backend.

    Example command:
        CreateTableCommand(
            path=['project-123', 'bucket-456'],
            tableName='users',
            columns=[...],
            primaryKeysNames=['user_id']
        )
    """

    def __init__(self, storage: MockStorageBackend):
        super().__init__(storage)

    def handle(
        self,
        command: table_pb2.CreateTableCommand,
        credentials,
        runtime_options: common_pb2.RuntimeOptions
    ) -> None:
        """
        Handle CreateTableCommand.

        Args:
            command: CreateTableCommand message
            credentials: Backend credentials (unused in mock)
            runtime_options: Runtime options

        Returns:
            None (CreateTableCommand has no response type)

        Raises:
            ValueError: If table parameters are invalid
        """
        self.validate_runtime_options(runtime_options)

        # Extract table details
        path = list(command.path)
        table_name = command.tableName
        columns = command.columns
        primary_keys = list(command.primaryKeysNames)

        # Validate inputs
        self.validate_table_path(path, table_name)
        self.validate_columns(columns)
        self.validate_primary_keys(columns, primary_keys)

        table_path_str = self.format_table_path(path, table_name)

        # Check if table already exists
        if self.storage.table_exists(path, table_name):
            raise ValueError(f"Table {table_path_str} already exists")

        # Convert protobuf columns to storage schema
        column_schemas = []
        for col in columns:
            col_schema = ColumnSchema(
                name=col.name,
                type=col.type,
                length=col.length if col.length else "",
                nullable=col.nullable,
                default=col.default if col.default else None
            )
            column_schemas.append(col_schema)

        # Create table in storage
        try:
            self.storage.create_table(
                path=path,
                table_name=table_name,
                columns=column_schemas,
                primary_keys=primary_keys if primary_keys else None
            )

            self.log_info(
                f"Table {table_path_str} created successfully with "
                f"{len(columns)} columns"
            )

            if primary_keys:
                self.log_info(f"Primary keys: {', '.join(primary_keys)}")

        except Exception as e:
            self.handle_error(e)
            raise


class PreviewTableHandler(BaseCommandHandler, TableCommandHandlerMixin):
    """
    Handler for PreviewTableCommand.

    Returns a preview of table data with optional filtering and ordering.

    Example command:
        PreviewTableCommand(
            path=['project-123', 'bucket-456'],
            tableName='users',
            columns=['user_id', 'email'],
            filters=ExportFilters(limit=10)
        )
    """

    def __init__(self, storage: MockStorageBackend):
        super().__init__(storage)

    def handle(
        self,
        command: table_pb2.PreviewTableCommand,
        credentials,
        runtime_options: common_pb2.RuntimeOptions
    ) -> table_pb2.PreviewTableResponse:
        """
        Handle PreviewTableCommand.

        Args:
            command: PreviewTableCommand message
            credentials: Backend credentials (unused in mock)
            runtime_options: Runtime options

        Returns:
            PreviewTableResponse with table data

        Raises:
            KeyError: If table doesn't exist
            ValueError: If parameters are invalid
        """
        self.validate_runtime_options(runtime_options)

        # Extract query parameters
        path = list(command.path)
        table_name = command.tableName
        columns = list(command.columns) if command.columns else None
        limit = None

        # Extract limit from filters if present
        if command.HasField('filters') and command.filters.limit > 0:
            limit = command.filters.limit

        self.validate_table_path(path, table_name)
        table_path_str = self.format_table_path(path, table_name)

        # Query table data
        try:
            rows, schema = self.storage.query_table(
                path=path,
                table_name=table_name,
                columns=columns,
                limit=limit
            )

            self.log_info(
                f"Preview table {table_path_str}: "
                f"returning {len(rows)} rows"
            )

            # Build response
            response = table_pb2.PreviewTableResponse()

            # Add column names
            if columns:
                response.columns.extend(columns)
            else:
                # Use all columns from schema
                response.columns.extend([col.name for col in schema.columns])

            # Add rows
            for row_data in rows:
                row = response.rows.add()
                for col_name in response.columns:
                    col_value = row.columns.add()
                    col_value.columnName = col_name

                    # Convert value to protobuf Value
                    value = row_data.get(col_name)
                    if value is not None:
                        if isinstance(value, bool):
                            col_value.value.bool_value = value
                        elif isinstance(value, int):
                            col_value.value.number_value = float(value)
                        elif isinstance(value, float):
                            col_value.value.number_value = value
                        elif isinstance(value, str):
                            col_value.value.string_value = value
                        else:
                            col_value.value.string_value = str(value)
                    else:
                        col_value.value.null_value = struct_pb2.NULL_VALUE

                    col_value.isTruncated = False

            return response

        except KeyError as e:
            self.handle_error(e)
            raise KeyError(f"Table {table_path_str} not found")
        except Exception as e:
            self.handle_error(e)
            raise


class TableImportFromFileHandler(BaseCommandHandler, TableCommandHandlerMixin):
    """
    Handler for TableImportFromFileCommand.

    Imports data from a file (S3, GCS, ABS) into a table.
    This is a mock implementation that generates sample data instead of
    actually reading from cloud storage.

    Example command:
        TableImportFromFileCommand(
            fileProvider=FileProvider.S3,
            fileFormat=FileFormat.CSV,
            filePath=FilePath(root='bucket', path='data', fileName='users.csv'),
            destination=Table(path=['project'], tableName='users'),
            importOptions=ImportOptions(...)
        )
    """

    def __init__(self, storage: MockStorageBackend):
        super().__init__(storage)

    def handle(
        self,
        command: table_pb2.TableImportFromFileCommand,
        credentials,
        runtime_options: common_pb2.RuntimeOptions
    ) -> table_pb2.TableImportResponse:
        """
        Handle TableImportFromFileCommand.

        Args:
            command: TableImportFromFileCommand message
            credentials: Backend credentials (unused in mock)
            runtime_options: Runtime options

        Returns:
            TableImportResponse with import statistics

        Raises:
            KeyError: If destination table doesn't exist
            ValueError: If parameters are invalid
        """
        self.validate_runtime_options(runtime_options)

        # Extract destination table info
        dest = command.destination
        path = list(dest.path)
        table_name = dest.tableName

        self.validate_table_path(path, table_name)
        table_path_str = self.format_table_path(path, table_name)

        # Get file info for logging
        file_info = f"{command.filePath.root}/{command.filePath.path}/{command.filePath.fileName}"

        self.log_info(
            f"Importing data from {file_info} into table {table_path_str}"
        )

        # TODO: In a real implementation, this would:
        # 1. Read credentials from command.fileCredentials
        # 2. Connect to cloud storage (S3/GCS/ABS)
        # 3. Download and parse the file
        # 4. Insert data into the backend database

        # For this mock, we'll generate some sample data
        try:
            # Get table schema
            schema = self.storage.get_table_schema(path, table_name)

            # Generate mock rows
            mock_rows = self._generate_mock_data(schema.columns, num_rows=100)

            # Insert rows
            imported_count = self.storage.insert_rows(path, table_name, mock_rows)

            # Get updated table stats
            updated_schema = self.storage.get_table_schema(path, table_name)

            self.log_info(
                f"Imported {imported_count} rows into {table_path_str}. "
                f"Total rows: {updated_schema.row_count}"
            )

            # Build response
            response = table_pb2.TableImportResponse()
            response.importedRowsCount = imported_count
            response.tableRowsCount = updated_schema.row_count
            response.tableSizeBytes = updated_schema.size_bytes

            # Add column names
            response.importedColumns.extend([col.name for col in schema.columns])

            # Add timer for demonstration
            timer = response.timers.add()
            timer.name = "import"
            timer.duration = "1.5s"

            return response

        except KeyError:
            error_msg = f"Destination table {table_path_str} not found"
            self.log_error(error_msg)
            raise KeyError(error_msg)
        except Exception as e:
            self.handle_error(e)
            raise

    def _generate_mock_data(
        self,
        columns: List[ColumnSchema],
        num_rows: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Generate mock data for demonstration purposes.

        In a real implementation, this would read actual data from files.

        Args:
            columns: Table column schemas
            num_rows: Number of rows to generate

        Returns:
            List of row dictionaries
        """
        rows = []
        for i in range(num_rows):
            row = {}
            for col in columns:
                # Generate simple mock data based on type
                if col.type.upper() in ('INTEGER', 'INT', 'BIGINT'):
                    row[col.name] = i
                elif col.type.upper() in ('STRING', 'VARCHAR', 'TEXT'):
                    row[col.name] = f"{col.name}_{i}"
                elif col.type.upper() in ('FLOAT', 'DOUBLE', 'DECIMAL'):
                    row[col.name] = i * 1.5
                elif col.type.upper() == 'BOOLEAN':
                    row[col.name] = i % 2 == 0
                else:
                    row[col.name] = f"value_{i}"
            rows.append(row)

        return rows
