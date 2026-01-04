"""
gRPC Client for StorageDriverService.

This module provides a convenient client wrapper for interacting with
the StorageDriverService gRPC server.

Usage:
    from examples.client.grpc_client import StorageDriverClient

    client = StorageDriverClient("localhost:50051")
    response = client.create_table(...)
    client.close()

Or use as context manager:
    with StorageDriverClient("localhost:50051") as client:
        response = client.create_table(...)
"""

import logging
from typing import List, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import grpc
from proto import service_pb2_grpc, common_pb2, table_pb2, executeQuery_pb2, info_pb2


logger = logging.getLogger(__name__)


class StorageDriverClient:
    """
    Client for StorageDriverService gRPC API.

    This class provides convenient methods for calling the StorageDriverService
    while handling the protobuf wrapping/unwrapping patterns.

    Example:
        >>> client = StorageDriverClient("localhost:50051")
        >>> client.create_table(['project'], 'users', [...])
        >>> client.close()
    """

    def __init__(self, address: str, timeout: int = 30):
        """
        Initialize the client.

        Args:
            address: Server address (e.g., "localhost:50051")
            timeout: Default timeout for requests in seconds
        """
        self.address = address
        self.timeout = timeout
        self.channel = grpc.insecure_channel(address)
        self.stub = service_pb2_grpc.StorageDriverServiceStub(self.channel)
        logger.info(f"Connected to StorageDriverService at {address}")

    def close(self):
        """Close the gRPC channel."""
        if self.channel:
            self.channel.close()
            logger.info("Channel closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def _execute(
        self,
        command,
        run_id: Optional[str] = None,
        query_tags: Optional[dict] = None
    ) -> common_pb2.DriverResponse:
        """
        Execute a command.

        Args:
            command: Command message
            run_id: Optional run ID
            query_tags: Optional query tags

        Returns:
            DriverResponse

        Raises:
            grpc.RpcError: On RPC errors
        """
        # Create DriverRequest
        request = common_pb2.DriverRequest()
        request.command.Pack(command)

        # Set runtime options
        if run_id:
            request.runtimeOptions.runId = run_id
        if query_tags:
            for key, value in query_tags.items():
                request.runtimeOptions.queryTags[key] = value

        # Execute request
        try:
            response = self.stub.Execute(request, timeout=self.timeout)

            # Log any messages from server
            for msg in response.messages:
                level = msg.level
                if level <= common_pb2.LogMessage.Level.Error:
                    logger.error(f"Server: {msg.message}")
                elif level == common_pb2.LogMessage.Level.Warning:
                    logger.warning(f"Server: {msg.message}")
                else:
                    logger.info(f"Server: {msg.message}")

            return response

        except grpc.RpcError as e:
            logger.error(f"RPC failed: {e.code()} - {e.details()}")
            raise

    def create_table(
        self,
        path: List[str],
        table_name: str,
        columns: List[dict],
        primary_keys: Optional[List[str]] = None,
        run_id: Optional[str] = None
    ) -> None:
        """
        Create a table.

        Args:
            path: Table path components (e.g., ['project', 'bucket'])
            table_name: Table name
            columns: List of column definitions, each with keys: 'name', 'type', 'nullable'
            primary_keys: Optional list of primary key column names
            run_id: Optional run ID

        Example:
            >>> client.create_table(
            ...     path=['my-project', 'my-bucket'],
            ...     table_name='users',
            ...     columns=[
            ...         {'name': 'user_id', 'type': 'INTEGER', 'nullable': False},
            ...         {'name': 'email', 'type': 'STRING', 'nullable': False},
            ...     ],
            ...     primary_keys=['user_id']
            ... )
        """
        cmd = table_pb2.CreateTableCommand()
        cmd.path.extend(path)
        cmd.tableName = table_name

        # Add columns
        for col_def in columns:
            col = cmd.columns.add()
            col.name = col_def['name']
            col.type = col_def['type']
            col.nullable = col_def.get('nullable', True)
            if 'length' in col_def:
                col.length = col_def['length']
            if 'default' in col_def:
                col.default = col_def['default']

        # Add primary keys
        if primary_keys:
            cmd.primaryKeysNames.extend(primary_keys)

        self._execute(cmd, run_id=run_id)
        logger.info(f"Table {'.'.join(path + [table_name])} created")

    def preview_table(
        self,
        path: List[str],
        table_name: str,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        run_id: Optional[str] = None
    ) -> table_pb2.PreviewTableResponse:
        """
        Preview table data.

        Args:
            path: Table path components
            table_name: Table name
            columns: Optional list of columns to return
            limit: Optional limit on number of rows
            run_id: Optional run ID

        Returns:
            PreviewTableResponse with table data

        Example:
            >>> response = client.preview_table(
            ...     path=['my-project', 'my-bucket'],
            ...     table_name='users',
            ...     limit=10
            ... )
            >>> print(f"Columns: {response.columns}")
            >>> for row in response.rows:
            ...     print(row)
        """
        cmd = table_pb2.PreviewTableCommand()
        cmd.path.extend(path)
        cmd.tableName = table_name

        if columns:
            cmd.columns.extend(columns)

        if limit:
            cmd.filters.limit = limit

        response = self._execute(cmd, run_id=run_id)

        # Unpack response
        preview_response = table_pb2.PreviewTableResponse()
        response.commandResponse.Unpack(preview_response)

        logger.info(f"Preview returned {len(preview_response.rows)} rows")
        return preview_response

    def import_from_file(
        self,
        path: List[str],
        table_name: str,
        file_provider: str = "S3",
        file_path: dict = None,
        run_id: Optional[str] = None
    ) -> table_pb2.TableImportResponse:
        """
        Import data from a file into a table.

        Args:
            path: Destination table path components
            table_name: Destination table name
            file_provider: File provider ("S3", "GCS", "ABS")
            file_path: File path dict with keys: 'root', 'path', 'fileName'
            run_id: Optional run ID

        Returns:
            TableImportResponse with import statistics

        Example:
            >>> response = client.import_from_file(
            ...     path=['my-project', 'my-bucket'],
            ...     table_name='users',
            ...     file_provider="S3",
            ...     file_path={'root': 'my-bucket', 'path': 'data', 'fileName': 'users.csv'}
            ... )
            >>> print(f"Imported {response.importedRowsCount} rows")
        """
        cmd = table_pb2.TableImportFromFileCommand()

        # Set destination
        cmd.destination.path.extend(path)
        cmd.destination.tableName = table_name

        # Set file provider
        if file_provider == "S3":
            cmd.fileProvider = table_pb2.ImportExportShared.FileProvider.S3
        elif file_provider == "GCS":
            cmd.fileProvider = table_pb2.ImportExportShared.FileProvider.GCS
        elif file_provider == "ABS":
            cmd.fileProvider = table_pb2.ImportExportShared.FileProvider.ABS

        # Set file format (CSV for now)
        cmd.fileFormat = table_pb2.ImportExportShared.FileFormat.CSV

        # Set file path
        if file_path:
            cmd.filePath.root = file_path.get('root', '')
            cmd.filePath.path = file_path.get('path', '')
            cmd.filePath.fileName = file_path.get('fileName', '')

        response = self._execute(cmd, run_id=run_id)

        # Unpack response
        import_response = table_pb2.TableImportResponse()
        response.commandResponse.Unpack(import_response)

        logger.info(f"Import completed: {import_response.importedRowsCount} rows")
        return import_response

    def execute_query(
        self,
        sql: str,
        run_id: Optional[str] = None
    ) -> executeQuery_pb2.ExecuteQueryResponse:
        """
        Execute a SQL query.

        Args:
            sql: SQL query string
            run_id: Optional run ID

        Returns:
            ExecuteQueryResponse with query results

        Example:
            >>> response = client.execute_query("SELECT * FROM users LIMIT 10")
        """
        cmd = executeQuery_pb2.ExecuteQueryCommand()
        cmd.sql = sql

        response = self._execute(cmd, run_id=run_id)

        # Unpack response
        query_response = executeQuery_pb2.ExecuteQueryResponse()
        response.commandResponse.Unpack(query_response)

        logger.info("Query executed")
        return query_response

    def get_object_info(
        self,
        path: List[str],
        object_name: str,
        run_id: Optional[str] = None
    ) -> info_pb2.ObjectInfoResponse:
        """
        Get object metadata.

        Args:
            path: Object path components
            object_name: Object name
            run_id: Optional run ID

        Returns:
            ObjectInfoResponse with object metadata

        Example:
            >>> response = client.get_object_info(['my-project', 'my-bucket'], 'users')
            >>> table_info = response.tableInfo[0]
            >>> print(f"Table: {table_info.tableName}")
            >>> print(f"Columns: {len(table_info.columns)}")
        """
        cmd = info_pb2.ObjectInfoCommand()
        cmd.path.extend(path)
        cmd.objectName = object_name

        response = self._execute(cmd, run_id=run_id)

        # Unpack response
        info_response = info_pb2.ObjectInfoResponse()
        response.commandResponse.Unpack(info_response)

        logger.info(f"Retrieved info for {object_name}")
        return info_response
