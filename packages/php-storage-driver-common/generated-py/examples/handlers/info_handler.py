"""
Information query handler.

This module implements the handler for ObjectInfoCommand.
"""

import logging

# Add generated proto modules to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from proto import info_pb2, common_pb2
from examples.handlers.base_handler import BaseCommandHandler
from examples.storage.mock_storage import MockStorageBackend


logger = logging.getLogger(__name__)


class ObjectInfoHandler(BaseCommandHandler):
    """
    Handler for ObjectInfoCommand.

    Returns metadata about database objects (tables, schemas, databases).

    Example command:
        ObjectInfoCommand(
            path=['project-123', 'bucket-456'],
            objectName='users'
        )
    """

    def __init__(self, storage: MockStorageBackend):
        super().__init__(storage)

    def handle(
        self,
        command: info_pb2.ObjectInfoCommand,
        credentials,
        runtime_options: common_pb2.RuntimeOptions
    ) -> info_pb2.ObjectInfoResponse:
        """
        Handle ObjectInfoCommand.

        Args:
            command: ObjectInfoCommand message
            credentials: Backend credentials (unused in mock)
            runtime_options: Runtime options

        Returns:
            ObjectInfoResponse with object metadata

        Raises:
            KeyError: If object doesn't exist
        """
        self.validate_runtime_options(runtime_options)

        path = list(command.path)
        object_name = command.objectName

        if not object_name:
            raise ValueError("Object name cannot be empty")

        self.log_info(f"Getting info for object: {'.'.join(path + [object_name])}")

        # For this mock, we assume it's a table query
        try:
            if not self.storage.table_exists(path, object_name):
                raise KeyError(f"Object not found: {object_name}")

            table_info = self.storage.get_table_info(path, object_name)

            # Build response
            response = info_pb2.ObjectInfoResponse()

            # Add table info
            table_info_msg = response.tableInfo.add()
            table_info_msg.tableName = table_info['name']
            table_info_msg.path.extend(path)

            # Add columns
            for col in table_info['columns']:
                col_msg = table_info_msg.columns.add()
                col_msg.name = col['name']
                col_msg.type = col['type']
                col_msg.nullable = col['nullable']

            # Add primary keys
            table_info_msg.primaryKeysNames.extend(table_info['primary_keys'])

            # Add row count
            table_info_msg.rowsCount = str(table_info['row_count'])

            self.log_info(f"Found table {object_name} with {len(table_info['columns'])} columns")

            return response

        except KeyError as e:
            self.handle_error(e)
            raise
        except Exception as e:
            self.handle_error(e)
            raise
