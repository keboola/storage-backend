"""
Query execution handler.

This module implements the handler for ExecuteQueryCommand.
"""

import logging

# Add generated proto modules to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from proto import executeQuery_pb2, common_pb2
from examples.handlers.base_handler import BaseCommandHandler
from examples.storage.mock_storage import MockStorageBackend


logger = logging.getLogger(__name__)


class ExecuteQueryHandler(BaseCommandHandler):
    """
    Handler for ExecuteQueryCommand.

    Executes ad-hoc SQL queries against the storage backend.
    This mock implementation demonstrates the pattern but doesn't execute real SQL.

    Example command:
        ExecuteQueryCommand(
            sql="SELECT * FROM users WHERE age > 18"
        )
    """

    def __init__(self, storage: MockStorageBackend):
        super().__init__(storage)

    def handle(
        self,
        command: executeQuery_pb2.ExecuteQueryCommand,
        credentials,
        runtime_options: common_pb2.RuntimeOptions
    ) -> executeQuery_pb2.ExecuteQueryResponse:
        """
        Handle ExecuteQueryCommand.

        Args:
            command: ExecuteQueryCommand message
            credentials: Backend credentials (unused in mock)
            runtime_options: Runtime options

        Returns:
            ExecuteQueryResponse with query results

        Raises:
            ValueError: If query is invalid
        """
        self.validate_runtime_options(runtime_options)

        sql = command.sql.strip()

        if not sql:
            raise ValueError("SQL query cannot be empty")

        self.log_info(f"Executing query: {sql[:100]}{'...' if len(sql) > 100 else ''}")

        # TODO: In a real implementation, this would:
        # 1. Parse and validate the SQL query
        # 2. Execute it against the backend database
        # 3. Return actual results

        # For this mock, we'll return an empty result set
        try:
            results = self.storage.execute_query(sql)

            self.log_info(f"Query executed successfully, returned {len(results)} rows")

            # Build response
            response = executeQuery_pb2.ExecuteQueryResponse()

            # Mock: set empty columns and rows
            # In a real implementation, these would come from actual query results

            return response

        except Exception as e:
            self.handle_error(e)
            raise
