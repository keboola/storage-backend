"""
Base handler for command processing.

This module provides the abstract base class for all command handlers.
Handlers are responsible for processing specific command types and returning responses.
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional
from google.protobuf.message import Message

# Add generated proto modules to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from proto import common_pb2
from examples.utils import LogMessageCollector
from examples.storage.mock_storage import MockStorageBackend


logger = logging.getLogger(__name__)


class BaseCommandHandler(ABC):
    """
    Abstract base class for command handlers.

    This class provides common functionality for all command handlers:
    - Access to storage backend
    - Log message collection
    - Error handling patterns
    - Response wrapping

    Subclasses must implement the handle() method to process specific command types.

    Example:
        >>> class MyHandler(BaseCommandHandler):
        ...     def handle(self, command, credentials, runtime_options):
        ...         self.log_collector.info("Processing command")
        ...         # Process command
        ...         return my_response
    """

    def __init__(self, storage: MockStorageBackend):
        """
        Initialize the handler.

        Args:
            storage: Storage backend instance
        """
        self.storage = storage
        self.log_collector = LogMessageCollector()
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def handle(
        self,
        command: Message,
        credentials: Optional[Message],
        runtime_options: common_pb2.RuntimeOptions
    ) -> Message:
        """
        Handle the command and return a response.

        This method must be implemented by subclasses to handle specific command types.

        Args:
            command: The command message to process
            credentials: Optional credentials for backend access
            runtime_options: Runtime options (runId, queryTags, etc.)

        Returns:
            Response message specific to the command type

        Raises:
            ValueError: For invalid command parameters
            KeyError: For missing resources (tables, buckets)
            Exception: For other errors
        """
        pass

    def get_log_messages(self) -> list:
        """
        Get all collected log messages.

        Returns:
            List of LogMessage instances
        """
        return self.log_collector.get_messages()

    def clear_logs(self) -> None:
        """Clear all collected log messages."""
        self.log_collector.clear()

    def log_info(self, message: str) -> None:
        """
        Log an informational message.

        The message is added to both:
        1. Internal Python logger (for debugging)
        2. Log collector (for inclusion in DriverResponse)

        Args:
            message: Log message
        """
        self.logger.info(message)
        self.log_collector.info(message)

    def log_warning(self, message: str) -> None:
        """
        Log a warning message.

        Args:
            message: Log message
        """
        self.logger.warning(message)
        self.log_collector.warning(message)

    def log_error(self, message: str) -> None:
        """
        Log an error message.

        Args:
            message: Log message
        """
        self.logger.error(message)
        self.log_collector.error(message)

    def log_debug(self, message: str) -> None:
        """
        Log a debug message.

        Args:
            message: Log message
        """
        self.logger.debug(message)
        self.log_collector.debug(message)

    def validate_runtime_options(self, runtime_options: common_pb2.RuntimeOptions) -> None:
        """
        Validate runtime options.

        Args:
            runtime_options: Runtime options to validate

        Raises:
            ValueError: If validation fails
        """
        if runtime_options and runtime_options.runId:
            self.log_debug(f"Processing request with runId: {runtime_options.runId}")

            # Log query tags if present
            if runtime_options.queryTags:
                tags = dict(runtime_options.queryTags)
                self.log_debug(f"Query tags: {tags}")

    def handle_error(self, error: Exception) -> None:
        """
        Handle an error during command processing.

        This method logs the error and adds it to the log collector.
        The error will be propagated up to be handled by the gRPC server.

        Args:
            error: The exception that occurred
        """
        error_msg = f"Error processing command: {str(error)}"
        self.log_error(error_msg)
        self.logger.exception("Exception details:")

    def create_empty_response(self, response_class):
        """
        Create an empty response of the specified type.

        Args:
            response_class: The response message class

        Returns:
            Empty response instance
        """
        return response_class()


class TableCommandHandlerMixin:
    """
    Mixin for handlers that deal with table operations.

    Provides common validation and helper methods for table-related commands.
    """

    def validate_table_path(self, path: list, table_name: str) -> None:
        """
        Validate table path components.

        Args:
            path: List of path components
            table_name: Table name

        Raises:
            ValueError: If validation fails
        """
        if not path:
            raise ValueError("Table path cannot be empty")

        if not table_name or not table_name.strip():
            raise ValueError("Table name cannot be empty")

        # Basic name validation
        if not all(c.isalnum() or c in ('_', '-') for c in table_name):
            raise ValueError(f"Invalid table name: {table_name}")

    def format_table_path(self, path: list, table_name: str) -> str:
        """
        Format table path for logging/display.

        Args:
            path: List of path components
            table_name: Table name

        Returns:
            Formatted path string (e.g., "project.bucket.table")
        """
        return '.'.join(list(path) + [table_name])

    def validate_columns(self, columns: list) -> None:
        """
        Validate column definitions.

        Args:
            columns: List of TableColumnShared messages

        Raises:
            ValueError: If validation fails
        """
        if not columns:
            raise ValueError("Table must have at least one column")

        column_names = set()
        for col in columns:
            if not col.name:
                raise ValueError("Column name cannot be empty")

            if col.name in column_names:
                raise ValueError(f"Duplicate column name: {col.name}")

            column_names.add(col.name)

            if not col.type:
                raise ValueError(f"Column {col.name} must have a type")

    def validate_primary_keys(self, columns: list, primary_keys: list) -> None:
        """
        Validate primary key definitions.

        Args:
            columns: List of column definitions
            primary_keys: List of primary key column names

        Raises:
            ValueError: If validation fails
        """
        if not primary_keys:
            return

        column_names = {col.name for col in columns}
        for pk in primary_keys:
            if pk not in column_names:
                raise ValueError(f"Primary key '{pk}' not found in table columns")
