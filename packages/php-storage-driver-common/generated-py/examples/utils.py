"""
Utility functions for the gRPC server example.

This module provides helper functions for logging, protobuf message handling,
and common operations.
"""

import logging
from typing import Optional, List
from google.protobuf.message import Message
from google.protobuf import any_pb2

# Add generated proto modules to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from proto import common_pb2


# Set up module logger
logger = logging.getLogger(__name__)


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure logging for the application.

    Args:
        level: Logging level (e.g., logging.INFO, logging.DEBUG)
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def create_log_message(
    level: common_pb2.LogMessage.Level,
    message: str,
    context: Optional[Message] = None
) -> common_pb2.LogMessage:
    """
    Create a LogMessage for inclusion in DriverResponse.

    Args:
        level: Log level (Emergency, Alert, Critical, Error, Warning, Notice, Informational, Debug)
        message: Log message text
        context: Optional context as a protobuf Message

    Returns:
        LogMessage instance

    Example:
        >>> log = create_log_message(
        ...     common_pb2.LogMessage.Level.Informational,
        ...     "Table created successfully"
        ... )
    """
    log_msg = common_pb2.LogMessage()
    log_msg.level = level
    log_msg.message = message

    if context:
        log_msg.context.Pack(context)

    return log_msg


def pack_message(message: Message) -> any_pb2.Any:
    """
    Pack a protobuf message into google.protobuf.Any.

    Args:
        message: Protobuf message to pack

    Returns:
        Any containing the packed message

    Example:
        >>> from proto import table_pb2
        >>> cmd = table_pb2.CreateTableCommand()
        >>> cmd.tableName = "users"
        >>> any_msg = pack_message(cmd)
    """
    any_msg = any_pb2.Any()
    any_msg.Pack(message)
    return any_msg


def unpack_message(any_msg: any_pb2.Any, target_class) -> Optional[Message]:
    """
    Unpack google.protobuf.Any into a specific message type.

    Args:
        any_msg: Any message to unpack
        target_class: Target protobuf message class

    Returns:
        Unpacked message or None if unpacking fails

    Example:
        >>> from proto import table_pb2
        >>> cmd = table_pb2.CreateTableCommand()
        >>> unpacked = unpack_message(any_msg, table_pb2.CreateTableCommand)
    """
    try:
        message = target_class()
        if any_msg.Unpack(message):
            return message
        return None
    except Exception as e:
        logger.error(f"Failed to unpack message: {e}")
        return None


def get_type_name(any_msg: any_pb2.Any) -> str:
    """
    Extract the type name from a google.protobuf.Any message.

    Args:
        any_msg: Any message

    Returns:
        Type name (e.g., "CreateTableCommand")

    Example:
        >>> type_name = get_type_name(any_msg)
        >>> print(type_name)
        CreateTableCommand
    """
    # type_url format: "type.googleapis.com/package.MessageType"
    # We extract the last part after the last '/'
    return any_msg.type_url.split('/')[-1]


def format_table_path(path: List[str], table_name: str) -> str:
    """
    Format a table path for display.

    Args:
        path: List of path components (e.g., ['project', 'bucket'])
        table_name: Table name

    Returns:
        Formatted path string

    Example:
        >>> format_table_path(['my-project', 'my-bucket'], 'users')
        'my-project.my-bucket.users'
    """
    return '.'.join(list(path) + [table_name])


def validate_table_path(path: List[str], table_name: str) -> tuple[bool, Optional[str]]:
    """
    Validate a table path.

    Args:
        path: List of path components
        table_name: Table name

    Returns:
        Tuple of (is_valid, error_message)

    Example:
        >>> valid, error = validate_table_path(['project'], 'users')
        >>> if not valid:
        ...     print(error)
    """
    if not path:
        return False, "Path cannot be empty"

    if not table_name or not table_name.strip():
        return False, "Table name cannot be empty"

    # Check for valid characters (alphanumeric, underscore, hyphen)
    for component in path:
        if not component or not component.strip():
            return False, "Path components cannot be empty"

    if not table_name.replace('_', '').replace('-', '').isalnum():
        return False, f"Invalid table name: {table_name}"

    return True, None


class LogMessageCollector:
    """
    Collector for LogMessage instances during command handling.

    This class provides a convenient way to collect log messages
    that will be included in the DriverResponse.

    Example:
        >>> collector = LogMessageCollector()
        >>> collector.info("Table created")
        >>> collector.warning("Table already exists, skipping")
        >>> messages = collector.get_messages()
    """

    def __init__(self):
        self._messages: List[common_pb2.LogMessage] = []

    def add(self, level: common_pb2.LogMessage.Level, message: str,
            context: Optional[Message] = None) -> None:
        """Add a log message."""
        self._messages.append(create_log_message(level, message, context))

    def emergency(self, message: str, context: Optional[Message] = None) -> None:
        """Add an emergency level message."""
        self.add(common_pb2.LogMessage.Level.Emergency, message, context)

    def alert(self, message: str, context: Optional[Message] = None) -> None:
        """Add an alert level message."""
        self.add(common_pb2.LogMessage.Level.Alert, message, context)

    def critical(self, message: str, context: Optional[Message] = None) -> None:
        """Add a critical level message."""
        self.add(common_pb2.LogMessage.Level.Critical, message, context)

    def error(self, message: str, context: Optional[Message] = None) -> None:
        """Add an error level message."""
        self.add(common_pb2.LogMessage.Level.Error, message, context)

    def warning(self, message: str, context: Optional[Message] = None) -> None:
        """Add a warning level message."""
        self.add(common_pb2.LogMessage.Level.Warning, message, context)

    def notice(self, message: str, context: Optional[Message] = None) -> None:
        """Add a notice level message."""
        self.add(common_pb2.LogMessage.Level.Notice, message, context)

    def info(self, message: str, context: Optional[Message] = None) -> None:
        """Add an informational level message."""
        self.add(common_pb2.LogMessage.Level.Informational, message, context)

    def debug(self, message: str, context: Optional[Message] = None) -> None:
        """Add a debug level message."""
        self.add(common_pb2.LogMessage.Level.Debug, message, context)

    def get_messages(self) -> List[common_pb2.LogMessage]:
        """Get all collected log messages."""
        return self._messages

    def clear(self) -> None:
        """Clear all collected messages."""
        self._messages.clear()

    def __len__(self) -> int:
        """Return the number of collected messages."""
        return len(self._messages)
