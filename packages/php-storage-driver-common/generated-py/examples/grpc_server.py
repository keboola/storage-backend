"""
gRPC Server implementation for StorageDriverService.

This module implements the gRPC server that handles Storage Driver commands.
It routes incoming requests to appropriate handlers based on command type.

Usage:
    python3 grpc_server.py

Or:
    from examples.grpc_server import serve
    serve(port=50051)
"""

import logging
import signal
import sys
from concurrent import futures
from pathlib import Path

# Add generated proto modules to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import grpc
from proto import service_pb2_grpc, common_pb2, table_pb2, executeQuery_pb2, info_pb2
from google.protobuf.message import Message

from examples.config import get_config
from examples.utils import setup_logging, get_type_name, pack_message
from examples.storage.mock_storage import MockStorageBackend
from examples.handlers.table_handler import (
    CreateTableHandler,
    PreviewTableHandler,
    TableImportFromFileHandler
)
from examples.handlers.query_handler import ExecuteQueryHandler
from examples.handlers.info_handler import ObjectInfoHandler


logger = logging.getLogger(__name__)


class StorageDriverServicer(service_pb2_grpc.StorageDriverServiceServicer):
    """
    Implementation of the StorageDriverService.

    This class implements the gRPC service defined in service.proto.
    It handles the Execute() RPC method by:
    1. Unpacking the command from DriverRequest
    2. Routing to the appropriate handler
    3. Wrapping the response in DriverResponse
    4. Including log messages for the client
    """

    def __init__(self, storage: MockStorageBackend):
        """
        Initialize the servicer.

        Args:
            storage: Mock storage backend instance
        """
        self.storage = storage
        logger.info("StorageDriverServicer initialized")

    def Execute(
        self,
        request: common_pb2.DriverRequest,
        context: grpc.ServicerContext
    ) -> common_pb2.DriverResponse:
        """
        Execute a storage driver command.

        This is the main RPC method that handles all commands.
        It uses the google.protobuf.Any wrapper pattern to support
        different command types through a single interface.

        Args:
            request: DriverRequest containing the command
            context: gRPC context

        Returns:
            DriverResponse with command result and log messages

        Raises:
            grpc.RpcError: On validation or execution errors
        """
        try:
            # Extract command type from Any field
            command_type_name = get_type_name(request.command)
            logger.info(f"Received command: {command_type_name}")

            # Log runtime info
            if request.runtimeOptions and request.runtimeOptions.runId:
                logger.debug(f"RunID: {request.runtimeOptions.runId}")

            # Route command to appropriate handler
            handler, command_msg = self._route_command(request.command, command_type_name)

            if handler is None:
                error_msg = f"Unsupported command type: {command_type_name}"
                logger.error(error_msg)
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(error_msg)
                return self._create_error_response(error_msg)

            # Execute handler
            try:
                response_msg = handler.handle(
                    command_msg,
                    None,  # TODO: Unpack credentials from request.credentials
                    request.runtimeOptions
                )

                # Get log messages from handler
                log_messages = handler.get_log_messages()

                # Wrap response
                return self._wrap_response(response_msg, log_messages)

            except ValueError as e:
                # Invalid parameters
                logger.error(f"Invalid parameters: {e}")
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(e))
                return self._create_error_response(str(e), handler.get_log_messages())

            except KeyError as e:
                # Resource not found
                logger.error(f"Resource not found: {e}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(str(e))
                return self._create_error_response(str(e), handler.get_log_messages())

            except Exception as e:
                # Internal error
                logger.exception(f"Internal error processing {command_type_name}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"Internal error: {str(e)}")
                return self._create_error_response(f"Internal error: {str(e)}")

        except Exception as e:
            # Error during routing or unpacking
            logger.exception("Error in Execute()")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return self._create_error_response(str(e))

    def _route_command(
        self,
        command_any,
        command_type_name: str
    ) -> tuple:
        """
        Route command to appropriate handler.

        Args:
            command_any: google.protobuf.Any containing the command
            command_type_name: Name of the command type

        Returns:
            Tuple of (handler_instance, unpacked_command) or (None, None)
        """
        # Table commands
        if command_type_name == 'CreateTableCommand':
            handler = CreateTableHandler(self.storage)
            command = table_pb2.CreateTableCommand()
            command_any.Unpack(command)
            return handler, command

        elif command_type_name == 'PreviewTableCommand':
            handler = PreviewTableHandler(self.storage)
            command = table_pb2.PreviewTableCommand()
            command_any.Unpack(command)
            return handler, command

        elif command_type_name == 'TableImportFromFileCommand':
            handler = TableImportFromFileHandler(self.storage)
            command = table_pb2.TableImportFromFileCommand()
            command_any.Unpack(command)
            return handler, command

        # Query commands
        elif command_type_name == 'ExecuteQueryCommand':
            handler = ExecuteQueryHandler(self.storage)
            command = executeQuery_pb2.ExecuteQueryCommand()
            command_any.Unpack(command)
            return handler, command

        # Info commands
        elif command_type_name == 'ObjectInfoCommand':
            handler = ObjectInfoHandler(self.storage)
            command = info_pb2.ObjectInfoCommand()
            command_any.Unpack(command)
            return handler, command

        # Add more command types here as needed
        # elif command_type_name == 'DropTableCommand':
        #     ...

        return None, None

    def _wrap_response(
        self,
        command_response: Message | None,
        log_messages: list
    ) -> common_pb2.DriverResponse:
        """
        Wrap command response in DriverResponse.

        Args:
            command_response: The command-specific response message (or None)
            log_messages: List of LogMessage instances

        Returns:
            DriverResponse with wrapped response and log messages
        """
        driver_response = common_pb2.DriverResponse()

        # Pack command response if present
        if command_response is not None:
            driver_response.commandResponse.Pack(command_response)

        # Add log messages
        driver_response.messages.extend(log_messages)

        return driver_response

    def _create_error_response(
        self,
        error_message: str,
        log_messages: list | None = None
    ) -> common_pb2.DriverResponse:
        """
        Create an error response.

        Args:
            error_message: Error message
            log_messages: Optional existing log messages

        Returns:
            DriverResponse with error log message
        """
        driver_response = common_pb2.DriverResponse()

        # Add existing log messages
        if log_messages:
            driver_response.messages.extend(log_messages)

        # Add error log message
        error_log = common_pb2.LogMessage()
        error_log.level = common_pb2.LogMessage.Level.Error
        error_log.message = error_message
        driver_response.messages.append(error_log)

        return driver_response


def serve(host: str = "localhost", port: int = 50051, max_workers: int = 10):
    """
    Start the gRPC server.

    Args:
        host: Server host
        port: Server port
        max_workers: Maximum number of worker threads

    Example:
        >>> serve(port=8080, max_workers=20)
    """
    # Set up logging
    setup_logging(logging.INFO)

    # Create storage backend
    storage = MockStorageBackend()

    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))

    # Register servicer
    service_pb2_grpc.add_StorageDriverServiceServicer_to_server(
        StorageDriverServicer(storage),
        server
    )

    # Bind to address
    address = f"{host}:{port}"
    server.add_insecure_port(address)

    # Start server
    server.start()
    logger.info(f"StorageDriverService gRPC server started on {address}")
    logger.info(f"Ready to accept connections (max workers: {max_workers})")

    # Handle graceful shutdown
    def handle_shutdown(signum, frame):
        logger.info("Shutting down server...")
        server.stop(grace=5)
        logger.info("Server stopped")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Wait for termination
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
        server.stop(grace=5)


if __name__ == "__main__":
    """
    Run the server from command line.

    Usage:
        python3 grpc_server.py
        python3 grpc_server.py --port 8080
    """
    import argparse

    parser = argparse.ArgumentParser(description="StorageDriver gRPC Server")
    parser.add_argument("--host", default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--port", type=int, default=50051, help="Server port (default: 50051)")
    parser.add_argument("--workers", type=int, default=10, help="Max worker threads (default: 10)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging level (default: INFO)")

    args = parser.parse_args()

    # Set up logging with specified level
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level)

    # Start server
    serve(host=args.host, port=args.port, max_workers=args.workers)
