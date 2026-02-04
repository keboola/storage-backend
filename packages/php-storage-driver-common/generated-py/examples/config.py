"""
Configuration for the gRPC server example.

This module provides configuration settings for the StorageDriverService gRPC server.
"""

import logging
from typing import Dict, Any


class ServerConfig:
    """
    Configuration for the gRPC server.

    Attributes:
        host: Server host address
        port: Server port
        max_workers: Maximum number of worker threads for handling requests
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_reflection: Enable gRPC server reflection for debugging
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        max_workers: int = 10,
        log_level: str = "INFO",
        enable_reflection: bool = True
    ):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.log_level = getattr(logging, log_level.upper())
        self.enable_reflection = enable_reflection

    @property
    def address(self) -> str:
        """Returns the full server address."""
        return f"{self.host}:{self.port}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "max_workers": self.max_workers,
            "log_level": logging.getLevelName(self.log_level),
            "enable_reflection": self.enable_reflection,
        }


# Default configuration
DEFAULT_CONFIG = ServerConfig()


def get_config(**kwargs) -> ServerConfig:
    """
    Get server configuration with optional overrides.

    Args:
        **kwargs: Configuration overrides

    Returns:
        ServerConfig instance

    Example:
        >>> config = get_config(port=8080, log_level="DEBUG")
        >>> print(config.address)
        localhost:8080
    """
    return ServerConfig(**kwargs)
