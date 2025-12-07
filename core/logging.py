import logging
import logging.config
import json
import sys
from typing import Dict, Any, Optional
from pathlib import Path


class StructuredFormatter(logging.Formatter):
    """Custom formatter that outputs structured JSON logs."""

    def format(self, record: logging.LogRecord) -> str:
        # Create the base log entry
        log_entry = {
            "timestamp": self.formatTime(record, self.default_time_format),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra fields from record
        if hasattr(record, "__dict__"):
            for key, value in record.__dict__.items():
                if key not in {
                    "name",
                    "msg",
                    "args",
                    "levelname",
                    "levelno",
                    "pathname",
                    "filename",
                    "module",
                    "exc_info",
                    "exc_text",
                    "stack_info",
                    "lineno",
                    "funcName",
                    "created",
                    "msecs",
                    "relativeCreated",
                    "thread",
                    "threadName",
                    "processName",
                    "process",
                    "message",
                }:
                    log_entry[key] = value

        return json.dumps(log_entry, default=str)


class ErrorContextFilter(logging.Filter):
    """Filter that adds error context to log records."""

    def __init__(self, service_name: str = "agentcore"):
        super().__init__()
        self.service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:
        # Add service name to all records
        if not hasattr(record, "service"):
            record.service = self.service_name

        # Add component if not present
        if not hasattr(record, "component"):
            # Try to infer component from logger name
            logger_parts = record.name.split(".")
            if len(logger_parts) > 1:
                record.component = logger_parts[-1]
            else:
                record.component = "unknown"

        return True


def setup_logging(
    level: str = "INFO",
    format_type: str = "structured",  # "structured" or "human"
    service_name: str = "agentcore",
    log_file: Optional[str] = None,
) -> None:
    """
    Setup structured logging for the application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: "structured" for JSON logs, "human" for readable logs
        service_name: Name of the service for log identification
        log_file: Optional file path to write logs to
    """
    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Create formatters
    if format_type == "structured":
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    # Setup handlers
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(ErrorContextFilter(service_name))
    handlers.append(console_handler)

    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(ErrorContextFilter(service_name))
        handlers.append(file_handler)

    # Configure root logger
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": level,
                    "formatter": "structured"
                    if format_type == "structured"
                    else "human",
                    "stream": "ext://sys.stdout",
                }
            },
            "formatters": {
                "structured": {
                    "()": StructuredFormatter,
                },
                "human": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "root": {
                "level": level,
                "handlers": ["console"],
            },
            "loggers": {
                # Specific loggers can be configured here
                "core": {
                    "level": level,
                    "handlers": ["console"],
                    "propagate": False,
                },
            },
        }
    )

    # Set root logger level
    logging.getLogger().setLevel(numeric_level)

    # Ensure core logger uses our configuration
    core_logger = logging.getLogger("core")
    core_logger.setLevel(numeric_level)
    for handler in handlers:
        core_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name."""
    return logging.getLogger(f"core.{name}")


class ErrorLogger:
    """Utility class for logging errors with context."""

    def __init__(self, component: str):
        self.logger = get_logger(component)
        self.component = component

    def log_error(
        self,
        error: Exception,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        level: str = "ERROR",
    ) -> None:
        """Log an error with structured context."""
        log_data = {
            "component": self.component,
            "error_type": type(error).__name__,
            "error_message": str(error),
        }

        if context:
            log_data.update(context)

        log_method = getattr(self.logger, level.lower(), self.logger.error)
        log_method(message, extra=log_data, exc_info=True)

    def log_operation_error(
        self, operation: str, error: Exception, context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log an error that occurred during an operation."""
        message = f"Error during {operation}"
        self.log_error(error, message, context)
