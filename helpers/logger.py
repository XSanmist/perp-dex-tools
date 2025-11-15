"""
Trading logger with structured output and error handling.
"""

import os
import csv
import logging
import logging.handlers
import queue
import asyncio
import sys
from datetime import datetime, timezone, timedelta
import pytz
from decimal import Decimal


class TradingLogger:
    """Enhanced logging with structured output and error handling."""

    def __init__(self, exchange: str, ticker: str, log_to_console: bool = False):
        self.exchange = exchange
        self.ticker = ticker
        # Ensure logs directory exists at the project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        logs_dir = os.path.join(project_root, 'logs')
        os.makedirs(logs_dir, exist_ok=True)

        order_file_name = f"{exchange}_{ticker}_orders.csv"
        debug_log_file_name = f"{exchange}_{ticker}_activity.log"

        account_name = os.getenv('ACCOUNT_NAME')
        if account_name:
            order_file_name = f"{exchange}_{ticker}_{account_name}_orders.csv"
            debug_log_file_name = f"{exchange}_{ticker}_{account_name}_activity.log"

        # Log file paths inside logs directory
        self.log_file = os.path.join(logs_dir, order_file_name)
        self.debug_log_file = os.path.join(logs_dir, debug_log_file_name)
        self.timezone = pytz.timezone(os.getenv('TIMEZONE', 'Asia/Shanghai'))
        self.logger = self._setup_logger(log_to_console)

    def _setup_logger(self, log_to_console: bool) -> logging.Logger:
        """Setup the logger with proper configuration."""
        logger = logging.getLogger(f"trading_bot_{self.exchange}_{self.ticker}")
        logger.setLevel(logging.INFO)

        # Prevent propagation to root logger to avoid duplicate messages
        logger.propagate = False

        # Prevent duplicate handlers
        if logger.handlers:
            return logger

        class TimeZoneFormatter(logging.Formatter):
            def __init__(self, fmt=None, datefmt=None, tz=None):
                super().__init__(fmt=fmt, datefmt=datefmt)
                self.tz = tz

            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=self.tz)
                if datefmt:
                    return dt.strftime(datefmt)
                return dt.isoformat()

        formatter = TimeZoneFormatter(
            "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            tz=self.timezone
        )

        # File handler
        file_handler = logging.FileHandler(self.debug_log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler if requested
        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def log(self, message: str, level: str = "INFO"):
        """Log a message with the specified level."""
        formatted_message = f"[{self.exchange.upper()}_{self.ticker.upper()}] {message}"
        if level.upper() == "DEBUG":
            self.logger.debug(formatted_message)
        elif level.upper() == "INFO":
            self.logger.info(formatted_message)
        elif level.upper() == "WARNING":
            self.logger.warning(formatted_message)
        elif level.upper() == "ERROR":
            self.logger.error(formatted_message)
        else:
            self.logger.info(formatted_message)

    def log_transaction(self, order_id: str, side: str, quantity: Decimal, price: Decimal, status: str):
        """Log a transaction to CSV file."""
        try:
            timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S")
            row = [timestamp, order_id, side, quantity, price, status]

            # Check if file exists to write headers
            file_exists = os.path.isfile(self.log_file)

            with open(self.log_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['Timestamp', 'OrderID', 'Side', 'Quantity', 'Price', 'Status'])
                writer.writerow(row)

        except Exception as e:
            self.log(f"Failed to log transaction: {e}", "ERROR")


# ==================== Hedge Mode Logger ====================

class UTC8Formatter(logging.Formatter):
    """Formatter that converts timestamps to UTC+8"""
    def formatTime(self, record, datefmt=None):
        # Convert the timestamp to UTC+8
        dt = datetime.fromtimestamp(record.created, tz=timezone(timedelta(hours=8)))
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]  # Same format as default, with milliseconds


class AsyncQueueHandler(logging.Handler):
    """
    Custom logging handler that puts log records into an asyncio.Queue.
    This allows log operations to be completely non-blocking.
    """
    def __init__(self, log_queue: asyncio.Queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        """Put log record into queue without blocking."""
        try:
            # Use put_nowait to avoid blocking
            # If queue is full, we'll catch the exception
            self.log_queue.put_nowait(record)
        except asyncio.QueueFull:
            # If queue is full, drop the log message
            # This prevents blocking the main thread
            pass
        except Exception:
            # Silently ignore other errors to prevent blocking
            pass


class HedgeLogger:
    """
    Advanced logger for hedge mode trading with fully async, non-blocking I/O.

    Features:
    - 100% non-blocking: Uses asyncio.Queue + background task for all I/O
    - Real-time logging: Background task writes immediately
    - UTC+8 timezone support
    - CSV trade logging
    - Graceful shutdown with automatic flush

    Usage:
        logger = HedgeLogger(exchange="extended", ticker="ETH")
        await logger.start()  # Start background logging task
        logger.info("Trading started")
        logger.log_trade(exchange="extended", side="buy", price=3500.0, quantity=0.1)
        await logger.shutdown()  # Stop background task and flush
    """

    def __init__(self, exchange: str, ticker: str, log_dir: str = "logs"):
        self.exchange = exchange
        self.ticker = ticker

        # Create logs directory
        os.makedirs(log_dir, exist_ok=True)

        # Log file paths
        self.log_filename = f"{log_dir}/{exchange}_{ticker}_hedge_mode_log.txt"
        self.csv_filename = f"{log_dir}/{exchange}_{ticker}_hedge_mode_trades.csv"

        # Initialize CSV file
        self._initialize_csv_file()

        # Create asyncio queue for log records
        self.log_queue = asyncio.Queue(maxsize=1000)  # Limit queue size

        # Background task for writing logs
        self.writer_task = None
        self._shutdown = False

        # Setup logger
        self.logger = logging.getLogger(f"hedge_bot_{ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        # Disable verbose logging from external libraries
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        # Create async queue handler (non-blocking)
        queue_handler = AsyncQueueHandler(self.log_queue)
        queue_handler.setLevel(logging.INFO)

        # Create formatter
        formatter = UTC8Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        queue_handler.setFormatter(formatter)

        # Add handler to logger
        self.logger.addHandler(queue_handler)

        # Prevent propagation to root logger
        self.logger.propagate = False

        # Open log file for writing (will be used by background task)
        self.log_file = open(self.log_filename, 'a', encoding='utf-8', buffering=1)

    async def start(self):
        """Start the background logging task. Must be called from async context."""
        if self.writer_task is None:
            self.writer_task = asyncio.create_task(self._log_writer())

    async def _log_writer(self):
        """Background task that writes log records from queue to file."""
        formatter = UTC8Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        while not self._shutdown or not self.log_queue.empty():
            try:
                # Wait for log record with timeout
                record = await asyncio.wait_for(self.log_queue.get(), timeout=0.1)

                # Format and write to file
                formatted = formatter.format(record)
                self.log_file.write(formatted + '\n')
                self.log_file.flush()  # Immediate flush

                # Also write to stderr for console
                sys.stderr.write(f"{record.levelname}:{record.name}:{record.getMessage()}\n")
                sys.stderr.flush()

            except asyncio.TimeoutError:
                # No log records, continue waiting
                continue
            except Exception as e:
                # Log errors to stderr to avoid infinite loop
                sys.stderr.write(f"Logger error: {e}\n")
                continue

    def _initialize_csv_file(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity'])

    def info(self, message: str):
        """Log info message."""
        self.logger.info(message)

    def warning(self, message: str):
        """Log warning message."""
        self.logger.warning(message)

    def error(self, message: str):
        """Log error message."""
        self.logger.error(message)

    def debug(self, message: str):
        """Log debug message."""
        self.logger.debug(message)

    def log_trade(self, exchange: str, side: str, price: float, quantity: float):
        """
        Log a trade to CSV file.

        Args:
            exchange: Exchange name (e.g., "extended", "lighter")
            side: Trade side ("buy" or "sell")
            price: Trade price
            quantity: Trade quantity
        """
        try:
            timestamp = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
            with open(self.csv_filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([exchange, timestamp, side, str(price), str(quantity)])
        except Exception as e:
            self.error(f"Failed to log trade: {e}")

    async def shutdown(self):
        """
        Gracefully shutdown the logger, flushing all pending logs.
        IMPORTANT: Call this method before program exit to ensure all logs are written.
        """
        # Signal shutdown
        self._shutdown = True

        # Wait for background task to finish processing queue
        if self.writer_task:
            try:
                await asyncio.wait_for(self.writer_task, timeout=5.0)
            except asyncio.TimeoutError:
                # Force cancel if timeout
                self.writer_task.cancel()
                try:
                    await self.writer_task
                except asyncio.CancelledError:
                    pass

        # Close log file
        if hasattr(self, 'log_file') and self.log_file:
            try:
                self.log_file.flush()
                self.log_file.close()
            except Exception:
                pass

        # Remove handlers
        for handler in self.logger.handlers[:]:
            try:
                self.logger.removeHandler(handler)
            except Exception:
                pass
