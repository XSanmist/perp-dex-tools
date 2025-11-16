import asyncio
import json
import signal
import os
import sys
import time
import requests
import argparse
import traceback
from decimal import Decimal
from typing import Tuple

from lighter.signer_client import SignerClient
import lighter  # For AccountApi to fetch positions
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.extended import ExtendedClient
import websockets
from datetime import datetime, timezone, timedelta

# Import the unified hedge logger
from helpers.logger import HedgeLogger
from helpers.telegram_bot import TelegramBot


class Config:
    """Simple config class to wrap dictionary for Extended client."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class HedgeBot:
    """Trading bot that places post-only orders on Extended and hedges with market orders on Lighter."""

    def __init__(self, ticker: str, order_quantity: Decimal, fill_timeout: int = 5, iterations: int = 20, sleep_time: int = 0, task_id: str = ''):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.lighter_order_filled = False
        self.iterations = iterations
        self.sleep_time = sleep_time
        self.extended_position = Decimal('0')
        self.lighter_position = Decimal('0')
        self.current_order = {}
        self.task_id = task_id
        self.current_iteration = 0
        self.start_time = datetime.now()

        # Initialize unified hedge logger
        self.hedge_logger = HedgeLogger(exchange="extended", ticker=ticker)
        self.logger = self.hedge_logger.logger  # For backward compatibility
        self.log_filename = self.hedge_logger.log_filename
        self.csv_filename = self.hedge_logger.csv_filename

        # State management
        self.stop_flag = False
        self.order_counter = 0

        # Extended state
        self.extended_client = None
        self.extended_contract_id = None
        self.extended_tick_size = None
        self.extended_order_status = None
        self.current_extended_order_id = None  # Track current order ID
        self.extended_order_hedged_size = Decimal('0')  # Track hedged size for current order

        # Extended order book state for websocket-based BBO
        self.extended_order_book = {'bids': {}, 'asks': {}}
        self.extended_best_bid = None
        self.extended_best_ask = None
        self.extended_order_book_ready = False

        # Lighter order book state
        self.lighter_client = None
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_order_book_ready = False
        self.lighter_order_book_offset = 0
        self.lighter_order_book_sequence_gap = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()
        self.lighter_order_lock = asyncio.Lock()  # Lock for sequential order placement to prevent nonce conflicts
        self.lighter_pending_hedge_size = Decimal('0')  # Accumulate small hedge amounts that are below min order size
        self.lighter_min_order_size = None  # Will be loaded from market config

        # Lighter WebSocket state
        self.lighter_ws_task = None
        self.lighter_order_result = None

        # Lighter order management
        self.lighter_order_status = None
        self.lighter_order_price = None
        self.lighter_order_side = None
        self.lighter_order_size = None
        self.lighter_order_start_time = None

        # Strategy state
        self.waiting_for_lighter_fill = False
        self.wait_start_time = None

        # Order execution tracking
        self.order_execution_complete = False

        # Current order details for immediate execution
        self.current_lighter_side = None
        self.current_lighter_quantity = None
        self.current_lighter_price = None
        self.lighter_order_info = None

        # Lighter API configuration
        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX'))

        # Extended configuration
        self.extended_vault = os.getenv('EXTENDED_VAULT')
        self.extended_stark_key_private = os.getenv('EXTENDED_STARK_KEY_PRIVATE')
        self.extended_stark_key_public = os.getenv('EXTENDED_STARK_KEY_PUBLIC')
        self.extended_api_key = os.getenv('EXTENDED_API_KEY')

        # Telegram configuration
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.last_progress_notification = 0  # Track last progress percentage notified

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")

        # Close WebSocket connections
        if self.extended_client:
            try:
                # Note: disconnect() is async, but shutdown() is sync
                # We'll let the cleanup happen naturally
                self.logger.info("🔌 Extended WebSocket will be disconnected")
            except Exception as e:
                self.logger.error(f"Error disconnecting Extended WebSocket: {e}")

        # Cancel Lighter WebSocket task
        if self.lighter_ws_task and not self.lighter_ws_task.done():
            try:
                self.lighter_ws_task.cancel()
                self.logger.info("🔌 Lighter WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling Lighter WebSocket task: {e}")

        # Note: Logger will be shutdown in async_shutdown called from run()

    async def async_shutdown(self):
        """Async shutdown for logger only."""
        if hasattr(self, 'hedge_logger'):
            await self.hedge_logger.shutdown()

    async def reconnect_lighter_websocket(self):
        """Reconnect Lighter WebSocket by cancelling and restarting the task."""
        try:
            # Cancel existing WebSocket task
            if self.lighter_ws_task and not self.lighter_ws_task.done():
                self.logger.info("🔄 Cancelling Lighter WebSocket task for reconnection...")
                self.lighter_ws_task.cancel()
                try:
                    await self.lighter_ws_task
                except asyncio.CancelledError:
                    pass

            # Reset orderbook state
            await self.reset_lighter_order_book()

            # Restart WebSocket task
            self.logger.info("🔄 Restarting Lighter WebSocket connection...")
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())

            # Give it a moment to establish connection
            await asyncio.sleep(1)

            self.logger.info("✅ Lighter WebSocket reconnection initiated")
        except Exception as e:
            self.logger.error(f"❌ Error during Lighter WebSocket reconnection: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")

    def send_telegram_notification(self, message: str):
        """Send notification via Telegram."""
        if not self.telegram_token or not self.telegram_chat_id:
            return

        try:
            with TelegramBot(self.telegram_token, self.telegram_chat_id) as tg_bot:
                tg_bot.send_text(message)
        except Exception as e:
            self.logger.error(f"Failed to send Telegram notification: {e}")

    def check_and_notify_progress(self):
        """Check progress and send notification every 5%."""
        if self.iterations <= 0:
            return

        current_progress = int((self.current_iteration / self.iterations) * 100)

        # Notify every 5%
        progress_milestone = (current_progress // 5) * 5

        if progress_milestone > self.last_progress_notification and progress_milestone % 5 == 0:
            self.last_progress_notification = progress_milestone

            message = (
                f"🤖 <b>Hedge Bot Progress Update</b>\n\n"
                f"Ticker: <code>{self.ticker}</code>\n"
                f"Progress: <b>{progress_milestone}%</b> ({self.current_iteration}/{self.iterations})\n"
                f"Extended Position: <code>{self.extended_position}</code>\n"
                f"Lighter Position: <code>{self.lighter_position}</code>"
            )

            self.send_telegram_notification(message)
            self.logger.info(f"📱 Progress notification sent: {progress_milestone}%")

    def update_status(self):
        """Update shared status file with current bot state."""
        if not self.task_id:
            return

        status_file = "logs/process_status.json"
        os.makedirs("logs", exist_ok=True)

        # Load existing status data
        try:
            if os.path.exists(status_file):
                with open(status_file, 'r') as f:
                    status_data = json.load(f)
            else:
                status_data = {}
        except Exception:
            status_data = {}

        # Calculate runtime
        runtime_seconds = int((datetime.now() - self.start_time).total_seconds())

        # Update status for this task
        status_data[self.task_id] = {
            'current_iteration': self.current_iteration,
            'total_iterations': self.iterations,
            'primary_position': float(self.extended_position),
            'secondary_position': float(self.lighter_position),
            'primary_exchange': self.exchange_name,
            'secondary_exchange': 'lighter',
            'runtime_seconds': runtime_seconds
        }

        # Write back to file
        try:
            with open(status_file, 'w') as f:
                json.dump(status_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to update status file: {e}")

    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
        """Log trade details to CSV file using HedgeLogger."""
        self.hedge_logger.log_trade(
            exchange=exchange,
            side=side,
            price=float(price),
            quantity=float(quantity)
        )
        self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price}")

    def handle_lighter_order_result(self, order_data):
        """Handle Lighter order result from WebSocket."""
        try:
            order_data["avg_filled_price"] = (Decimal(order_data["filled_quote_amount"]) /
                                              Decimal(order_data["filled_base_amount"]))
            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                self.lighter_position -= Decimal(order_data["filled_base_amount"])
            else:
                order_data["side"] = "LONG"
                self.lighter_position += Decimal(order_data["filled_base_amount"])

            self.logger.info(f"📊 Lighter order filled: {order_data['side']} "
                             f"{order_data['filled_base_amount']} @ {order_data['avg_filled_price']}")

            # Log Lighter trade to CSV
            self.log_trade_to_csv(
                exchange='Lighter',
                side=order_data['side'],
                price=str(order_data['avg_filled_price']),
                quantity=str(order_data['filled_base_amount'])
            )

            # Mark execution as complete
            self.lighter_order_filled = True  # Mark order as filled
            self.order_execution_complete = True

            # Update status after position change
            self.update_status()

        except Exception as e:
            self.logger.error(f"Error handling Lighter order result: {e}")

    async def reset_lighter_order_book(self):
        """Reset Lighter order book state."""
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_sequence_gap = False
            self.lighter_snapshot_loaded = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list):
        """Update Lighter order book with new levels."""
        for level in levels:
            # Handle different data structures - could be list [price, size] or dict {"price": ..., "size": ...}
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(level[0])
                size = Decimal(level[1])
            elif isinstance(level, dict):
                price = Decimal(level.get("price", 0))
                size = Decimal(level.get("size", 0))
            else:
                self.logger.warning(f"⚠️ Unexpected level format: {level}")
                continue

            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                # Remove zero size orders
                self.lighter_order_book[side].pop(price, None)

    def validate_order_book_offset(self, new_offset: int) -> bool:
        """Validate order book offset sequence."""
        if new_offset <= self.lighter_order_book_offset:
            self.logger.warning(
                f"⚠️ Out-of-order update: new_offset={new_offset}, current_offset={self.lighter_order_book_offset}")
            return False
        return True

    def validate_order_book_integrity(self) -> bool:
        """Validate order book integrity."""
        # Check for negative prices or sizes
        for side in ["bids", "asks"]:
            for price, size in self.lighter_order_book[side].items():
                if price <= 0 or size <= 0:
                    self.logger.error(f"❌ Invalid order book data: {side} price={price}, size={size}")
                    return False
        return True

    def get_lighter_best_levels(self) -> Tuple[Tuple[Decimal, Decimal], Tuple[Decimal, Decimal]]:
        """Get best bid and ask levels from Lighter order book."""
        best_bid = None
        best_ask = None

        if self.lighter_order_book["bids"]:
            best_bid_price = max(self.lighter_order_book["bids"].keys())
            best_bid_size = self.lighter_order_book["bids"][best_bid_price]
            best_bid = (best_bid_price, best_bid_size)

        if self.lighter_order_book["asks"]:
            best_ask_price = min(self.lighter_order_book["asks"].keys())
            best_ask_size = self.lighter_order_book["asks"][best_ask_price]
            best_ask = (best_ask_price, best_ask_size)

        return best_bid, best_ask

    def get_lighter_mid_price(self) -> Decimal:
        """Get mid price from Lighter order book."""
        best_bid, best_ask = self.get_lighter_best_levels()

        if best_bid is None or best_ask is None:
            raise Exception("Cannot calculate mid price - missing order book data")

        mid_price = (best_bid[0] + best_ask[0]) / Decimal('2')
        return mid_price

    def get_lighter_order_price(self, is_ask: bool) -> Decimal:
        """Get order price from Lighter order book."""
        best_bid, best_ask = self.get_lighter_best_levels()

        if best_bid is None or best_ask is None:
            raise Exception("Cannot calculate order price - missing order book data")

        if is_ask:
            order_price = best_bid[0] + self.tick_size
        else:
            order_price = best_ask[0] - self.tick_size

        return order_price

    def calculate_adjusted_price(self, original_price: Decimal, side: str, adjustment_percent: Decimal) -> Decimal:
        """Calculate adjusted price for order modification."""
        adjustment = original_price * adjustment_percent

        if side.lower() == 'buy':
            # For buy orders, increase price to improve fill probability
            return original_price + adjustment
        else:
            # For sell orders, decrease price to improve fill probability
            return original_price - adjustment

    async def request_fresh_snapshot(self, ws):
        """Request fresh order book snapshot."""
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

    async def handle_lighter_ws(self):
        """Handle Lighter WebSocket connection and messages."""
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        cleanup_counter = 0

        while not self.stop_flag:
            timeout_count = 0
            try:
                # Reset order book state before connecting
                await self.reset_lighter_order_book()

                async with websockets.connect(url) as ws:
                    # Subscribe to order book updates
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

                    # Subscribe to account orders updates
                    account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"

                    # Get auth token for the subscription
                    try:
                        # Set auth token to expire in 10 minutes
                        ten_minutes_deadline = int(time.time() + 10 * 60)
                        auth_token, err = self.lighter_client.create_auth_token_with_expiry(ten_minutes_deadline)
                        if err is not None:
                            self.logger.warning(f"⚠️ Failed to create auth token for account orders subscription: {err}")
                        else:
                            auth_message = {
                                "type": "subscribe",
                                "channel": account_orders_channel,
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(auth_message))
                            self.logger.info("✅ Subscribed to account orders with auth token (expires in 10 minutes)")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error creating auth token for account orders subscription: {e}")

                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError as e:
                                self.logger.warning(f"⚠️ JSON parsing error in Lighter websocket: {e}")
                                continue

                            # Reset timeout counter on successful message
                            timeout_count = 0

                            async with self.lighter_order_book_lock:
                                if data.get("type") == "subscribed/order_book":
                                    # Initial snapshot - clear and populate the order book
                                    self.lighter_order_book["bids"].clear()
                                    self.lighter_order_book["asks"].clear()

                                    # Handle the initial snapshot
                                    order_book = data.get("order_book", {})
                                    if order_book and "offset" in order_book:
                                        self.lighter_order_book_offset = order_book["offset"]
                                        self.logger.info(f"✅ Initial order book offset set to: {self.lighter_order_book_offset}")

                                    # Debug: Log the structure of bids and asks
                                    bids = order_book.get("bids", [])
                                    asks = order_book.get("asks", [])
                                    if bids:
                                        self.logger.debug(f"📊 Sample bid structure: {bids[0] if bids else 'None'}")
                                    if asks:
                                        self.logger.debug(f"📊 Sample ask structure: {asks[0] if asks else 'None'}")

                                    self.update_lighter_order_book("bids", bids)
                                    self.update_lighter_order_book("asks", asks)
                                    self.lighter_snapshot_loaded = True
                                    self.lighter_order_book_ready = True

                                    self.logger.info(f"✅ Lighter order book snapshot loaded with "
                                                     f"{len(self.lighter_order_book['bids'])} bids and "
                                                     f"{len(self.lighter_order_book['asks'])} asks")

                                elif data.get("type") == "update/order_book" and self.lighter_snapshot_loaded:
                                    # Extract offset from the message
                                    order_book = data.get("order_book", {})
                                    if not order_book or "offset" not in order_book:
                                        self.logger.warning("⚠️ Order book update missing offset, skipping")
                                        continue

                                    new_offset = order_book["offset"]

                                    # Validate offset sequence
                                    if not self.validate_order_book_offset(new_offset):
                                        self.lighter_order_book_sequence_gap = True
                                        break

                                    # Update the order book with new data
                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))

                                    # Validate order book integrity after update
                                    if not self.validate_order_book_integrity():
                                        self.logger.warning("🔄 Order book integrity check failed, requesting fresh snapshot...")
                                        break

                                    # Get the best bid and ask levels
                                    best_bid, best_ask = self.get_lighter_best_levels()

                                    # Update global variables
                                    if best_bid is not None:
                                        self.lighter_best_bid = best_bid[0]
                                    if best_ask is not None:
                                        self.lighter_best_ask = best_ask[0]

                                elif data.get("type") == "ping":
                                    # Respond to ping with pong
                                    await ws.send(json.dumps({"type": "pong"}))
                                elif data.get("type") == "update/account_orders":
                                    # Handle account orders updates
                                    orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                                    if len(orders) == 1:
                                        order_data = orders[0]
                                        if order_data.get("status") == "filled":
                                            self.handle_lighter_order_result(order_data)
                                elif data.get("type") == "update/order_book" and not self.lighter_snapshot_loaded:
                                    # Ignore updates until we have the initial snapshot
                                    continue

                            # Periodic cleanup outside the lock
                            cleanup_counter += 1
                            if cleanup_counter >= 1000:
                                cleanup_counter = 0

                            # Handle sequence gap and integrity issues outside the lock
                            if self.lighter_order_book_sequence_gap:
                                try:
                                    await self.request_fresh_snapshot(ws)
                                    self.lighter_order_book_sequence_gap = False
                                except Exception as e:
                                    self.logger.error(f"⚠️ Failed to request fresh snapshot: {e}")
                                    break

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            if timeout_count % 3 == 0:
                                self.logger.warning(f"⏰ No message from Lighter websocket for {timeout_count} seconds")
                            continue
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(f"⚠️ Lighter websocket connection closed: {e}")
                            break
                        except websockets.exceptions.WebSocketException as e:
                            self.logger.warning(f"⚠️ Lighter websocket error: {e}")
                            break
                        except Exception as e:
                            self.logger.error(f"⚠️ Error in Lighter websocket: {e}")
                            self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                            break
            except Exception as e:
                self.logger.error(f"⚠️ Failed to connect to Lighter websocket: {e}")

            # Wait a bit before reconnecting
            await asyncio.sleep(2)

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def initialize_lighter_client(self):
        """Initialize the Lighter client."""
        if self.lighter_client is None:
            api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
            if not api_key_private_key:
                raise Exception("API_KEY_PRIVATE_KEY environment variable not set")

            self.logger.info("Lighter client create")
            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                private_key=api_key_private_key,
                account_index=self.account_index,
                api_key_index=self.api_key_index,
            )

            # Check client
            self.logger.info("Lighter client check")
            err = self.lighter_client.check_client()
            if err is not None:
                raise Exception(f"CheckClient error: {err}")

            # Initialize API client for position queries
            configuration = lighter.Configuration(host=self.lighter_base_url)
            self.lighter_api_client = lighter.ApiClient(configuration)

            self.logger.info("✅ Lighter client initialized successfully")
        return self.lighter_client

    def initialize_extended_client(self):
        """Initialize the Extended client."""
        if not all([self.extended_vault, self.extended_stark_key_private, self.extended_stark_key_public, self.extended_api_key]):
            raise ValueError("EXTENDED_VAULT, EXTENDED_STARK_KEY_PRIVATE, EXTENDED_STARK_KEY_PUBLIC, and EXTENDED_API_KEY must be set in environment variables")

        # Create config for Extended client
        config_dict = {
            'ticker': self.ticker,
            'contract_id': '',  # Will be set when we get contract info
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),  # Will be updated when we get contract info
            'close_order_side': 'sell'  # Default, will be updated based on strategy
        }

        # Wrap in Config class for Extended client
        config = Config(config_dict)

        # Initialize Extended client
        self.extended_client = ExtendedClient(config)

        self.logger.info("✅ Extended client initialized successfully")
        return self.extended_client

    def get_lighter_market_config(self) -> Tuple[int, int, int, Decimal, Decimal]:
        """Get Lighter market configuration."""
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        headers = {"accept": "application/json"}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            if not response.text.strip():
                raise Exception("Empty response from Lighter API")

            data = response.json()

            if "order_books" not in data:
                raise Exception("Unexpected response format")

            for market in data["order_books"]:
                if market["symbol"] == self.ticker:
                    price_multiplier = pow(10, market["supported_price_decimals"])
                    min_base_amount = Decimal(market.get("min_base_amount", "0"))
                    return (market["market_id"],
                           pow(10, market["supported_size_decimals"]),  # size multiplier
                           price_multiplier,  # price multiplier
                           Decimal("1") / (Decimal("10") ** market["supported_price_decimals"]), # price step/ ticker size
                           min_base_amount  # minimum order size
                           )

            raise Exception(f"Ticker {self.ticker} not found")

        except Exception as e:
            self.logger.error(f"⚠️ Error getting market config: {e}")
            raise

    async def get_extended_contract_info(self) -> Tuple[str, Decimal]:
        """Get Extended contract ID and tick size."""
        if not self.extended_client:
            raise Exception("Extended client not initialized")

        contract_id, tick_size = await self.extended_client.get_contract_attributes()

        if self.order_quantity < self.extended_client.config.quantity:
            raise ValueError(
                f"Order quantity is less than min quantity: {self.order_quantity} < {self.extended_client.config.quantity}")

        return contract_id, tick_size

    async def fetch_extended_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        """Fetch best bid/ask prices from Extended using websocket data."""
        # Use WebSocket data if available
        if self.extended_order_book_ready and self.extended_best_bid and self.extended_best_ask:
            if self.extended_best_bid > 0 and self.extended_best_ask > 0 and self.extended_best_bid < self.extended_best_ask:
                return self.extended_best_bid, self.extended_best_ask

        # Fallback to REST API if websocket data is not available
        self.logger.warning("WebSocket BBO data not available, falling back to REST API")
        if not self.extended_client:
            raise Exception("Extended client not initialized")

        best_bid, best_ask = await self.extended_client.fetch_bbo_prices(self.extended_contract_id)

        return best_bid, best_ask

    def round_to_tick(self, price: Decimal) -> Decimal:
        """Round price to tick size."""
        if self.extended_tick_size is None:
            return price
        return (price / self.extended_tick_size).quantize(Decimal('1')) * self.extended_tick_size

    async def place_bbo_order(self, side: str, quantity: Decimal):
        self.logger.info(f"place_bbo_order called: {side} {quantity}")

        # Get best bid/ask prices with timeout
        try:
            self.logger.info(f"Fetching BBO prices...")
            best_bid, best_ask = await asyncio.wait_for(
                self.fetch_extended_bbo_prices(),
                timeout=10.0
            )
            self.logger.info(f"BBO prices fetched: bid={best_bid}, ask={best_ask}")
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout fetching BBO prices")
            raise Exception("Timeout fetching BBO prices")
        except Exception as e:
            self.logger.error(f"Error fetching BBO prices: {e}")
            raise

        # Place the order using Extended client with timeout
        try:
            self.logger.info(f"Placing order: {side} {quantity} @ contract {self.extended_contract_id}")
            order_result = await asyncio.wait_for(
                self.extended_client.place_open_order(
                    contract_id=self.extended_contract_id,
                    quantity=quantity,
                    direction=side.lower()
                ),
                timeout=10.0
            )
            self.logger.info(f"Order placement result: success={order_result.success}, order_id={order_result.order_id}")
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout placing order")
            raise Exception("Timeout placing order")
        except Exception as e:
            self.logger.error(f"Error placing order: {e}")
            raise

        if order_result.success:
            return order_result.order_id, order_result.price
        else:
            raise Exception(f"Failed to place order: {order_result.error_message}")

    async def place_extended_post_only_order(self, side: str, quantity: Decimal):
        """Place a post-only order on Extended."""
        if not self.extended_client:
            raise Exception("Extended client not initialized")

        # Track target quantity and filled quantity for this order session
        target_quantity = quantity
        session_start_position = self.extended_position  # Record starting position

        self.extended_order_status = None
        self.logger.info(f"[OPEN] [Extended] [{side}] Placing Extended POST-ONLY order for {target_quantity}")
        self.logger.info(f"Session start - Target: {target_quantity}, Starting position: {session_start_position}")

        order_id, order_price = await self.place_bbo_order(side, target_quantity)

        start_time = time.time()
        last_cancel_time = 0
        loop_count = 0

        while not self.stop_flag:
            loop_count += 1

            if self.extended_order_status in ['CANCELED', 'CANCELLED']:
                # Calculate how much has been filled so far in this session
                position_change = self.extended_position - session_start_position
                if side == 'buy':
                    filled_so_far = position_change  # Positive for buy
                else:
                    filled_so_far = -position_change  # Position decreases on sell, so negate

                remaining_quantity = target_quantity - filled_so_far

                self.logger.info(f"Order {order_id} was canceled")
                self.logger.info(f"📊 Session tracking:")
                self.logger.info(f"   Target quantity:    {target_quantity}")
                self.logger.info(f"   Filled so far:      {filled_so_far}")
                self.logger.info(f"   Remaining quantity: {remaining_quantity}")
                self.logger.info(f"   Current position:   {self.extended_position} (started at {session_start_position})")

                # Check if we've already filled enough
                if remaining_quantity <= Decimal('0.001'):
                    self.logger.info(f"✅ Target quantity already filled via partial fills, no need to place new order")
                    break

                # Check if remaining quantity is below minimum order size
                extended_min_size = getattr(self.extended_client, 'min_order_size', Decimal('0.01'))
                if remaining_quantity < extended_min_size:
                    self.logger.info(
                        f"⚠️ Remaining quantity {remaining_quantity} is below minimum order size {extended_min_size}, "
                        f"treating as filled. Session completed with {filled_so_far}/{target_quantity} filled."
                    )
                    self.logger.info(
                        f"📊 Residual unfilled: {remaining_quantity} (will not be placed due to exchange minimum)"
                    )
                    break

                self.extended_order_status = None  # Reset to None to trigger new order

                try:
                    self.logger.info(f"Placing new order for remaining quantity: {side} {remaining_quantity}")
                    order_id, order_price = await self.place_bbo_order(side, remaining_quantity)
                    self.logger.info(f"New order placed: {order_id} @ {order_price}")
                    start_time = time.time()
                    last_cancel_time = 0  # Reset cancel timer
                except Exception as e:
                    self.logger.error(f"❌ Failed to place new order after cancellation: {e}")
                    import traceback
                    self.logger.error(f"❌ Traceback: {traceback.format_exc()}")
                    await asyncio.sleep(1)
                    continue
                await asyncio.sleep(0.5)
            elif self.extended_order_status in ['NEW', 'OPEN', 'PENDING', 'CANCELING', 'PARTIALLY_FILLED']:
                await asyncio.sleep(0.5)

                # Check if we need to cancel and replace the order
                should_cancel = False
                if side == 'buy':
                    if order_price < self.extended_best_bid:
                        should_cancel = True
                        self.logger.info(f"Order price {order_price} < best bid {self.extended_best_bid}, should cancel")
                else:
                    if order_price > self.extended_best_ask:
                        should_cancel = True
                        self.logger.info(f"Order price {order_price} > best ask {self.extended_best_ask}, should cancel")

                # Cancel order if it's been too long or price is off
                current_time = time.time()
                time_elapsed = current_time - start_time

                # Force cancel after 30 seconds regardless of price
                force_cancel = time_elapsed > 30

                if time_elapsed > 10:
                    if (should_cancel or force_cancel) and current_time - last_cancel_time > 5:  # Prevent rapid cancellations
                        try:
                            reason = "price mismatch" if should_cancel else "force timeout (30s)"
                            self.logger.info(f"Canceling order {order_id} due to {reason} (elapsed: {time_elapsed:.1f}s, order_price: {order_price}, best_bid: {self.extended_best_bid}, best_ask: {self.extended_best_ask})")
                            cancel_result = await asyncio.wait_for(
                                self.extended_client.cancel_order(order_id),
                                timeout=10.0
                            )
                            self.logger.info(f"cancel_result: {cancel_result}")
                            if cancel_result.success:
                                last_cancel_time = current_time
                                # Don't reset start_time here, let the cancellation trigger new order
                            else:
                                self.logger.error(f"❌ Error canceling Extended order: {cancel_result.error_message}")
                        except asyncio.TimeoutError:
                            self.logger.error(f"❌ Timeout canceling Extended order {order_id}")
                        except Exception as e:
                            self.logger.error(f"❌ Error canceling Extended order: {e}")
                    elif not should_cancel and not force_cancel:
                        self.logger.info(f"Waiting for Extended order to be filled (elapsed: {time_elapsed:.1f}s, order_price: {order_price}, best_bid: {self.extended_best_bid}, best_ask: {self.extended_best_ask})")
            elif self.extended_order_status == 'FILLED':
                self.logger.info(f"Order {order_id} filled successfully")
                break
            else:
                if self.extended_order_status is not None:
                    self.logger.error(f"❌ Unknown Extended order status: {self.extended_order_status}")
                    break
                else:
                    await asyncio.sleep(0.5)

    def handle_extended_order_book_update(self, message):
        """Handle Extended order book updates from WebSocket."""
        try:
            if isinstance(message, str):
                message = json.loads(message)

            self.logger.debug(f"Received Extended order book message: {message}")

            # Check if this is an order book update message
            if message.get("type") in ["SNAPSHOT", "DELTA"]:
                data = message.get("data", {})

                if data:
                    # Handle SNAPSHOT - replace entire order book
                    if message.get("type") == "SNAPSHOT":
                        self.extended_order_book['bids'].clear()
                        self.extended_order_book['asks'].clear()

                    # Update bids - Extended format is [{"p": "price", "q": "size"}, ...]
                    bids = data.get('b', [])
                    for bid in bids:
                        if isinstance(bid, dict):
                            price = Decimal(bid.get('p', '0'))
                            size = Decimal(bid.get('q', '0'))
                        else:
                            # Fallback for array format [price, size]
                            price = Decimal(bid[0])
                            size = Decimal(bid[1])
                        
                        if size > 0:
                            self.extended_order_book['bids'][price] = size
                        else:
                            # Remove zero size orders
                            self.extended_order_book['bids'].pop(price, None)

                    # Update asks - Extended format is [{"p": "price", "q": "size"}, ...]
                    asks = data.get('a', [])
                    for ask in asks:
                        if isinstance(ask, dict):
                            price = Decimal(ask.get('p', '0'))
                            size = Decimal(ask.get('q', '0'))
                        else:
                            # Fallback for array format [price, size]
                            price = Decimal(ask[0])
                            size = Decimal(ask[1])
                        
                        if size > 0:
                            self.extended_order_book['asks'][price] = size
                        else:
                            # Remove zero size orders
                            self.extended_order_book['asks'].pop(price, None)

                    # Update best bid and ask
                    if self.extended_order_book['bids']:
                        self.extended_best_bid = max(self.extended_order_book['bids'].keys())
                    if self.extended_order_book['asks']:
                        self.extended_best_ask = min(self.extended_order_book['asks'].keys())

                    if not self.extended_order_book_ready:
                        self.extended_order_book_ready = True
                        self.logger.info(f"📊 Extended order book ready - Best bid: {self.extended_best_bid}, "
                                         f"Best ask: {self.extended_best_ask}")
                    else:
                        self.logger.debug(f"📊 Order book updated - Best bid: {self.extended_best_bid}, "
                                          f"Best ask: {self.extended_best_ask}")

        except Exception as e:
            self.logger.error(f"Error handling Extended order book update: {e}")
            self.logger.error(f"Message content: {message}")

    def handle_extended_order_update(self, order_data):
        """Handle Extended order updates from WebSocket - triggers immediate hedging."""
        side = order_data.get('side', '').lower()
        filled_size = Decimal(order_data.get('filled_size', '0'))  # This is incremental fill
        price = Decimal(order_data.get('price', '0'))

        if filled_size <= 0:
            self.logger.warning(f"⚠️ Ignoring order update with zero filled_size")
            return

        # Determine Lighter side (opposite of Extended side)
        if side == 'buy':
            lighter_side = 'sell'
        else:
            lighter_side = 'buy'

        self.logger.info(f"📋 Extended filled {filled_size}, triggering immediate Lighter hedge: {lighter_side} {filled_size} @ {price}")

        # Create async task to place Lighter order immediately (non-blocking)
        asyncio.create_task(self._execute_lighter_hedge(lighter_side, filled_size, price))

    async def _execute_lighter_hedge(self, lighter_side: str, quantity: Decimal, extended_price: Decimal):
        """Execute Lighter hedge order immediately (runs as independent task)."""
        try:
            self.logger.info(f"🔄 Starting Lighter hedge: {lighter_side} {quantity}")

            if not self.lighter_client:
                await self.initialize_lighter_client()

            # Use lock to ensure orders are sent sequentially to prevent nonce conflicts
            async with self.lighter_order_lock:
                # Calculate actual quantity considering pending hedge
                # pending_hedge_size: positive for buy, negative for sell
                if lighter_side.lower() == 'buy':
                    # For buy orders, add positive pending (subtract negative pending)
                    actual_quantity = quantity + self.lighter_pending_hedge_size
                else:
                    # For sell orders, subtract positive pending (add negative pending)
                    actual_quantity = quantity - self.lighter_pending_hedge_size

                # Check if quantity meets minimum order size
                if actual_quantity < self.lighter_min_order_size:
                    # Store as pending with correct sign
                    if lighter_side.lower() == 'buy':
                        self.lighter_pending_hedge_size += quantity
                    else:
                        self.lighter_pending_hedge_size -= quantity

                    self.logger.warning(f"⚠️ Hedge {lighter_side} {quantity} is below min {self.lighter_min_order_size}, "
                                      f"pending hedge now: {self.lighter_pending_hedge_size} "
                                      f"(will merge with opposite side)")
                    return

                # Log pending merge if exists
                pending_to_clear = self.lighter_pending_hedge_size
                if pending_to_clear != 0:
                    self.logger.info(f"📊 Merging pending hedge {pending_to_clear} with {lighter_side} {quantity} = {actual_quantity}")

                # Get current best levels
                best_bid, best_ask = self.get_lighter_best_levels()

                # Check if orderbook data is available
                if lighter_side.lower() == 'buy':
                    if best_ask is None or len(best_ask) == 0:
                        self.logger.error(f"❌ Cannot execute Lighter hedge: best_ask is None or empty (orderbook not ready)")
                        self.logger.warning(f"🔄 Triggering Lighter WebSocket reconnection...")

                        # Trigger WebSocket reconnection
                        await self.reconnect_lighter_websocket()

                        # Wait for orderbook to reload (max 10 seconds)
                        for i in range(20):
                            await asyncio.sleep(0.5)
                            best_bid, best_ask = self.get_lighter_best_levels()
                            if best_ask is not None and len(best_ask) > 0:
                                self.logger.info(f"✅ Lighter orderbook restored after reconnection")
                                break
                        else:
                            self.logger.error(f"❌ Lighter orderbook still not ready after reconnection, skipping hedge")
                            return

                    is_ask = False
                    price = best_ask[0] * Decimal('1.005')  # 0.5% above ask for quick fill
                else:
                    if best_bid is None or len(best_bid) == 0:
                        self.logger.error(f"❌ Cannot execute Lighter hedge: best_bid is None or empty (orderbook not ready)")
                        self.logger.warning(f"🔄 Triggering Lighter WebSocket reconnection...")

                        # Trigger WebSocket reconnection
                        await self.reconnect_lighter_websocket()

                        # Wait for orderbook to reload (max 10 seconds)
                        for i in range(20):
                            await asyncio.sleep(0.5)
                            best_bid, best_ask = self.get_lighter_best_levels()
                            if best_bid is not None and len(best_bid) > 0:
                                self.logger.info(f"✅ Lighter orderbook restored after reconnection")
                                break
                        else:
                            self.logger.error(f"❌ Lighter orderbook still not ready after reconnection, skipping hedge")
                            return

                    is_ask = True
                    price = best_bid[0] * Decimal('0.995')  # 0.5% below bid for quick fill

                self.logger.info(f"🎯 Lighter hedge order: {lighter_side} {actual_quantity} @ {price} (Extended filled @ {extended_price})")

                # Place the order
                # Use milliseconds (not microseconds) to stay within Lighter's limit of 281474976710655
                client_order_index = int(time.time() * 1000)

                tx_info, error = self.lighter_client.sign_create_order(
                    market_index=self.lighter_market_index,
                    client_order_index=client_order_index,
                    base_amount=int(actual_quantity * self.base_amount_multiplier),
                    price=int(price * self.price_multiplier),
                    is_ask=is_ask,
                    order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                    time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    reduce_only=False,
                    trigger_price=0,
                )

                if error is not None:
                    raise Exception(f"Sign error: {error}")

                tx_hash = await self.lighter_client.send_tx(
                    tx_type=self.lighter_client.TX_TYPE_CREATE_ORDER,
                    tx_info=tx_info
                )

                self.logger.info(f"✅ Lighter hedge order placed: {lighter_side} {actual_quantity} @ {price}, tx_hash: {tx_hash}")

                # Clear pending only after successful order placement
                self.lighter_pending_hedge_size = Decimal('0')

                # Small delay to ensure nonce is updated on server before next order
                await asyncio.sleep(0.1)

                # Store actual_quantity for monitoring
                placed_quantity = actual_quantity

            # Monitor the order (with timeout) - outside lock so monitoring can happen in parallel
            await self._monitor_lighter_hedge(client_order_index, placed_quantity, lighter_side, timeout=30)

        except Exception as e:
            self.logger.error(f"❌ Error executing Lighter hedge: {e}")
            self.logger.error(f"❌ Traceback: {traceback.format_exc()}")

    async def _monitor_lighter_hedge(self, client_order_index: int, expected_quantity: Decimal, side: str, timeout: int = 30):
        """Monitor a specific Lighter hedge order."""
        self.logger.info(f"🔍 Monitoring Lighter hedge order {client_order_index}")
        start_time = time.time()

        # TODO: Implement proper order monitoring via WebSocket
        # For now, just wait with timeout
        await asyncio.sleep(1)  # Give it time to fill

        elapsed = time.time() - start_time
        if elapsed > timeout:
            self.logger.warning(f"⚠️ Lighter hedge order {client_order_index} monitoring timeout after {elapsed:.1f}s")
        else:
            self.logger.info(f"✅ Lighter hedge order {client_order_index} completed in {elapsed:.1f}s")

    async def get_lighter_position(self) -> Decimal:
        """Get Lighter account position for the current market."""
        try:
            # Use AccountApi to fetch positions
            account_api = lighter.AccountApi(self.lighter_api_client)
            account_data = await account_api.account(by="index", value=str(self.account_index))

            if not account_data or not account_data.accounts:
                self.logger.warning("⚠️ Failed to get Lighter account data")
                return Decimal('0')

            # Get positions from first account
            positions = account_data.accounts[0].positions

            # Find position for current market
            for position in positions:
                if position.market_id == self.lighter_market_index:
                    # position.sign: 1 for Long, -1 for Short
                    # position.position: absolute value of position size
                    position_size = Decimal(position.position)
                    position_sign = int(position.sign)

                    # Calculate signed position: positive for long, negative for short
                    signed_position = position_size * position_sign

                    self.logger.info(f"📊 Lighter position: size={position_size}, sign={position_sign}, signed={signed_position}")
                    return signed_position

            # No position found for this market
            return Decimal('0')

        except Exception as e:
            self.logger.error(f"❌ Error fetching Lighter position: {e}")
            return Decimal('0')

    async def verify_positions_balanced(self, tolerance: Decimal = Decimal('0.001')) -> Tuple[bool, Decimal, Decimal, Decimal]:
        """
        Verify that positions are balanced across both exchanges.

        Returns:
            Tuple of (is_balanced, extended_position, lighter_position, delta)
        """
        self.logger.info("🔍 Verifying position balance across exchanges...")

        try:
            # Fetch real positions from both exchanges
            extended_real_position = await self.extended_client.get_account_positions()
            lighter_real_position = await self.get_lighter_position()

            # Calculate delta
            delta = extended_real_position + lighter_real_position

            self.logger.info(
                f"📊 Position Check:\n"
                f"   Extended (real): {extended_real_position}\n"
                f"   Lighter (real):  {lighter_real_position}\n"
                f"   Delta:           {delta}\n"
                f"   Local tracking - Extended: {self.extended_position}, Lighter: {self.lighter_position}"
            )

            # Check if balanced (within tolerance)
            is_balanced = abs(delta) <= tolerance

            if is_balanced:
                self.logger.info(f"✅ Positions are balanced (delta: {delta} within tolerance: {tolerance})")
            else:
                self.logger.warning(f"⚠️ Positions are IMBALANCED! Delta: {delta} exceeds tolerance: {tolerance}")

            # Update local tracking with real positions
            self.extended_position = extended_real_position
            self.lighter_position = lighter_real_position

            return is_balanced, extended_real_position, lighter_real_position, delta

        except Exception as e:
            self.logger.error(f"❌ Error verifying positions: {e}")
            self.logger.error(f"❌ Traceback: {traceback.format_exc()}")
            return False, Decimal('0'), Decimal('0'), Decimal('0')

    async def place_lighter_market_order(self, lighter_side: str, quantity: Decimal, price: Decimal):
        if not self.lighter_client:
            await self.initialize_lighter_client()

        best_bid, best_ask = self.get_lighter_best_levels()

        # Determine order parameters
        if lighter_side.lower() == 'buy':
            is_ask = False
            price = best_ask[0] * Decimal('1.002')
        else:
            is_ask = True
            price = best_bid[0] * Decimal('0.998')

        self.logger.info(f"Placing Lighter market order: {lighter_side} {quantity} | is_ask: {is_ask}")

        # Reset order state
        self.lighter_order_filled = False
        self.lighter_order_price = price
        self.lighter_order_side = lighter_side
        self.lighter_order_size = quantity

        try:
            client_order_index = int(time.time() * 1000)
            # Sign the order transaction
            tx_info, error = self.lighter_client.sign_create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,  # 优化：这里为对冲单，用市价单是不是更好？
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"Sign error: {error}")

            # Prepare the form data
            tx_hash = await self.lighter_client.send_tx(
                tx_type=self.lighter_client.TX_TYPE_CREATE_ORDER,
                tx_info=tx_info
            )
            self.logger.info(f"🚀 Lighter limit order sent: {lighter_side} {quantity}")
            await self.monitor_lighter_order(client_order_index)

            return tx_hash
        except Exception as e:
            self.logger.error(f"❌ Error placing Lighter order: {e}")
            return None

    async def monitor_lighter_order(self, client_order_index: int):
        """Monitor Lighter order and adjust price if needed."""
        self.logger.info(f"🔍 Starting to monitor Lighter order - Order ID: {client_order_index}")

        start_time = time.time()
        while not self.lighter_order_filled and not self.stop_flag:
            # Check for timeout (30 seconds total)
            if time.time() - start_time > 30:
                self.logger.error(f"❌ Timeout waiting for Lighter order fill after {time.time() - start_time:.1f}s")
                self.logger.error(f"❌ Order state - Filled: {self.lighter_order_filled}")

                # Fallback: Mark as filled to continue trading
                self.logger.warning("⚠️ Using fallback - marking order as filled to continue trading")
                self.lighter_order_filled = True  # 疑问：这里超时直接修改状态，肯定是不对的。应该撤销 ligter 单，重新下一个新的单子
                self.waiting_for_lighter_fill = False
                self.order_execution_complete = True
                break

            await asyncio.sleep(0.1)  # Check every 100ms

    async def modify_lighter_order(self, client_order_index: int, new_price: Decimal):
        """Modify current Lighter order with new price using client_order_index."""
        try:
            if client_order_index is None:
                self.logger.error("❌ Cannot modify order - no order ID available")
                return

            # Calculate new Lighter price
            lighter_price = int(new_price * self.price_multiplier)

            self.logger.info(f"🔧 Attempting to modify order - Market: {self.lighter_market_index}, "
                             f"Client Order Index: {client_order_index}, New Price: {lighter_price}")

            # Use the native SignerClient's modify_order method
            tx_info, tx_hash, error = await self.lighter_client.modify_order(
                market_index=self.lighter_market_index,
                order_index=client_order_index,  # Use client_order_index directly
                base_amount=int(self.lighter_order_size * self.base_amount_multiplier),
                price=lighter_price,
                trigger_price=0
            )

            if error is not None:
                self.logger.error(f"❌ Lighter order modification error: {error}")
                return

            self.lighter_order_price = new_price
            self.logger.info(f"🔄 Lighter order modified successfully: {self.lighter_order_side} "
                             f"{self.lighter_order_size} @ {new_price}")

        except Exception as e:
            self.logger.error(f"❌ Error modifying Lighter order: {e}")
            import traceback
            self.logger.error(f"❌ Full traceback: {traceback.format_exc()}")

    async def setup_extended_websocket(self):
        """Setup Extended websocket for order updates and order book data."""
        if not self.extended_client:
            raise Exception("Extended client not initialized")

        def order_update_handler(order_data):
            """Handle order updates from Extended WebSocket."""
            if order_data.get('contract_id') != self.extended_contract_id:
                self.logger.info(f"Ignoring order update from {order_data.get('contract_id')}")
                return

            try:
                order_id = order_data.get('order_id')
                status = order_data.get('status')
                side = order_data.get('side', '').lower()
                cumulative_filled = Decimal(order_data.get('filled_size', '0'))  # Cumulative filled from exchange
                size = Decimal(order_data.get('size', '0'))
                price = order_data.get('price', '0')

                if side == 'buy':
                    order_type = "OPEN"
                else:
                    order_type = "CLOSE"

                # Calculate incremental fill for this update
                if order_id != self.current_extended_order_id:
                    # New order - reset tracking
                    self.current_extended_order_id = order_id
                    self.extended_order_hedged_size = Decimal('0')
                    incremental_filled = cumulative_filled
                    self.logger.info(f"🆕 New Extended order detected: {order_id}")
                else:
                    # Same order - calculate increment
                    incremental_filled = cumulative_filled - self.extended_order_hedged_size

                # Handle fills (both PARTIALLY_FILLED and FILLED)
                if incremental_filled > 0 and status in ['PARTIALLY_FILLED', 'FILLED']:
                    # Update Extended position (only the increment)
                    if side == 'buy':
                        self.extended_position += incremental_filled
                    else:
                        self.extended_position -= incremental_filled

                    self.logger.info(
                        f"[{order_id}] [{order_type}] [Extended] [{status}]: "
                        f"Incremental Fill {incremental_filled} (Cumulative: {cumulative_filled}/{size}) @ {price}"
                    )

                    # Log Extended trade to CSV (incremental only)
                    self.log_trade_to_csv(
                        exchange='Extended',
                        side=side,
                        price=str(price),
                        quantity=str(incremental_filled)
                    )

                    # Trigger immediate Lighter hedge (incremental only)
                    self.handle_extended_order_update({
                        'order_id': order_id,
                        'side': side,
                        'status': status,
                        'size': size,
                        'price': price,
                        'contract_id': self.extended_contract_id,
                        'filled_size': incremental_filled  # Pass incremental fill!
                    })

                    # Update hedged size tracker
                    self.extended_order_hedged_size = cumulative_filled

                    # Update status after position change
                    self.update_status()

                # Update order status
                if status == 'FILLED':
                    self.extended_order_status = status
                    self.logger.info(f"✅ Extended order {order_id} fully filled: {cumulative_filled}/{size}")
                elif status == 'PARTIALLY_FILLED':
                    self.extended_order_status = "OPEN"
                elif status in ['CANCELED', 'CANCELLED']:
                    self.extended_order_status = status
                    if self.extended_order_hedged_size < cumulative_filled:
                        self.logger.warning(
                            f"⚠️ Order {order_id} cancelled with partial fill: "
                            f"Filled {cumulative_filled}, Hedged {self.extended_order_hedged_size}"
                        )
                elif status in ['NEW', 'OPEN', 'PENDING', 'CANCELING']:
                    self.extended_order_status = status
                else:
                    self.logger.warning(f"Unknown order status: {status}")
                    self.extended_order_status = status

            except Exception as e:
                self.logger.error(f"Error handling Extended order update: {e}")

        try:
            # Setup order update handler
            self.extended_client.setup_order_update_handler(order_update_handler)
            self.logger.info("✅ Extended WebSocket order update handler set up")

            # Connect to Extended WebSocket
            await self.extended_client.connect()
            self.logger.info("✅ Extended WebSocket connection established")

            # Setup separate WebSocket connection for depth updates
            await self.setup_extended_depth_websocket()

        except Exception as e:
            self.logger.error(f"Could not setup Extended WebSocket handlers: {e}")

    async def setup_extended_depth_websocket(self):
        """Setup separate WebSocket connection for Extended depth updates."""
        try:
            import websockets

            async def handle_depth_websocket():
                """Handle depth WebSocket connection."""
                # Use the correct Extended WebSocket URL for order book stream
                market_name = f"{self.ticker}-USD"  # Extended uses format like BTC-USD
                url = f"wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks/{market_name}?depth=1"

                while not self.stop_flag:
                    try:
                        async with websockets.connect(url) as ws:
                            self.logger.info(f"✅ Connected to Extended order book stream for {market_name}")

                            # Listen for messages
                            async for message in ws:
                                if self.stop_flag:
                                    break

                                try:
                                    # Handle ping frames
                                    if isinstance(message, bytes) and message == b'\x09':
                                        await ws.pong()
                                        continue

                                    data = json.loads(message)
                                    self.logger.debug(f"Received Extended order book message: {data}")

                                    # Handle order book updates
                                    if data.get("type") in ["SNAPSHOT", "DELTA"]:
                                        self.handle_extended_order_book_update(data)

                                except json.JSONDecodeError as e:
                                    self.logger.warning(f"Failed to parse Extended order book message: {e}")
                                except Exception as e:
                                    self.logger.error(f"Error handling Extended order book message: {e}")

                    except websockets.exceptions.ConnectionClosed:
                        self.logger.warning("Extended order book WebSocket connection closed, reconnecting...")
                    except Exception as e:
                        self.logger.error(f"Extended order book WebSocket error: {e}")

                    # Wait before reconnecting
                    if not self.stop_flag:
                        await asyncio.sleep(2)

            # Start depth WebSocket in background
            asyncio.create_task(handle_depth_websocket())
            self.logger.info("✅ Extended order book WebSocket task started")

        except Exception as e:
            self.logger.error(f"Could not setup Extended order book WebSocket: {e}")

    async def trading_loop(self):
        """Main trading loop implementing the new strategy."""
        self.logger.info(f"🚀 Starting hedge bot for {self.ticker}")

        # Initialize clients
        try:
            self.logger.info(f"lighter init")
            self.initialize_lighter_client()
            self.logger.info(f"extended init")
            self.initialize_extended_client()

            # Get contract info
            self.extended_contract_id, self.extended_tick_size = await self.get_extended_contract_info()
            self.lighter_market_index, self.base_amount_multiplier, self.price_multiplier, self.tick_size, self.lighter_min_order_size = self.get_lighter_market_config()

            self.logger.info(f"Contract info loaded - Extended: {self.extended_contract_id}, "
                             f"Lighter: {self.lighter_market_index}, Min order size: {self.lighter_min_order_size}")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            return

        # Setup Extended websocket
        try:
            await self.setup_extended_websocket()
            self.logger.info("✅ Extended WebSocket connection established")

            # Wait for initial order book data with timeout
            self.logger.info("⏳ Waiting for initial order book data...")
            timeout = 10  # seconds
            start_time = time.time()
            while not self.extended_order_book_ready and not self.stop_flag: # wait for order book ready max 10s
                if time.time() - start_time > timeout:
                    self.logger.warning(f"⚠️ Timeout waiting for WebSocket order book data after {timeout}s")
                    break
                await asyncio.sleep(0.5)

            if self.extended_order_book_ready:
                self.logger.info("✅ WebSocket order book data received")
            else:
                self.logger.warning("⚠️ WebSocket order book not ready, will use REST API fallback")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Extended websocket: {e}")
            return

        # Setup Lighter websocket
        try:
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            self.logger.info("✅ Lighter WebSocket task started")

            # Wait for initial Lighter order book data with timeout
            self.logger.info("⏳ Waiting for initial Lighter order book data...")
            timeout = 10  # seconds
            start_time = time.time()
            while not self.lighter_order_book_ready and not self.stop_flag:
                if time.time() - start_time > timeout:
                    self.logger.warning(f"⚠️ Timeout waiting for Lighter WebSocket order book data after {timeout}s")
                    break
                await asyncio.sleep(0.5)

            if self.lighter_order_book_ready:
                self.logger.info("✅ Lighter WebSocket order book data received")
            else:
                self.logger.error("❌ Lighter WebSocket order book not ready - cannot proceed")
                raise Exception("Failed to initialize Lighter order book within timeout period")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Lighter websocket: {e}")
            return

        await asyncio.sleep(5)

        # Send start notification
        start_msg = (
            f"🚀 <b>Hedge Bot Started</b>\n\n"
            f"Ticker: <code>{self.ticker}</code>\n"
            f"Order Quantity: <code>{self.order_quantity}</code>\n"
            f"Total Iterations: <b>{self.iterations}</b>\n"
            f"Extended Contract: <code>{self.extended_contract_id}</code>\n"
        )
        self.send_telegram_notification(start_msg)

        iterations = 0
        while iterations < self.iterations and not self.stop_flag:
            iterations += 1
            self.current_iteration = iterations
            self.logger.info("-----------------------------------------------")
            self.logger.info(f"🔄 Trading loop iteration {iterations}")
            self.logger.info("-----------------------------------------------")

            # Update status file
            self.update_status()

            # 疑问：每一轮开始之前，A、B 两个交易所的持仓都应该为 0 才对？如果不对 0，应该先进入一个清理的逻辑？
            self.logger.info(f"[STEP 1] Extended position: {self.extended_position} | Lighter position: {self.lighter_position}")

            if abs(self.extended_position + self.lighter_position) > self.order_quantity*2:
                self.logger.error(f"❌ Position diff is too large: {self.extended_position + self.lighter_position}")
                break

            try:
                # Determine side based on some logic (for now, alternate)
                # 对冲交易模式，第一个交易所都是买入方向，这里可以改为可以参数设置
                side = 'buy'
                self.logger.info(f"[STEP 1] Placing Extended {side} order for {self.order_quantity}")

                # Place Extended order - hedging will happen automatically via WebSocket
                await self.place_extended_post_only_order(side, self.order_quantity)

                self.logger.info(f"✅ [STEP 1] Extended order completed (Lighter hedges executed automatically)")
            except Exception as e:
                error_msg = (
                    f"❌ <b>Hedge Bot Error - STEP 1</b>\n\n"
                    f"Ticker: <code>{self.ticker}</code>\n"
                    f"Iteration: <b>{iterations}/{self.iterations}</b>\n"
                    f"Error: <code>{str(e)}</code>\n\n"
                    f"Extended Position: <code>{self.extended_position}</code>\n"
                    f"Lighter Position: <code>{self.lighter_position}</code>"
                )
                self.send_telegram_notification(error_msg)
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                break

            if self.stop_flag:
                break

            # Wait and verify positions are balanced after STEP 1
            self.logger.info("=" * 60)
            self.logger.info("🔍 [POST-STEP 1 VERIFICATION] Checking position balance...")
            self.logger.info("=" * 60)

            # Retry position verification with delays to allow for settlement
            max_retries = 10
            retry_delay = 2  # seconds
            is_balanced = False

            for attempt in range(1, max_retries + 1):
                self.logger.info(f"⏳ Verification attempt {attempt}/{max_retries}, waiting {retry_delay}s for settlement...")
                await asyncio.sleep(retry_delay)

                is_balanced, ext_pos, ltr_pos, delta = await self.verify_positions_balanced(
                    tolerance=Decimal('0.005')  # Allow 0.005 tolerance
                )

                if is_balanced:
                    self.logger.info(f"✅ Positions balanced on attempt {attempt}")
                    break
                else:
                    self.logger.warning(f"⚠️ Attempt {attempt}: Still imbalanced (delta={delta})")

            if not is_balanced:
                error_msg = (
                    f"❌ <b>Hedge Bot Critical Error</b>\n\n"
                    f"Ticker: <code>{self.ticker}</code>\n"
                    f"Iteration: <b>{iterations}/{self.iterations}</b>\n\n"
                    f"<b>Position Imbalance Detected!</b>\n"
                    f"Extended: <code>{ext_pos}</code>\n"
                    f"Lighter: <code>{ltr_pos}</code>\n"
                    f"Delta: <code>{delta}</code>\n"
                    f"Tolerance: <code>0.005</code>\n\n"
                    f"⚠️ Trading terminated for safety"
                )
                self.send_telegram_notification(error_msg)
                self.logger.error(f"❌ CRITICAL: Positions are imbalanced after STEP 1!")
                self.logger.error(f"❌ Extended: {ext_pos}, Lighter: {ltr_pos}, Delta: {delta}")
                self.logger.error(f"❌ Expected balanced positions (delta ≤ 0.005)")
                self.logger.error(f"❌ Terminating trading loop for safety")
                raise Exception(f"Position imbalance detected: delta={delta}, exceeds tolerance=0.005")

            self.logger.info("=" * 60)

            # Send progress notification after successful position verification
            # Position data here is most accurate after balance check
            self.check_and_notify_progress()

            # Sleep after step 1
            if self.sleep_time > 0:
                self.logger.info(f"💤 Sleeping {self.sleep_time} seconds after STEP 1...")
                await asyncio.sleep(self.sleep_time)

            # Close position
            self.logger.info(f"[STEP 2] Extended position: {self.extended_position} | Lighter position: {self.lighter_position}")
            try:
                # Determine side based on some logic (for now, alternate)
                side = 'sell'
                self.logger.info(f"[STEP 2] Placing Extended {side} order for {self.order_quantity}")

                # Place Extended order - hedging will happen automatically via WebSocket
                await self.place_extended_post_only_order(side, self.order_quantity)

                self.logger.info(f"✅ [STEP 2] Extended order completed (Lighter hedges executed automatically)")
            except Exception as e:
                error_msg = (
                    f"❌ <b>Hedge Bot Error - STEP 2</b>\n\n"
                    f"Ticker: <code>{self.ticker}</code>\n"
                    f"Iteration: <b>{iterations}/{self.iterations}</b>\n"
                    f"Error: <code>{str(e)}</code>\n\n"
                    f"Extended Position: <code>{self.extended_position}</code>\n"
                    f"Lighter Position: <code>{self.lighter_position}</code>"
                )
                self.send_telegram_notification(error_msg)
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                break

            # Close remaining position (if any)
            # 优化：第二步结束后，仓位应该为 0，用查询订单的方式去检查 position，如果有不为 0，则进行平仓操作
            self.logger.info(f"[STEP 3] Extended position: {self.extended_position} | Lighter position: {self.lighter_position}")
            if self.extended_position == 0:
                self.logger.info("✅ [STEP 3] No remaining position to close")
                continue

            # Check if remaining position is below minimum order size
            remaining_position = abs(self.extended_position)
            extended_min_size = getattr(self.extended_client, 'min_order_size', Decimal('0.01'))  # Default to 0.01 if not set

            if remaining_position < extended_min_size:
                self.logger.info(
                    f"⚠️ [STEP 3] Remaining position {remaining_position} is below minimum order size {extended_min_size}, "
                    f"skipping close order. Position will be ignored."
                )
                self.logger.info(
                    f"📊 Final residual - Extended: {self.extended_position}, Lighter: {self.lighter_position}, "
                    f"Delta: {self.extended_position + self.lighter_position}"
                )
                continue

            if self.extended_position > 0:
                side = 'sell'
            else:
                side = 'buy'

            try:
                self.logger.info(f"[STEP 3] Closing remaining Extended position: {side} {remaining_position}")

                # Place Extended order - hedging will happen automatically via WebSocket
                await self.place_extended_post_only_order(side, remaining_position)

                self.logger.info(f"✅ [STEP 3] Remaining position closed (Lighter hedges executed automatically)")
            except Exception as e:
                error_msg = (
                    f"❌ <b>Hedge Bot Error - STEP 3</b>\n\n"
                    f"Ticker: <code>{self.ticker}</code>\n"
                    f"Iteration: <b>{iterations}/{self.iterations}</b>\n"
                    f"Error: <code>{str(e)}</code>\n\n"
                    f"Extended Position: <code>{self.extended_position}</code>\n"
                    f"Lighter Position: <code>{self.lighter_position}</code>"
                )
                self.send_telegram_notification(error_msg)
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                break

        # Send completion notification if loop finished successfully
        if iterations >= self.iterations:
            completion_msg = (
                f"✅ <b>Hedge Bot Completed</b>\n\n"
                f"Ticker: <code>{self.ticker}</code>\n"
                f"Completed: <b>{iterations}/{self.iterations}</b> iterations\n"
                f"Final Extended Position: <code>{self.extended_position}</code>\n"
                f"Final Lighter Position: <code>{self.lighter_position}</code>\n\n"
                f"🎉 All iterations finished successfully!"
            )
            self.send_telegram_notification(completion_msg)

    async def run(self):
        """Run the hedge bot."""
        self.setup_signal_handlers()

        # Start async logger
        await self.hedge_logger.start()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        finally:
            self.logger.info("🔄 Cleaning up...")

            # Send final notification with current position info
            try:
                # Fetch final positions from both exchanges
                final_extended_pos = Decimal('0')
                final_lighter_pos = Decimal('0')

                if self.extended_client:
                    try:
                        final_extended_pos = await self.extended_client.get_account_positions()
                    except Exception as e:
                        self.logger.error(f"Failed to get final Extended position: {e}")

                if self.lighter_client:
                    try:
                        final_lighter_pos = await self.get_lighter_position()
                    except Exception as e:
                        self.logger.error(f"Failed to get final Lighter position: {e}")

                # Calculate runtime
                runtime = datetime.now() - self.start_time
                runtime_str = str(runtime).split('.')[0]  # Remove microseconds

                # Calculate completion percentage
                completion_pct = int((self.current_iteration / self.iterations * 100)) if self.iterations > 0 else 0

                # Send final notification
                final_msg = (
                    f"🏁 <b>Hedge Bot Shutdown</b>\n\n"
                    f"Ticker: <code>{self.ticker}</code>\n"
                    f"Completed: <b>{self.current_iteration}/{self.iterations}</b> ({completion_pct}%)\n"
                    f"Runtime: <code>{runtime_str}</code>\n\n"
                    f"<b>Final Positions:</b>\n"
                    f"Extended: <code>{final_extended_pos}</code>\n"
                    f"Lighter: <code>{final_lighter_pos}</code>\n"
                    f"Delta: <code>{final_extended_pos + final_lighter_pos}</code>"
                )

                self.send_telegram_notification(final_msg)
                self.logger.info("📱 Final shutdown notification sent")

            except Exception as e:
                self.logger.error(f"Failed to send final notification: {e}")

            await self.async_shutdown()


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Trading bot for Extended and Lighter')
    parser.add_argument('--exchange', type=str,
                        help='Exchange')
    parser.add_argument('--ticker', type=str, default='BTC',
                        help='Ticker symbol (default: BTC)')
    parser.add_argument('--size', type=str,
                        help='Number of tokens to buy/sell per order')
    parser.add_argument('--iter', type=int,
                        help='Number of iterations to run')
    parser.add_argument('--fill-timeout', type=int, default=5,
                        help='Timeout in seconds for maker order fills (default: 5)')
    parser.add_argument('--sleep', type=int, default=0,
                        help='Sleep time in seconds after each step (default: 0)')

    return parser.parse_args()
