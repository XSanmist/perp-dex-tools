"""
Momentum Trading Bot - Follows price momentum using stop-limit orders

This bot implements a momentum trading strategy that avoids "catching falling knives"
by using conditional (stop-limit) orders that trigger only when price moves in the
desired direction. After entry, it immediately places a take-profit order at a
configurable percentage above entry price.

Key Features:
- Uses stop-limit orders instead of passive limit orders
- Follows momentum rather than mean reversion
- Immediate take-profit order placement after fill
- Multi-exchange support via ExchangeFactory
- Configurable trading parameters
- Telegram notifications for trade events
"""

import os
import time
import asyncio
import traceback
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from datetime import datetime

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.telegram_bot import TelegramBot


@dataclass
class MomentumConfig:
    """Configuration for momentum trading bot."""

    # Market configuration
    ticker: str                      # e.g., "ETH"
    exchange: str                    # Exchange name (e.g., "extended")

    # Order size
    quantity: Decimal                # Order size per trade

    # Entry strategy
    direction: str                   # "buy" or "sell" - trading direction
    tick_offset: int                 # Number of ticks to offset from best price (e.g., 1 means 1 tick away)

    # Exit strategy (take-profit)
    take_profit_pct: Decimal        # Take profit percentage (e.g., 0.02 for 0.02%)

    # Moving grid strategy
    enable_grid: bool = True        # Enable moving grid (add positions when price moves against us)
    grid_trigger_pct: Decimal = Decimal('0.05')  # Trigger new grid entry when price moves 0.05% against position

    # Trading controls
    max_positions: int = 10         # Maximum concurrent grid positions
    wait_time: int = 5              # Wait time between checks (seconds)

    # Auto-filled fields (will be set during initialization)
    contract_id: Optional[str] = None       # Will be fetched from exchange
    tick_size: Optional[Decimal] = None     # Will be fetched from exchange

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'sell' if self.direction == "buy" else 'buy'


@dataclass
class GridPosition:
    """Track individual grid position."""
    entry_order_id: str
    entry_price: Decimal
    entry_quantity: Decimal
    exit_order_id: Optional[str] = None
    is_filled: bool = False
    exit_filled: bool = False


@dataclass
class PositionState:
    """Track all open grid positions."""
    positions: list = None  # List of GridPosition
    is_position_open: bool = False
    lowest_entry_price: Optional[Decimal] = None  # For long: track lowest entry
    highest_entry_price: Optional[Decimal] = None  # For short: track highest entry

    def __post_init__(self):
        if self.positions is None:
            self.positions = []

    def reset(self):
        """Reset all position state."""
        self.positions = []
        self.is_position_open = False
        self.lowest_entry_price = None
        self.highest_entry_price = None

    def add_position(self, entry_order_id: str, entry_price: Decimal, entry_quantity: Decimal):
        """Add a new grid position."""
        grid_pos = GridPosition(
            entry_order_id=entry_order_id,
            entry_price=entry_price,
            entry_quantity=entry_quantity
        )
        self.positions.append(grid_pos)
        return grid_pos

    def get_position_by_entry_order_id(self, order_id: str) -> Optional[GridPosition]:
        """Find position by entry order ID."""
        for pos in self.positions:
            if pos.entry_order_id == order_id:
                return pos
        return None

    def get_position_by_exit_order_id(self, order_id: str) -> Optional[GridPosition]:
        """Find position by exit order ID."""
        for pos in self.positions:
            if pos.exit_order_id == order_id:
                return pos
        return None

    def get_total_quantity(self) -> Decimal:
        """Get total quantity across all filled positions."""
        return sum(pos.entry_quantity for pos in self.positions if pos.is_filled and not pos.exit_filled)

    def get_average_entry_price(self) -> Optional[Decimal]:
        """Calculate weighted average entry price."""
        filled_positions = [pos for pos in self.positions if pos.is_filled and not pos.exit_filled]
        if not filled_positions:
            return None

        total_cost = sum(pos.entry_price * pos.entry_quantity for pos in filled_positions)
        total_quantity = sum(pos.entry_quantity for pos in filled_positions)

        if total_quantity == 0:
            return None

        return total_cost / total_quantity

    def update_extreme_price(self, entry_price: Decimal, direction: str):
        """Update lowest/highest entry price for grid tracking."""
        if direction == "buy":
            if self.lowest_entry_price is None or entry_price < self.lowest_entry_price:
                self.lowest_entry_price = entry_price
        else:  # sell
            if self.highest_entry_price is None or entry_price > self.highest_entry_price:
                self.highest_entry_price = entry_price


class MomentumBot:
    """
    Momentum Trading Bot - Main trading logic

    This bot uses stop-limit orders to enter positions only when price moves in the
    desired direction (momentum), then immediately places take-profit orders.

    Flow:
    1. Place stop-limit order at configured stop_price/limit_price
    2. Monitor order via WebSocket for fill
    3. On fill: immediately place take-profit limit order
    4. Monitor take-profit order until filled
    5. Repeat cycle
    """

    def __init__(self, config: MomentumConfig):
        """Initialize momentum trading bot."""
        self.config = config
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # Create exchange client using factory pattern
        try:
            self.exchange_client = ExchangeFactory.create_exchange(
                config.exchange,
                config
            )
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # Telegram configuration
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')

        # Position tracking
        self.position = PositionState()

        # Trading state
        self.shutdown_requested = False
        self.loop = None

        # Market info (will be initialized)
        self.market_initialized = False

        # Events for async coordination
        self.entry_filled_event = asyncio.Event()
        self.exit_filled_event = asyncio.Event()

        # Entry order monitoring state (for current order being placed)
        self.current_entry_order_id: Optional[str] = None
        self.entry_order_status: Optional[str] = None
        self.entry_order_price: Optional[Decimal] = None

        # Setup WebSocket handlers
        self._setup_websocket_handlers()

    async def initialize_market_info(self):
        """Initialize market information from exchange."""
        if self.market_initialized:
            return

        self.logger.log("📊 Initializing market information...", "INFO")

        # Call get_contract_attributes() - this is the standard way
        # All exchanges should implement this method
        contract_id, tick_size = await self.exchange_client.get_contract_attributes()

        self.config.contract_id = contract_id
        self.config.tick_size = tick_size

        self.logger.log(f"✅ Contract ID: {self.config.contract_id}", "INFO")
        self.logger.log(f"✅ Tick Size: {self.config.tick_size}", "INFO")

        self.market_initialized = True
        self.logger.log("✅ Market information initialized", "INFO")

    async def get_best_price(self) -> Decimal:
        """
        Get the best price from the market.

        For buy orders: get best ask (lowest sell price)
        For sell orders: get best bid (highest buy price)

        Returns:
            Decimal: Best price in the market
        """
        try:
            # Use the exchange client's fetch_bbo_prices method
            if hasattr(self.exchange_client, 'fetch_bbo_prices'):
                best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)

                if self.config.direction == "buy":
                    # For buy, we want the best ask (lowest sell price)
                    if best_ask > 0:
                        self.logger.log(f"📊 Best ask: {best_ask}", "INFO")
                        return best_ask
                else:
                    # For sell, we want the best bid (highest buy price)
                    if best_bid > 0:
                        self.logger.log(f"📊 Best bid: {best_bid}", "INFO")
                        return best_bid

            raise ValueError("Unable to get best price from orderbook")

        except Exception as e:
            self.logger.log(f"❌ Error getting best price: {e}", "ERROR")
            raise

    def calculate_entry_prices(self, best_price: Decimal) -> tuple[Decimal, Decimal]:
        """
        Calculate stop price and limit price based on best price and tick offset.

        For buy orders:
            - stop_price = best_price + (tick_offset * tick_size)
            - limit_price = stop_price (same as stop price)

        For sell orders:
            - stop_price = best_price - (tick_offset * tick_size)
            - limit_price = stop_price (same as stop price)

        Args:
            best_price: Current best price in the market

        Returns:
            tuple: (stop_price, limit_price)
        """
        # Calculate offset in price terms
        price_offset = Decimal(str(self.config.tick_offset)) * self.config.tick_size

        if self.config.direction == "buy":
            # For buy: place order tick_offset ticks ABOVE best ask
            stop_price = best_price + price_offset
        else:
            # For sell: place order tick_offset ticks BELOW best bid
            stop_price = best_price - price_offset

        # For momentum bot, limit price = stop price (we want immediate fill when triggered)
        limit_price = stop_price

        # Round to tick size (should already be aligned, but ensure it)
        stop_price = self.exchange_client.round_to_tick(stop_price)
        limit_price = self.exchange_client.round_to_tick(limit_price)

        self.logger.log(
            f"💰 Entry prices calculated: stop={stop_price}, limit={limit_price} "
            f"(offset={self.config.tick_offset} ticks = {price_offset} from best={best_price})",
            "INFO"
        )

        return stop_price, limit_price

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Check if this is for our contract
                if message.get('contract_id') != self.config.contract_id:
                    return

                order_id = message.get('order_id')
                status = message.get('status')
                side = message.get('side', '')
                filled_size = Decimal(message.get('filled_size', 0))
                price = Decimal(message.get('price', 0))

                # Log order update
                self.logger.log(
                    f"[{side.upper()}] [{order_id}] {status} "
                    f"{message.get('size')} @ {price}",
                    "INFO"
                )

                # Check if this is an entry order for current monitoring
                if order_id == self.current_entry_order_id:
                    self.entry_order_status = status

                # Handle entry order fill - find the position
                grid_pos = self.position.get_position_by_entry_order_id(order_id)
                if grid_pos and status == 'FILLED':
                    grid_pos.is_filled = True
                    grid_pos.entry_price = price
                    grid_pos.entry_quantity = filled_size
                    self.position.is_position_open = True

                    # Update extreme price tracking
                    self.position.update_extreme_price(price, self.config.direction)

                    # Signal entry filled
                    if self.loop is not None:
                        self.loop.call_soon_threadsafe(self.entry_filled_event.set)

                    self.logger.log(f"✅ Grid entry filled: {filled_size} @ {price}", "INFO")
                    self.logger.log_transaction(order_id, side, filled_size, price, status)

                    # Log current grid status
                    total_qty = self.position.get_total_quantity()
                    avg_price = self.position.get_average_entry_price()
                    self.logger.log(
                        f"📊 Grid status: {len([p for p in self.position.positions if p.is_filled and not p.exit_filled])} positions, "
                        f"total qty={total_qty}, avg price={avg_price}",
                        "INFO"
                    )

                # Handle exit order fill
                grid_pos = self.position.get_position_by_exit_order_id(order_id)
                if grid_pos and status == 'FILLED':
                    grid_pos.exit_filled = True

                    # Signal exit filled
                    if self.loop is not None:
                        self.loop.call_soon_threadsafe(self.exit_filled_event.set)

                    self.logger.log(f"✅ Grid exit filled: {filled_size} @ {price}", "INFO")
                    self.logger.log_transaction(order_id, side, filled_size, price, status)

                    # Log current grid status
                    total_qty = self.position.get_total_quantity()
                    avg_price = self.position.get_average_entry_price()
                    remaining_positions = len([p for p in self.position.positions if p.is_filled and not p.exit_filled])
                    self.logger.log(
                        f"📊 Grid status: {remaining_positions} positions remaining, "
                        f"total qty={total_qty}, avg price={avg_price}",
                        "INFO"
                    )

                    # If all positions closed, reset
                    if remaining_positions == 0:
                        self.logger.log("✅ All grid positions closed, resetting...", "INFO")
                        self.position.reset()

                # Handle partial fills
                elif status == 'PARTIALLY_FILLED':
                    self.logger.log(f"⏳ Partial fill: {filled_size} @ {price}", "INFO")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Register handler with exchange client
        self.exchange_client.setup_order_update_handler(order_update_handler)

    def send_telegram_notification(self, message: str):
        """Send notification via Telegram."""
        if not self.telegram_token or not self.telegram_chat_id:
            return

        try:
            with TelegramBot(self.telegram_token, self.telegram_chat_id) as tg_bot:
                tg_bot.send_text(message)
        except Exception as e:
            self.logger.log(f"Failed to send Telegram notification: {e}", "ERROR")

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"🛑 Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Cancel any open orders
            if self.position.entry_order_id and not self.position.is_position_open:
                await self.exchange_client.cancel_order(self.position.entry_order_id)

            # Disconnect from exchange
            await self.exchange_client.disconnect()
            self.logger.log("✅ Graceful shutdown completed", "INFO")

            # Send shutdown notification
            shutdown_msg = (
                f"🛑 <b>Momentum Bot Shutdown</b>\n\n"
                f"Ticker: <code>{self.config.ticker}</code>\n"
                f"Reason: {reason}\n"
                f"Position Open: {self.position.is_position_open}"
            )
            self.send_telegram_notification(shutdown_msg)

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")

    async def place_entry_order(self) -> bool:
        """
        Place conditional entry order with monitoring and re-placement logic.

        Flow:
        1. Get best price from market
        2. Calculate trigger price based on tick_offset
        3. Calculate take profit price
        4. Place conditional order with TP
        5. Wait 5 seconds and check if filled
        6. If not filled, check if price still optimal
        7. If not optimal, cancel and re-place order

        Returns:
            bool: True if order placed successfully, False otherwise
        """
        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Get current best price
                best_price = await self.get_best_price()

                # Calculate trigger price (entry price)
                trigger_price, _ = self.calculate_entry_prices(best_price)

                # Calculate take profit price
                if self.config.direction == "buy":
                    # For long: TP is above entry
                    tp_price = trigger_price * (Decimal('1') + self.config.take_profit_pct / Decimal('100'))
                else:
                    # For short: TP is below entry
                    tp_price = trigger_price * (Decimal('1') - self.config.take_profit_pct / Decimal('100'))

                tp_price = self.exchange_client.round_to_tick(tp_price)

                self.logger.log(
                    f"📊 Placing {self.config.direction} conditional order (attempt {retry_count + 1}/{max_retries}): "
                    f"trigger={trigger_price}, TP={tp_price}, qty={self.config.quantity}",
                    "INFO"
                )

                # Reset events and state
                self.entry_filled_event.clear()
                self.entry_order_status = None
                self.entry_order_price = trigger_price

                # Place conditional order with take profit
                if hasattr(self.exchange_client, 'place_conditional_order'):
                    order_result = await self.exchange_client.place_conditional_order(
                        self.config.contract_id,
                        self.config.quantity,
                        trigger_price,
                        self.config.direction,
                        take_profit_price=tp_price
                    )
                else:
                    # Fallback to regular order
                    self.logger.log("⚠️ Exchange doesn't support conditional orders, using regular order", "WARNING")
                    order_result = await self.exchange_client.place_open_order(
                        self.config.contract_id,
                        self.config.quantity,
                        self.config.direction
                    )

                if not order_result.success:
                    self.logger.log(f"❌ Failed to place entry order: {order_result.error_message}", "ERROR")
                    return False

                # Add new grid position
                grid_pos = self.position.add_position(
                    entry_order_id=order_result.order_id,
                    entry_price=trigger_price,
                    entry_quantity=self.config.quantity
                )

                # Track for monitoring
                self.current_entry_order_id = order_result.order_id

                self.logger.log(
                    f"✅ Entry order placed: {order_result.order_id} "
                    f"(trigger={trigger_price}, TP={tp_price})",
                    "INFO"
                )

                # Wait 5 seconds
                self.logger.log("⏳ Waiting 5 seconds to check order status...", "INFO")
                await asyncio.sleep(5)

                # Check if order filled (via WebSocket update)
                if self.entry_order_status == 'FILLED':
                    self.logger.log(f"✅ Entry order filled within 5 seconds", "INFO")
                    return True

                # Order not filled, check if price still optimal
                current_best_price = await self.get_best_price()
                current_optimal_price, _ = self.calculate_entry_prices(current_best_price)

                if self.entry_order_price != current_optimal_price:
                    # Price changed, need to cancel and re-place
                    self.logger.log(
                        f"⚠️ Price changed: order trigger={self.entry_order_price}, "
                        f"current optimal={current_optimal_price}. Cancelling and re-placing...",
                        "WARNING"
                    )

                    # Cancel old order
                    try:
                        cancel_result = await self.exchange_client.cancel_order(self.current_entry_order_id)
                        if cancel_result.success:
                            self.logger.log(f"✅ Order {self.current_entry_order_id} cancelled", "INFO")

                            # Remove the cancelled grid position from list
                            self.position.positions = [
                                pos for pos in self.position.positions
                                if pos.entry_order_id != self.current_entry_order_id
                            ]
                        else:
                            self.logger.log(f"⚠️ Failed to cancel order: {cancel_result.error_message}", "WARNING")

                            # Check if order filled during cancellation
                            await asyncio.sleep(0.5)
                            if self.entry_order_status == 'FILLED':
                                self.logger.log(f"✅ Order filled during cancellation attempt", "INFO")
                                return True
                    except Exception as e:
                        self.logger.log(f"❌ Error cancelling order: {e}", "ERROR")

                    # Reset state and retry
                    self.current_entry_order_id = None
                    self.entry_order_status = None
                    self.entry_order_price = None
                    retry_count += 1
                    continue
                else:
                    # Price still optimal, continue waiting
                    self.logger.log(
                        f"✅ Order price still optimal ({self.entry_order_price}), continuing to wait...",
                        "INFO"
                    )

                    # Wait another 5 seconds
                    await asyncio.sleep(5)

                    # Check again if filled
                    if self.entry_order_status == 'FILLED':
                        self.logger.log(f"✅ Entry order filled", "INFO")
                        return True

                    # Still not filled, retry
                    retry_count += 1

            except Exception as e:
                self.logger.log(f"❌ Error placing entry order: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
                retry_count += 1
                await asyncio.sleep(2)

        # Max retries reached
        self.logger.log(f"❌ Failed to fill entry order after {max_retries} attempts", "ERROR")
        return False

    async def place_exit_order(self, grid_pos: GridPosition) -> bool:
        """
        Place take-profit order for a specific grid position.

        Args:
            grid_pos: The grid position to place TP order for

        Returns:
            bool: True if order placed successfully, False otherwise
        """
        try:
            if not grid_pos.is_filled:
                self.logger.log(f"❌ Cannot place exit order: grid position not filled yet", "ERROR")
                return False

            # Calculate take-profit price
            if self.config.direction == "buy":
                # For long: sell at higher price (when price goes UP to TP)
                exit_price = grid_pos.entry_price * (
                    Decimal('1') + self.config.take_profit_pct / Decimal('100')
                )
            else:
                # For short: buy at lower price (when price goes DOWN to TP)
                exit_price = grid_pos.entry_price * (
                    Decimal('1') - self.config.take_profit_pct / Decimal('100')
                )

            # Round to tick size
            exit_price = self.exchange_client.round_to_tick(exit_price)

            self.logger.log(
                f"📊 Placing {self.config.close_order_side} take-profit LIMIT order: "
                f"price={exit_price}, qty={grid_pos.entry_quantity}",
                "INFO"
            )

            # Use regular limit order for take-profit
            order_result = await self.exchange_client.place_close_order(
                self.config.contract_id,
                grid_pos.entry_quantity,
                exit_price,
                self.config.close_order_side
            )

            if not order_result.success:
                self.logger.log(f"❌ Failed to place exit order: {order_result.error_message}", "ERROR")
                return False

            grid_pos.exit_order_id = order_result.order_id
            self.logger.log(f"✅ Exit order placed: {order_result.order_id} for entry {grid_pos.entry_order_id}", "INFO")

            # Send Telegram notification
            total_qty = self.position.get_total_quantity()
            avg_price = self.position.get_average_entry_price()
            trade_msg = (
                f"🚀 <b>Grid Bot - Position Opened</b>\n\n"
                f"Ticker: <code>{self.config.ticker}</code>\n"
                f"Direction: <b>{self.config.direction.upper()}</b>\n"
                f"Entry Price: <code>{grid_pos.entry_price}</code>\n"
                f"Exit Price: <code>{exit_price}</code>\n"
                f"Quantity: <code>{grid_pos.entry_quantity}</code>\n"
                f"Total Positions: <b>{len([p for p in self.position.positions if p.is_filled and not p.exit_filled])}</b>\n"
                f"Total Quantity: <code>{total_qty}</code>\n"
                f"Average Price: <code>{avg_price}</code>\n"
                f"Take Profit: <b>{self.config.take_profit_pct}%</b>"
            )
            self.send_telegram_notification(trade_msg)

            return True

        except Exception as e:
            self.logger.log(f"❌ Error placing exit order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False

    def has_pending_entry_order(self) -> bool:
        """
        Check if there's already a pending (unfilled) entry order.

        Returns:
            bool: True if there's a pending entry order
        """
        for pos in self.position.positions:
            if not pos.is_filled:
                return True
        return False

    async def cancel_pending_entry_orders(self) -> bool:
        """
        Cancel all pending (unfilled) entry orders.

        Returns:
            bool: True if all cancellations successful
        """
        pending_positions = [pos for pos in self.position.positions if not pos.is_filled]

        if not pending_positions:
            return True

        all_success = True
        for pos in pending_positions:
            try:
                self.logger.log(f"🚫 Cancelling pending entry order: {pos.entry_order_id}", "INFO")
                cancel_result = await self.exchange_client.cancel_order(pos.entry_order_id)

                if cancel_result.success:
                    self.logger.log(f"✅ Order {pos.entry_order_id} cancelled", "INFO")
                    # Remove from positions list
                    self.position.positions = [
                        p for p in self.position.positions
                        if p.entry_order_id != pos.entry_order_id
                    ]
                else:
                    self.logger.log(f"⚠️ Failed to cancel order {pos.entry_order_id}: {cancel_result.error_message}", "WARNING")
                    all_success = False
            except Exception as e:
                self.logger.log(f"❌ Error cancelling order {pos.entry_order_id}: {e}", "ERROR")
                all_success = False

        return all_success

    async def should_add_grid_position(self) -> bool:
        """
        Check if we should add a new grid position based on price movement.

        For long: add position if price dropped 0.05% below lowest entry
        For short: add position if price rose 0.05% above highest entry

        CRITICAL: Also verify that the new entry price will be 0.05% better than
        the current lowest/highest entry price, otherwise skip this opportunity.

        Important: If grid trigger detected and there's a pending entry order,
        cancel it first and place new order at better price.

        Returns:
            bool: True if should add new position
        """
        if not self.config.enable_grid:
            return False

        # Check if we've reached max positions
        active_positions = len([p for p in self.position.positions if p.is_filled and not p.exit_filled])
        if active_positions >= self.config.max_positions:
            return False

        # Get current best price
        current_price = await self.get_best_price()

        # Calculate what the new entry price would be
        new_entry_price, _ = self.calculate_entry_prices(current_price)

        # Check grid trigger condition
        should_trigger = False

        if self.config.direction == "buy":
            # For long: check if price dropped below lowest entry
            if self.position.lowest_entry_price is None:
                should_trigger = True  # No positions yet, should enter
            else:
                # Calculate threshold: lowest_entry - 0.05%
                threshold_price = self.position.lowest_entry_price * (
                    Decimal('1') - self.config.grid_trigger_pct / Decimal('100')
                )

                # Check if current price triggers grid condition
                if current_price <= threshold_price:
                    # CRITICAL: Also verify new entry price is 0.05% lower than lowest entry
                    required_entry_price = self.position.lowest_entry_price * (
                        Decimal('1') - self.config.grid_trigger_pct / Decimal('100')
                    )

                    if new_entry_price <= required_entry_price:
                        self.logger.log(
                            f"📉 Grid trigger: price {current_price} <= threshold {threshold_price}, "
                            f"new entry {new_entry_price} <= required {required_entry_price} "
                            f"(lowest entry {self.position.lowest_entry_price} - {self.config.grid_trigger_pct}%)",
                            "INFO"
                        )
                        should_trigger = True
                    else:
                        self.logger.log(
                            f"⏸️ Grid trigger met but new entry price {new_entry_price} > required {required_entry_price}, skipping",
                            "INFO"
                        )
        else:  # sell
            # For short: check if price rose above highest entry
            if self.position.highest_entry_price is None:
                should_trigger = True  # No positions yet, should enter
            else:
                # Calculate threshold: highest_entry + 0.05%
                threshold_price = self.position.highest_entry_price * (
                    Decimal('1') + self.config.grid_trigger_pct / Decimal('100')
                )

                # Check if current price triggers grid condition
                if current_price >= threshold_price:
                    # CRITICAL: Also verify new entry price is 0.05% higher than highest entry
                    required_entry_price = self.position.highest_entry_price * (
                        Decimal('1') + self.config.grid_trigger_pct / Decimal('100')
                    )

                    if new_entry_price >= required_entry_price:
                        self.logger.log(
                            f"📈 Grid trigger: price {current_price} >= threshold {threshold_price}, "
                            f"new entry {new_entry_price} >= required {required_entry_price} "
                            f"(highest entry {self.position.highest_entry_price} + {self.config.grid_trigger_pct}%)",
                            "INFO"
                        )
                        should_trigger = True
                    else:
                        self.logger.log(
                            f"⏸️ Grid trigger met but new entry price {new_entry_price} < required {required_entry_price}, skipping",
                            "INFO"
                        )

        # If grid triggered and there's a pending entry order, cancel it first
        if should_trigger and self.has_pending_entry_order():
            self.logger.log(
                "⚠️ Grid trigger detected with pending entry order - cancelling old order to place new one at better price",
                "WARNING"
            )
            await self.cancel_pending_entry_orders()

        return should_trigger

    async def run_trading_cycle(self):
        """
        Main trading cycle with moving grid strategy.

        Flow:
        1. Check if should add new grid position
        2. If yes, place entry order and wait for fill
        3. Place take-profit order for filled position
        4. Monitor price continuously for grid opportunities
        5. Handle exit fills via WebSocket callback
        """
        try:
            # Check if we should add a new grid position
            if not await self.should_add_grid_position():
                # No grid opportunity, just wait
                await asyncio.sleep(self.config.wait_time)
                return

            # Place new grid entry order
            self.logger.log(
                f"📊 Grid opportunity detected, placing new entry order "
                f"(current positions: {len([p for p in self.position.positions if p.is_filled and not p.exit_filled])})",
                "INFO"
            )

            if not await self.place_entry_order():
                self.logger.log("⚠️ Failed to place entry order, retrying...", "WARN")
                await asyncio.sleep(self.config.wait_time)
                return

            # Wait for entry fill (handled by place_entry_order's internal logic)
            # The entry order is already filled when place_entry_order returns True

            # Find the grid position we just added
            if not self.current_entry_order_id:
                self.logger.log("❌ No current entry order ID", "ERROR")
                return

            grid_pos = self.position.get_position_by_entry_order_id(self.current_entry_order_id)
            if not grid_pos:
                self.logger.log(f"❌ Cannot find grid position for order {self.current_entry_order_id}", "ERROR")
                return

            if not grid_pos.is_filled:
                self.logger.log(f"⚠️ Grid position not filled yet, skipping TP placement", "WARN")
                return

            # Place exit order for this position
            if not await self.place_exit_order(grid_pos):
                self.logger.log("❌ Failed to place exit order for grid position", "ERROR")
                return

            self.logger.log(f"✅ Grid position cycle completed, continuing to monitor...", "INFO")

        except Exception as e:
            self.logger.log(f"❌ Error in trading cycle: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            # Send error notification
            error_msg = (
                f"❌ <b>Grid Bot Error</b>\n\n"
                f"Ticker: <code>{self.config.ticker}</code>\n"
                f"Error: {str(e)[:200]}"
            )
            self.send_telegram_notification(error_msg)

    async def run(self):
        """
        Main run loop for momentum bot.

        Connects to exchange and runs trading cycles continuously
        until shutdown is requested.
        """
        try:
            # Store event loop reference for thread-safe operations
            self.loop = asyncio.get_event_loop()

            # Connect to exchange
            self.logger.log("🔌 Connecting to exchange...", "INFO")
            await self.exchange_client.connect()
            self.logger.log("✅ Connected to exchange", "INFO")

            # Initialize market information
            await self.initialize_market_info()

            # Wait for orderbook to be ready
            self.logger.log("⏳ Waiting for orderbook data...", "INFO")
            await asyncio.sleep(3)

            # Send startup notification
            startup_msg = (
                f"🤖 <b>Moving Grid Bot Started</b>\n\n"
                f"Exchange: <code>{self.config.exchange}</code>\n"
                f"Ticker: <code>{self.config.ticker}</code>\n"
                f"Contract ID: <code>{self.config.contract_id}</code>\n"
                f"Direction: <b>{self.config.direction.upper()}</b>\n"
                f"Quantity: <code>{self.config.quantity}</code>\n"
                f"Tick Offset: <b>{self.config.tick_offset} ticks</b>\n"
                f"Take Profit: <b>{self.config.take_profit_pct}%</b>\n"
                f"Grid Enabled: <b>{self.config.enable_grid}</b>\n"
                f"Grid Trigger: <b>{self.config.grid_trigger_pct}%</b>\n"
                f"Max Positions: <b>{self.config.max_positions}</b>"
            )
            self.send_telegram_notification(startup_msg)

            # Main trading loop
            while not self.shutdown_requested:
                await self.run_trading_cycle()

                # Wait before next cycle
                if not self.shutdown_requested:
                    await asyncio.sleep(self.config.wait_time)

        except Exception as e:
            self.logger.log(f"❌ Fatal error in run loop: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            raise

        finally:
            # Cleanup
            await self.graceful_shutdown("Run loop terminated")


async def main():
    """
    Example usage of MomentumBot.

    This function demonstrates how to configure and run the momentum bot.
    In production, configuration should be loaded from environment variables
    or a config file.
    """
    # Example configuration for ETH momentum trading
    config = MomentumConfig(
        ticker="ETH",
        exchange="extended",
        quantity=Decimal("0.1"),
        direction="buy",
        tick_offset=1,  # Entry 1 tick away from current best price
        take_profit_pct=Decimal("0.02"),   # Take profit at 0.02%
        max_positions=1,
        wait_time=5
    )

    bot = MomentumBot(config)

    try:
        await bot.run()
    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received")
        await bot.graceful_shutdown("Keyboard interrupt")


if __name__ == "__main__":
    asyncio.run(main())
