"""
Extended exchange client interface.
All exchange implementations should inherit from this class.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple, Type, Union
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger

from x10.perpetual.trading_client import PerpetualTradingClient
from x10.perpetual.configuration import MAINNET_CONFIG as STARKNET_MAINNET_CONFIG
from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.orders import (
    TimeInForce,
    OrderSide,
    OrderType,
    OrderTriggerPriceType,
    OrderTriggerDirection,
    OrderPriceType,
    CreateOrderConditionalTriggerModel,
    CreateOrderTpslTriggerModel,
    NewOrderModel,
    SelfTradeProtectionLevel
)
from x10.utils.nonce import generate_nonce
from x10.utils.date import to_epoch_millis
from x10.perpetual.fees import DEFAULT_FEES

# Try to import create_order_object for proper settlement signature generation
try:
    from x10.perpetual.order_object import create_order_object
    HAS_CREATE_ORDER_OBJECT = True
except ImportError:
    HAS_CREATE_ORDER_OBJECT = False

# Try to import order_object_settlement for proper TP/SL settlement generation
try:
    from x10.perpetual.order_object_settlement import SettlementDataCtx, create_order_settlement_data
    HAS_SETTLEMENT_DATA = True
except ImportError:
    HAS_SETTLEMENT_DATA = False

from cryptography.hazmat.primitives.asymmetric import ed25519
import base64
import websockets
import time

import json
import traceback
import asyncio
import aiohttp

from dotenv import load_dotenv
import os
from datetime import datetime, timezone, timedelta

load_dotenv()

async def _stream_worker(
    url: str,
    handler,
    stop_event: asyncio.Event,
    extra_headers: dict | list[tuple[str, str]] | None = None,):
    while not stop_event.is_set():
        try:
            async with websockets.connect(
                url, 
                ping_interval=20,
                ping_timeout=20,
                extra_headers=extra_headers
            ) as ws:
                print(f"✅ connected to {url}")
                async for raw in ws:
                    if raw == "ping":
                        await ws.send("pong")
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("type") == "PING":
                        await ws.send(json.dumps({"type": "PONG"}))
                        continue

                    await handler(msg)

        except Exception as e:
            print(f"❌ {url} error: {e}")
            await asyncio.sleep(3)  # simple reconnect delay

def utc_now():
    return datetime.now(tz=timezone.utc)

class ExtendedClient(BaseExchangeClient):
    """Extended exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the exchange client with configuration."""
        super().__init__(config)

        vault = os.getenv('EXTENDED_VAULT')
        private_key = os.getenv('EXTENDED_STARK_KEY_PRIVATE')
        public_key = os.getenv('EXTENDED_STARK_KEY_PUBLIC')
        api_key = os.getenv('EXTENDED_API_KEY')
        self.api_key = api_key

        self.stark_account = StarkPerpetualAccount(vault=vault, private_key=private_key, public_key=public_key, api_key=api_key)
        # 按照 trading_client.py 的方式初始化
        self.stark_config = STARKNET_MAINNET_CONFIG
        self.perpetual_trading_client = PerpetualTradingClient(self.stark_config, self.stark_account)

        # Initialize logger using the same format as helpers
        self.logger = TradingLogger(exchange="extended", ticker=self.config.ticker, log_to_console=True)
        self._order_update_handler = None

        self.orderbook = None
        
        # For websocket
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        
        # Maintain open order dict because there is a delay in the official Rest API
        self.open_orders = {} # {order_id: order_info}
        self.partially_filled_size = 0
        self.partially_filled_avg_price = 0
        self.initial_check_for_open_orders = True  # PATCH: will turn to False after 2 times (to match the trading bot logic), so that we can get the open orders even after restarting the script
        self.get_active_orders_cnt = 0

    def _validate_config(self) -> None:
        """Validate the exchange-specific configuration."""
        required_env_vars = ['EXTENDED_VAULT', 'EXTENDED_STARK_KEY_PRIVATE', 'EXTENDED_STARK_KEY_PUBLIC', 'EXTENDED_API_KEY']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def connect(self) -> None:
        """Connect to the exchange (WebSocket, etc.)."""
        
        self._stop_event.clear()
        
        host = STARKNET_MAINNET_CONFIG.stream_url
        self._tasks = [
            # connect to the account update stream (for order updates)
            asyncio.create_task(_stream_worker(
                host + "/account",
                self.handle_account, 
                self._stop_event,
                extra_headers=[("X-API-Key", self.api_key)]
                )),
            # connect to the orderbook update stream
            asyncio.create_task(_stream_worker(
                host + "/orderbooks/" + self.config.ticker + "-USD" + "?depth=1",
                self.handle_orderbook,
                self._stop_event
                )),
        ]
        self.logger.log("Streams started", "INFO")
        
    async def disconnect(self) -> None:
        """Disconnect from the exchange gracefully."""
        try:
            self.logger.log("Starting graceful disconnect from Extended exchange", "INFO")
            
            # 1. Cancel outstanding buy orders
            active_orders = await self.get_active_orders(self.config.contract_id)
            for order in active_orders:
                if order.side == "buy":
                    await self.cancel_order(order.order_id)
                
            
            # Stop WebSocket streams
            self._stop_event.set()
            for t in self._tasks:
                t.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self.logger.log("Streams stopped", "INFO")
            
            # 2. Close the main client connection if it exists
            if hasattr(self, 'perpetual_trading_client') and self.perpetual_trading_client:
                try:
                    await self.perpetual_trading_client.close()
                    self.logger.log("Main client connection closed", "INFO")
                except Exception as e:
                    self.logger.log(f"Error closing main client: {e}", "WARNING")
            
            # 5. Reset internal state
            self.orderbook = None
            self._order_update_handler = None
            
            self.logger.log("Extended exchange disconnected successfully", "INFO")
            
        except Exception as e:
            self.logger.log(f"Error during Extended disconnect: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            raise
        
        
    async def fetch_bbo_prices(self, contract_id: str) -> tuple[Decimal, Decimal]:
        """Fetch best bid and offer prices from orderbook."""
        try:
            # Get the orderbook from the websocket updated cache
            orderbook = self.orderbook
            
            if orderbook == None:
                self.logger.log(f"Error fetching BBO prices for {contract_id}: orderbook is None", level="ERROR")
                return Decimal('0'), Decimal('0')
            
            # Get best bid (highest bid price)
            best_bid = Decimal('0')
            if orderbook["bid"] and len(orderbook["bid"]) > 0:
                best_bid = Decimal(orderbook["bid"][0]["p"])
            
            # Get best ask (lowest ask price)  
            best_ask = Decimal('0')
            if orderbook["ask"] and len(orderbook["ask"]) > 0:
                best_ask = Decimal(orderbook["ask"][0]["p"])

            return best_bid, best_ask
            
        except Exception as e:
            self.logger.log(f"Error fetching BBO prices for {contract_id}: {str(e)}", level="ERROR")
            return Decimal('0'), Decimal('0')

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Extended using official SDK with retry logic for POST_ONLY rejections."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            try:
                if self.orderbook == None:
                    # the websocket orderbook is not updated yet, sleep for 1 second
                    await asyncio.sleep(1)
                    self.logger.log(f"Orderbook is not updated yet, sleeping for 1 second", level="INFO")
                    continue

                best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

                if best_bid <= 0 or best_ask <= 0:
                    return OrderResult(success=False, error_message='Invalid bid/ask prices')

                if direction == 'buy':
                    # For buy orders, place slightly below best ask to ensure execution
                    order_price = best_ask - self.config.tick_size
                    side = OrderSide.BUY
                else:
                    # For sell orders, place slightly above best bid to ensure execution
                    order_price = best_bid + self.config.tick_size
                    side = OrderSide.SELL

                # Round price to appropriate precision
                rounded_price = self.round_to_tick(order_price)

                # set timeout to 9 seconds for open orders to avoid orders being filled right when trading_bot hit 10s timeout and call cancel_order
                # Place the order using official SDK (post-only to ensure maker order)
                order_result = await self.perpetual_trading_client.place_order(
                    market_name=contract_id,
                    amount_of_synthetic=quantity,
                    price=rounded_price,
                    side=side,
                    time_in_force=TimeInForce.GTT,
                    post_only=True,  # Ensure MAKER orders
                    expire_time = utc_now() + timedelta(days=1), # SDK 1 hour default
                )

                if not order_result or not order_result.data or order_result.status != 'OK':
                    return OrderResult(success=False, error_message='Failed to place order')

                # Extract order ID from response
                order_id = order_result.data.id
                if not order_id:
                    return OrderResult(success=False, error_message='No order ID in response')

                # Check order status after a short delay to see if it was rejected
                await asyncio.sleep(0.01)
                order_info = await self.get_order_info(order_id)

                if order_info:
                    if order_info.status in ['CANCELED', 'REJECTED']:
                        if retry_count < max_retries - 1:
                            self.logger.log(f"POST-ONLY order rejected, retrying", level="INFO")
                            retry_count += 1
                            continue
                        else:
                            self.logger.log(f"POST-ONLY order rejected after {max_retries} attempts", level="ERROR")
                            return OrderResult(success=False, error_message=f'Order rejected after {max_retries} attempts')
                    elif order_info.status in ['NEW', 'OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                        # Order successfully placed
                        return OrderResult(
                            success=True,
                            order_id=order_id,
                            side=side.value,
                            size=quantity,
                            price=rounded_price,
                            status=order_info.status
                        )
                    else:
                        return OrderResult(success=False, error_message=f'Unexpected order status: {order_info.status}')
                else:
                    # Assume order is successful if we can't get info
                    self.logger.log(f"Could not get order info for {order_id}, assuming order is successful", level="ERROR")
                    return OrderResult(
                        success=True,
                        order_id=order_id,
                        side=side.value,
                        size=quantity,
                        price=rounded_price
                    )

            except Exception as e:
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)  # Wait before retry
                    continue
                else:
                    return OrderResult(success=False, error_message=str(e))

        return OrderResult(success=False, error_message='Max retries exceeded')
    
    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Extended using official SDK with retry logic for POST_ONLY rejections."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            try:
                best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
                print(f"best_bid: {best_bid}, best_ask: {best_ask}")
                print('--------------------------------')
                if best_bid <= 0 or best_ask <= 0:
                    return OrderResult(success=False, error_message='Invalid bid/ask prices')

                # Convert side string to OrderSide enum
                order_side = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL

                # Adjust order price based on market conditions and side
                adjusted_price = price
                
                # PATCH: (for partially filled orders) Adjust the quantity to add partially filled order size so that we can close them together
                # cache these just in case order fails to place
                prev_partially_filled_size = self.partially_filled_size
                prev_partially_filled_avg_price = self.partially_filled_avg_price
                
                if self.partially_filled_size > 0:
                    self.logger.log(f"Adding partially_filled_size {self.partially_filled_size} and partially_filled_avg_price {self.partially_filled_avg_price} to the close order", level="INFO")
                    quantity = quantity + self.partially_filled_size
                    expected_tp_price_for_partial_fills = (1 + self.config.take_profit/100) * self.partially_filled_avg_price
                    adjusted_price = (price * quantity + expected_tp_price_for_partial_fills * self.partially_filled_size) / (quantity + self.partially_filled_size)
                    self.logger.log(f"Updated close order quantity to {quantity} and adjusted_price to {adjusted_price}", level="INFO")

                    # reset to 0
                    self.partially_filled_size = 0
                    self.partially_filled_avg_price = 0
                    self.logger.log(f"Reset partially_filled_size and partially_filled_avg_price to 0", level="INFO")

                if side.lower() == 'sell':
                    # For sell orders, ensure price is above best bid to be a maker order
                    if price <= best_bid:
                        adjusted_price = best_bid + self.config.tick_size
                elif side.lower() == 'buy':
                    # For buy orders, ensure price is below best ask to be a maker order
                    if price >= best_ask:
                        adjusted_price = best_ask - self.config.tick_size

                # Round price to appropriate precision
                rounded_price = self.round_to_tick(adjusted_price)
                quantity = quantity.quantize(self.min_order_size, rounding=ROUND_HALF_UP)

                # Place the order using official SDK (post-only to avoid taker fees)
                order_result = await self.perpetual_trading_client.place_order(
                    market_name=contract_id,
                    amount_of_synthetic=quantity,
                    price=rounded_price,
                    side=order_side,
                    time_in_force=TimeInForce.GTT,
                    post_only=True,  # Ensure MAKER orders
                    expire_time = utc_now() + timedelta(days=90), # SDK 1 hour default
                )

                if not order_result or not order_result.data or order_result.status != 'OK':
                    # reset to previous values
                    self.partially_filled_size = prev_partially_filled_size
                    self.partially_filled_avg_price = prev_partially_filled_avg_price
                    self.logger.log(f"Reverted partially_filled_size and partially_filled_avg_price to {prev_partially_filled_size} and {prev_partially_filled_avg_price}", level="INFO")
                    return OrderResult(success=False, error_message='Failed to place order')

                # Extract order ID from response
                order_id = order_result.data.id
                if not order_id:
                    return OrderResult(success=False, error_message='No order ID in response')

                # Check order status after a short delay to see if it was rejected
                await asyncio.sleep(0.01)
                order_info = await self.get_order_info(order_id)

                if order_info:
                    if order_info.status == 'CANCELED':
                        if retry_count < max_retries - 1:
                            retry_count += 1
                            continue
                        else:
                            return OrderResult(success=False, error_message=f'Close order rejected after {max_retries} attempts')
                    elif order_info.status in ['NEW', 'OPEN', 'PARTIALLY_FILLED', 'FILLED']:
                        # Order successfully placed
                        return OrderResult(
                            success=True,
                            order_id=order_id,
                            side=side,
                            size=quantity,
                            price=rounded_price,
                            status=order_info.status
                        )
                    elif order_info.status == 'REJECTED':
                        raise Exception(f'Close order rejected: {order_info.status}')
                    else:
                        raise Exception(f'Unexpected close order status: {order_info.status}')
                else:
                    # Assume order is successful if we can't get info
                    self.logger.log(f"Could not get order info for {order_id}, assuming order is successful", level="ERROR")
                    return OrderResult(
                        success=True,
                        order_id=order_id,
                        side=side,
                        size=quantity,
                        price=rounded_price
                    )

            except Exception as e:
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)  # Wait before retry
                    continue
                else:
                    return OrderResult(success=False, error_message=str(e))

        return OrderResult(success=False, error_message='Max retries exceeded')

    async def place_market_order(self, contract_id: str, quantity: Decimal, side: str, price_offset: Decimal = Decimal('0.0002')) -> OrderResult:
        """
        Place a true market order with Extended using IOC (Immediate Or Cancel).
        This guarantees immediate execution at the best available price.

        Note: price_offset parameter is ignored - kept for API compatibility.
        """
        try:
            from datetime import datetime, timedelta, timezone

            def utc_now():
                return datetime.now(timezone.utc)

            # Get best bid/ask
            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
            if best_bid <= 0 or best_ask <= 0:
                return OrderResult(success=False, error_message='Invalid bid/ask prices')

            # For IOC market orders, use a slightly worse price than best to avoid rejection
            # Extended seems to reject IOC orders at exact best bid/ask
            # Use 0.05% worse price (穿价 0.05% 避免被拒绝)
            price_cushion = Decimal('1.0005')  # 0.05% cushion

            if side.lower() == 'buy':
                # 买单：使用略高于卖一价的价格
                target_price = best_ask * price_cushion
            else:
                # 卖单：使用略低于买一价的价格
                target_price = best_bid / price_cushion

            # Round price
            target_price = self.round_to_tick(target_price)
            quantity = quantity.quantize(self.min_order_size, rounding=ROUND_HALF_UP)

            # Convert side
            order_side = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL

            self.logger.log(f"Extended IOC market order: {side} {quantity} @ {target_price} (BBO: {best_bid}/{best_ask})", level="INFO")

            # Place IOC order - executes immediately or cancels unfilled portion
            order_result = await self.perpetual_trading_client.place_order(
                market_name=contract_id,
                amount_of_synthetic=quantity,
                price=target_price,
                side=order_side,
                time_in_force=TimeInForce.IOC,  # Immediate Or Cancel - guarantees immediate execution
                post_only=False,
                expire_time=utc_now() + timedelta(days=1),  # 设置较长过期时间避免网络延迟导致订单过期
            )

            # 打印完整的 order_result 用于调试
            self.logger.log(f"IOC 下单返回完整数据: order_result={order_result}", level="INFO")
            if order_result:
                self.logger.log(f"  status={order_result.status}", level="INFO")
                if hasattr(order_result, 'data') and order_result.data:
                    self.logger.log(f"  data={order_result.data}", level="INFO")
                    if hasattr(order_result.data, '__dict__'):
                        self.logger.log(f"  data.__dict__={order_result.data.__dict__}", level="INFO")

            if not order_result or not order_result.data or order_result.status != 'OK':
                error_msg = 'Failed to place market order'
                if order_result:
                    error_msg += f' (status: {order_result.status})'
                self.logger.log(error_msg, level="ERROR")
                return OrderResult(success=False, error_message=error_msg)

            order_id = order_result.data.id
          
            return OrderResult(
                success=True,
                order_id=order_id,
                side=side,
                size=quantity,
                price=target_price,
                status='OPEN'  # IOC orders fill immediately, but we return OPEN for API compatibility
            )

        except Exception as e:
            self.logger.log(f"Failed to place market order: {e}", level="ERROR")
            return OrderResult(success=False, error_message=str(e))

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Extended using the internal order ID."""
        try:
            # save this
            prev_partially_filled_size = self.partially_filled_size
            prev_partially_filled_avg_price = self.partially_filled_avg_price

            # cancel the order
            cancel_result = await self.perpetual_trading_client.orders.cancel_order(order_id)

            if not cancel_result or not cancel_result.data:
                self.logger.log(f"Failed to cancel order {order_id}: {cancel_result}", level="ERROR")
                # revert this
                self.partially_filled_size = prev_partially_filled_size
                self.partially_filled_avg_price = prev_partially_filled_avg_price
                self.logger.log(f"Reverted partially filled size and price: {self.partially_filled_size} and {self.partially_filled_avg_price}", level="INFO")
                return OrderResult(success=False, error_message='Failed to cancel order')

            await asyncio.sleep(0.1)
            # get order info to know what was the (partially) filled order size
            order_info = await self.get_order_info(order_id)
            min_order_size = self.min_order_size
            filled_size = 0
            if order_info:
                if order_info.filled_size >= min_order_size:
                    filled_size = order_info.filled_size
                elif order_info.filled_size > 0:
                    self.logger.log(f"Order {order_id} is partially filled, but filled size is less than min order size, returning filled_size as 0, we will send a close order for this later", level="INFO")
                    self.logger.log(f"Caching partially filled size and price: {order_info.filled_size} and {order_info.price}", level="INFO")
                    # cache this filled_size and price
                    self.partially_filled_size = self.partially_filled_size + order_info.filled_size
                    if self.partially_filled_avg_price > 0:
                        self.partially_filled_avg_price = (self.partially_filled_avg_price * self.partially_filled_size + order_info.filled_size * order_info.price) / (self.partially_filled_size + order_info.filled_size)
                    else:
                        self.partially_filled_avg_price = order_info.price
                    self.logger.log(f"Updated partially filled size and price: {self.partially_filled_size} and {self.partially_filled_avg_price}", level="INFO")
                else:
                    # filled_size is 0
                    pass
            else:
                self.logger.log(f"Could not get order info for {order_id} when cancelling order, returning filled_size as 0", level="ERROR")            

            return OrderResult(success=True, filled_size=filled_size)

        except Exception as e:
            return OrderResult(success=False, error_message=str(e))

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information using REST API endpoint."""
        order_info = None
        url = f"https://api.starknet.extended.exchange/api/v1/user/orders/{order_id}"
        headers = {
            "X-Api-Key": self.api_key,
            "User-Agent": "User-Agent"
        }

        attempt = 0
        while not order_info and attempt < 50:
            attempt += 1
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()

                            # 打印完整的响应数据用于调试
                            self.logger.log(f"Extended API 响应 (order_id={order_id}, attempt={attempt}): {data}", "INFO")

                            if data.get("status") != "OK" or not data.get("data"):
                                self.logger.log(f"Failed to get order info attempt {attempt} for {order_id}: status={data.get('status')}, has_data={bool(data.get('data'))}", "ERROR")
                                return None
                            
                            order_data = data["data"]
                            
                            # Convert status to match expected format
                            status = order_data.get("status", "")
                            if status == "NEW":
                                status = "OPEN"
                            elif status == "CANCELLED":
                                status = "CANCELED"
                            
                            # Create OrderInfo object
                            order_info = OrderInfo(
                                order_id=str(order_data.get("id", "")),
                                side=order_data.get("side", "").lower(),
                                size=Decimal(order_data.get("qty", "0")) - Decimal(order_data.get("filledQty", "0")),
                                price=Decimal(order_data.get("price", "0")),
                                status=status,
                                filled_size=Decimal(order_data.get("filledQty", "0")),
                                remaining_size=Decimal(order_data.get("qty", "0")) - Decimal(order_data.get("filledQty", "0"))
                            )
                            return order_info
                        
                        elif response.status == 404:
                            # Order not found
                            self.logger.log(f"Order {order_id} not found attempt {attempt}", "INFO")
                        
                        else:
                            self.logger.log(f"Failed to get order info attempt {attempt} for {order_id}: HTTP {response.status}", "ERROR")
                            
            except Exception as e:
                self.logger.log(f"Error getting order info attempt {attempt} for {order_id}: {str(e)}", "ERROR")
            
            await asyncio.sleep(0.5)
        return order_info

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract using official SDK."""
        ## ORIGINAL
        # contract_id should be market name, e.g. ETH-USD
        if self.initial_check_for_open_orders:
            active_orders = await self.perpetual_trading_client.account.get_open_orders(market_names=[contract_id])

            if not active_orders or not hasattr(active_orders, 'data'):
                return []

            # Filter orders for the specific contract and ensure they are dictionaries
            # The API returns orders under 'data' key as a list
            order_list = active_orders.data
            contract_orders = []

            for order in order_list:
                if order.market == contract_id:
                    if order.status == 'NEW':
                        order_status = 'OPEN'
                    else:
                        order_status = order.status

                    contract_orders.append(OrderInfo(
                        order_id=order.id,
                        side=order.side.lower(),
                        size=Decimal(order.qty) - Decimal(order.filled_qty),  # PATCH: changed this to remaining size to match with the trading bot logic, might cause issues later if main trading bot logic is changed
                        price=Decimal(order.price),
                        status=order_status,
                        filled_size=Decimal(order.filled_qty),
                        remaining_size=Decimal(order.qty) - Decimal(order.filled_qty)
                    ))
            
            # use the websocket method for the remaining orders updates
            if self.get_active_orders_cnt == 0:
                self.get_active_orders_cnt += 1
            else:
                self.get_active_orders_cnt += 1
                if self.get_active_orders_cnt == 2:
                    self.initial_check_for_open_orders = False
            return contract_orders
              
        ## FIX
        # use open orders dict because there is a delay in the official Rest API
        order_list = self.open_orders.values()
        contract_orders = []
        
        for order in order_list:
            if order.get('market') == contract_id:
                if order.get('status') == 'NEW':
                    order_status = 'OPEN'
                else:
                    order_status = order.get('status')

                formatted_order = OrderInfo(
                    order_id=order.get('id'),
                    side=order.get('side').lower(),
                    size=Decimal(order.get('qty')) - Decimal(order.get('filledQty')),   # PATCH: changed this to remaining size to match with the trading bot logic, might cause issues later if main trading bot logic is changed
                    price=Decimal(order.get('price')),
                    status=order_status,
                    filled_size=Decimal(order.get('filledQty')),
                    remaining_size=Decimal(order.get('qty')) - Decimal(order.get('filledQty'))
                )

                # handle it using dict methods because that's the format received through websocket
                contract_orders.append(formatted_order)

        return contract_orders

    async def get_account_positions(self) -> Decimal:
        """Get account positions."""
        # contract_id should be market name, e.g. ETH-USD
        positions_data = await self.perpetual_trading_client.account.get_positions(market_names=[self.config.ticker+"-USD"])
        if not positions_data or not hasattr(positions_data, 'data'):
            self.logger.log("No positions or failed to get positions", "WARNING")
            position_amt = 0
        else:
            # The API returns positions under data
            positions = positions_data.data
            if positions:
                # Find position for current contract
                position = None
                for p in positions:
                    if p.market == self.config.contract_id:
                        position = p
                        break

                if position:
                    # Return absolute position for compatibility with existing bots
                    position_amt = abs(Decimal(position.size))
                else:
                    position_amt = 0
            else:
                position_amt = 0
        return position_amt

    @query_retry(default_return=None)
    async def get_signed_position(self) -> Decimal:
        """
        Get signed position for hedge mode.
        Returns: Decimal with sign (positive for long, negative for short)

        Note: Extended API (X10 SDK) uses separate fields:
        - position.size: unsigned Decimal (always positive)
        - position.side: PositionSide enum (LONG or SHORT)
        """
        # Get positions from API
        positions_data = await self.perpetual_trading_client.account.get_positions(market_names=[self.config.ticker+"-USD"])
        if not positions_data or not hasattr(positions_data, 'data'):
            self.logger.log("No positions or failed to get positions", "WARNING")
            return Decimal('0')

        positions = positions_data.data
        if positions:
            # Find position for current contract
            position = None
            for p in positions:
                if p.market == self.config.contract_id:
                    position = p
                    break

            if position:
                # Get unsigned size
                size_value = Decimal(position.size)

                # Check position side to determine sign
                # position.side is a PositionSide enum: LONG or SHORT
                if hasattr(position, 'side'):
                    side = str(position.side).upper()
                    if 'SHORT' in side:
                        size_value = -size_value
                        self.logger.log(f"Extended SHORT position: {size_value}", "DEBUG")
                    else:
                        self.logger.log(f"Extended LONG position: {size_value}", "DEBUG")
                else:
                    self.logger.log(f"WARNING: No 'side' field in Extended position, assuming LONG: {size_value}", "WARNING")

                return size_value

        return Decimal('0')

    async def handle_account(self, message):
        """Handle order updates from WebSocket using correct pattern."""
        try:
            # self.logger.log("Received account update", "INFO")
            
            # Parse the message structure
            if isinstance(message, str):
                message = json.loads(message)

            # Check if this is a order update
            event = message.get("type", "")
            if event == "ORDER":
                # Extract order data from the nested structure
                data = message.get('data', {})
                orders = data.get('orders', [])
                
                if orders and len(orders) > 0:
                    # Loop over all the order updates, extended websocket may send multiple order updates in one message
                    for order in orders:
                        if order.get('market') != self.config.contract_id:
                            continue
                        order_id = order.get('id')
                        status = order.get('status')
                        side = order.get('side', '').lower()
                        filled_size = order.get('filledQty')

                        # Safe attribute access for compatibility
                        close_order_side = getattr(self.config, 'close_order_side', None)
                        if close_order_side is not None and side == close_order_side:
                            order_type = "CLOSE"
                        else:
                            order_type = "OPEN"

                        # TODO: seems to be unrelated to extended, need to check later
                        # edgex returns TWO filled events for the same order; take the first one
                        # if status == "FILLED" and len(data.get('collateral', [])):
                        #     return
                        
                        # extended return status of open orders as "NEW", change this to match with the original script logic
                        if status == "NEW":
                            status = "OPEN"
                            
                        # extended spells canceled as "CANCELLED", change this to match with the original script logic
                        if status == "CANCELLED":
                            status = "CANCELED"
                            
                        # (for extended only) maintain open orders dict
                        if status == "OPEN" or status == "PARTIALLY_FILLED":
                            self.open_orders[order_id] = order
                        elif status == "CANCELED" or status == "FILLED":
                            self.open_orders.pop(order_id, None)
                        
                        if status in ['OPEN', 'PARTIALLY_FILLED', 'FILLED', 'CANCELED']:
                            if self._order_update_handler:
                                # Call the handler (support both sync and async handlers)
                                result = self._order_update_handler({
                                    'order_id': order_id,
                                    'side': side,
                                    'order_type': order_type,
                                    'status': status,
                                    'size': order.get('qty'),
                                    'price': order.get('price'),
                                    'contract_id': order.get('market'),
                                    'filled_size': filled_size
                                })
                                # If the handler is async, await it
                                if asyncio.iscoroutine(result):
                                    await result
                            
        except asyncio.CancelledError:
            self.logger.log("Order update handler cancelled", "INFO")
            raise
        except Exception as e:
            self.logger.log(f"Error handling order update: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler
                
    async def handle_orderbook(self, message):
        """Handle orderbook updates from WebSocket using correct pattern."""
        try:
            self.logger.log("Received orderbook update", "DEBUG")

            # Parse the message structure
            if isinstance(message, str):
                message = json.loads(message)

            # Check if this is a orderbook update
            event = message.get("type", "")
            if event == "SNAPSHOT":
                data = message.get('data', {})
                market = data.get('m', '')
                bids = data.get('b', [])
                asks = data.get('a', [])
                
                # update orderbook
                self.orderbook = {
                    'market': market,
                    'bid': bids,  # should be list of [{"p": price, "q": quantity}] with a length of 1 
                    'ask': asks   # should be list of [{"p": price, "q": quantity}] with a length of 1
                }
                
                self.logger.log(f"Orderbook updated for {market}: bid={bids[0] if bids else 'N/A'}, ask={asks[0] if asks else 'N/A'}", "DEBUG")
                
        except asyncio.CancelledError:
            self.logger.log("Orderbook update handler cancelled", "INFO")
            raise
        except Exception as e:
            self.logger.log(f"Error handling orderbook update: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
        
                
    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "extended"

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """"""
        ticker = self.config.ticker
        if len(ticker) == 0:
            self.logger.log("Ticker is empty", "ERROR")
            raise ValueError("Ticker is empty")
        
        # Create the market name
        self.config.contract_id = ticker + "-USD"

        # Fetch market information to get tick size and min order size
        market_information = await self.perpetual_trading_client.markets_info.get_markets(market_names=[self.config.contract_id])
        
        # Raise error if market information is not available
        if not market_information or not hasattr(market_information, 'data') or len(market_information.data) == 0:
            self.logger.log(f"Failed to get market information for {self.config.contract_id}", "ERROR")
            raise ValueError(f"Failed to get market information for {self.config.contract_id}")
        
        # Check if config quantity is less than min order size
        min_quantity = Decimal(str(market_information.data[0].trading_config.min_order_size))
        self.min_order_size = min_quantity
        if self.config.quantity < min_quantity:
            self.logger.log(f"Order quantity is less than min quantity: {self.config.quantity} < {min_quantity}", "ERROR")
            raise ValueError(f"Order quantity is less than min quantity: {self.config.quantity} < {min_quantity}")
        
        # Set tick size in config
        self.config.tick_size = Decimal(str(market_information.data[0].trading_config.min_price_change))

        return self.config.contract_id, self.config.tick_size

    async def get_order_price(self, direction: str) -> Decimal:
        """Get the price of an order with Backpack using official SDK."""
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0:
            self.logger.log("Invalid bid/ask prices", "ERROR")
            raise ValueError("Invalid bid/ask prices")

        tick_size = self.config.tick_size
        if direction == 'buy':
            # For buy orders, place slightly below best ask to ensure execution
            order_price = best_ask - tick_size
        else:
            # For sell orders, place slightly above best bid to ensure execution
            order_price = best_bid + tick_size
        return self.round_to_tick(order_price)

    async def place_conditional_order(
        self,
        contract_id: str,
        quantity: Decimal,
        trigger_price: Decimal,
        side: str,
        take_profit_price: Optional[Decimal] = None
    ) -> OrderResult:
        """
        Place a conditional order using x10 SDK's NewOrderModel with proper conditional trigger.

        CURRENT STATUS: Using regular limit orders as a workaround due to error 1133 with conditional orders.

        The issue is that when using orders.place_order(NewOrderModel) for conditional orders,
        the Extended API returns error 1133 "Invalid order parameters". Investigation shows:
        - The JSON payload is correctly formatted according to API docs
        - The 'settlement' field (StarkKey signature) is REQUIRED but missing from the request
        - The SDK should add this automatically, but it's not working for conditional orders
        - Regular limit orders work fine using place_order() convenience method

        Possible causes:
        1. SDK bug in handling conditional orders through orders.place_order()
        2. SDK version incompatibility
        3. Missing SDK method to manually sign conditional orders

        Current workaround: Using regular limit orders instead of conditional orders.
        This means orders execute immediately at market price instead of waiting for trigger.

        TODO: Contact Extended support or investigate SDK source code to fix conditional orders.

        Args:
            contract_id: Contract ID (e.g., "ETH-USD")
            quantity: Order size
            trigger_price: Price that triggers the order (currently used as limit price)
            side: "buy" or "sell"
            take_profit_price: Optional take profit price (not currently used)

        Returns:
            OrderResult with order details
        """
        try:
            # Convert side string to OrderSide enum
            order_side = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL

            # Round prices to tick size
            trigger_price = self.round_to_tick(trigger_price)
            execution_price = self.round_to_tick(trigger_price)  # Use same price for execution

            if take_profit_price:
                take_profit_price = self.round_to_tick(take_profit_price)

            self.logger.log(
                f"Placing CONDITIONAL order: {side.upper()} {quantity} @ trigger={trigger_price}",
                "INFO"
            )

            # Generate nonce and expiry
            nonce = generate_nonce()
            expire_time = utc_now() + timedelta(days=1)
            expiry_millis = to_epoch_millis(expire_time)

            # Get fee rate - use DEFAULT_FEES directly as it's more reliable
            fee_rate = DEFAULT_FEES.taker_fee_rate
            self.logger.log(f"Using fee rate: {fee_rate}", "DEBUG")

            # Create conditional trigger model
            # For BUY: trigger when price goes UP (momentum breakout)
            # For SELL: trigger when price goes DOWN (momentum breakdown)
            conditional_trigger = CreateOrderConditionalTriggerModel(
                trigger_price=trigger_price,
                trigger_price_type=OrderTriggerPriceType.LAST,  # Use last traded price
                direction=OrderTriggerDirection.UP if side.lower() == 'buy' else OrderTriggerDirection.DOWN,
                execution_price_type=OrderPriceType.LIMIT  # Execute as limit order when triggered
            )

            # Construct NewOrderModel with conditional trigger
            # NOTE: We do NOT include take_profit/stop_loss here because they require
            # settlement signatures which are complex to generate. TPSL should be created
            # separately after the conditional order is triggered and filled.
            new_order = NewOrderModel(
                id=f"cond-{nonce}",  # External ID
                market=contract_id,
                type=OrderType.CONDITIONAL,  # Explicitly set as conditional
                side=order_side,
                qty=quantity,
                price=execution_price,  # Execution price when triggered
                time_in_force=TimeInForce.GTT,
                expiry_epoch_millis=expiry_millis,
                fee=fee_rate,
                nonce=Decimal(nonce),
                self_trade_protection_level=SelfTradeProtectionLevel.ACCOUNT,
                trigger=conditional_trigger  # Pass the trigger model
                # NOTE: take_profit and stop_loss are intentionally NOT set here
            )

            self.logger.log(
                f"📋 Submitting conditional order with nonce={nonce}, "
                f"trigger={trigger_price}, execution={execution_price}",
                "INFO"
            )

            # DEBUG: Print the order JSON that will be sent
            try:
                # Force INFO level for critical debugging
                order_json = new_order.to_api_request_json(exclude_none=True)
                self.logger.log("=" * 60, "INFO")
                self.logger.log("=== Order JSON to send ===", "INFO")
                self.logger.log(json.dumps(order_json, default=str, indent=2), "INFO")
                self.logger.log("=" * 60, "INFO")
            except Exception as e:
                self.logger.log(f"Failed to serialize order JSON: {e}", "ERROR")
                import traceback as tb
                self.logger.log(f"Traceback: {tb.format_exc()}", "ERROR")

            # DEBUG: Validate market constraints using the market data we already have
            try:
                # We already fetched market info during initialization, use that
                if hasattr(self, 'config') and hasattr(self.config, 'tick_size'):
                    self.logger.log("=== Market validation (from config) ===", "INFO")
                    self.logger.log(f"contract_id = {contract_id}", "INFO")
                    self.logger.log(f"config.tick_size = {self.config.tick_size}", "INFO")
                    if hasattr(self, 'min_order_size'):
                        self.logger.log(f"min_order_size = {self.min_order_size}", "INFO")
                        if quantity < self.min_order_size:
                            self.logger.log(
                                f"⚠️ WARNING: qty {quantity} < min_order_size {self.min_order_size}",
                                "WARNING"
                            )
            except Exception as e:
                self.logger.log(f"Could not validate market constraints: {e}", "WARNING")

            # Place order using the orders API
            try:
                # DEBUG: Check if the SDK account is properly initialized
                self.logger.log(f"=== SDK Account Check ===", "INFO")
                self.logger.log(f"Account type: {type(self.perpetual_trading_client.account).__name__}", "INFO")
                self.logger.log(f"Has create_order_object: {HAS_CREATE_ORDER_OBJECT}", "INFO")

                # Try using create_order_object if available
                if HAS_CREATE_ORDER_OBJECT:
                    self.logger.log("🔧 Attempting to use create_order_object for proper settlement signature", "INFO")

                    try:
                        # Get market info using the correct API
                        market_info = await self.perpetual_trading_client.markets_info.get_markets(market_names=[contract_id])

                        if not market_info or not hasattr(market_info, 'data') or len(market_info.data) == 0:
                            raise ValueError(f"Market {contract_id} not found or no data returned")

                        market = market_info.data[0]
                        self.logger.log(f"Found market: {market.name}", "INFO")

                        # NOTE: Disable embedded TP for now - Extended doesn't seem to honor it
                        # TP will be set as a separate limit order after entry fills
                        if False and HAS_SETTLEMENT_DATA and take_profit_price:
                            self.logger.log(f"🎯 Using create_order_settlement_data to generate TP settlement @ {take_profit_price}", "INFO")

                            # Create SettlementDataCtx with same nonce/expire_time/signer for all orders
                            settlement_ctx = SettlementDataCtx(
                                market=market,
                                fees=DEFAULT_FEES,
                                builder_fee=None,
                                nonce=nonce,
                                collateral_position_id=self.stark_account.vault,
                                expire_time=expire_time,
                                signer=self.stark_account.sign,
                                public_key=self.stark_account.public_key,
                                starknet_domain=self.stark_config.starknet_domain,
                            )

                            # Generate settlement for entry order
                            entry_settlement_data = create_order_settlement_data(
                                side=order_side,
                                synthetic_amount=quantity,
                                price=execution_price,
                                ctx=settlement_ctx,
                            )

                            self.logger.log(f"✅ Entry settlement generated", "INFO")

                            # Generate settlement for TP (opposite side)
                            tp_side = OrderSide.SELL if order_side == OrderSide.BUY else OrderSide.BUY
                            tp_settlement_data = create_order_settlement_data(
                                side=tp_side,
                                synthetic_amount=quantity,
                                price=take_profit_price,
                                ctx=settlement_ctx,
                            )

                            self.logger.log(f"✅ TP settlement generated for {tp_side.value} @ {take_profit_price}", "INFO")

                            # Create TP trigger model with settlement
                            tp_trigger = CreateOrderTpslTriggerModel(
                                trigger_price=take_profit_price,
                                trigger_price_type=OrderTriggerPriceType.LAST,
                                price=take_profit_price,
                                price_type=OrderPriceType.LIMIT,
                                settlement=tp_settlement_data.settlement,
                                debugging_amounts=tp_settlement_data.debugging_amounts,
                            )

                            # Create NewOrderModel with entry settlement, conditional trigger, and TP
                            modified_order = NewOrderModel(
                                id=f"cond-{nonce}",
                                market=contract_id,
                                type=OrderType.CONDITIONAL,
                                side=order_side,
                                qty=quantity,
                                price=execution_price,
                                time_in_force=TimeInForce.GTT,
                                expiry_epoch_millis=expiry_millis,
                                fee=fee_rate,
                                nonce=Decimal(nonce),
                                self_trade_protection_level=SelfTradeProtectionLevel.ACCOUNT,
                                trigger=conditional_trigger,
                                settlement=entry_settlement_data.settlement,
                                take_profit=tp_trigger,
                                debugging_amounts=entry_settlement_data.debugging_amounts,
                            )

                            self.logger.log(f"✅ Created conditional order with TP @ {take_profit_price}", "INFO")

                        else:
                            # Fallback to order without TP if settlement data not available
                            if take_profit_price:
                                self.logger.log(
                                    f"⚠️ TP @ {take_profit_price} will be set as separate conditional order after entry fills "
                                    f"(settlement data generation not available: HAS_SETTLEMENT_DATA={HAS_SETTLEMENT_DATA})",
                                    "INFO"
                                )

                            # Use create_order_object to build order with settlement signature
                            base_order = create_order_object(
                                account=self.stark_account,
                                market=market,
                                side=order_side,
                                amount_of_synthetic=quantity,
                                price=execution_price,
                                expire_time=expire_time,
                                time_in_force=TimeInForce.GTT,
                                reduce_only=False,
                                post_only=True,  # Enable Post Only to ensure Maker execution
                                starknet_domain=self.stark_config.starknet_domain,
                            )

                            self.logger.log(f"✅ Base order object created with settlement signature", "INFO")

                            # Create modified copy with conditional trigger
                            modified_order = base_order.model_copy(update={
                                'type': OrderType.CONDITIONAL,
                                'trigger': conditional_trigger
                            })

                            self.logger.log(f"✅ Created conditional order without TP", "INFO")

                        # DEBUG: Print modified order JSON
                        try:
                            modified_json = modified_order.to_api_request_json(exclude_none=True)
                            self.logger.log("=" * 60, "INFO")
                            self.logger.log("=== Modified Order JSON (with settlement) ===", "INFO")
                            self.logger.log(json.dumps(modified_json, default=str, indent=2), "INFO")
                            self.logger.log("=" * 60, "INFO")
                        except Exception as e:
                            self.logger.log(f"Failed to serialize modified order: {e}", "ERROR")

                        # Place the signed order
                        order_result = await self.perpetual_trading_client.orders.place_order(order=modified_order)

                    except Exception as e:
                        self.logger.log(f"❌ create_order_object failed: {e}", "ERROR")
                        self.logger.log(f"Exception type: {type(e).__name__}", "ERROR")
                        self.logger.log(f"Full traceback: {traceback.format_exc()}", "ERROR")
                        # Re-raise to stop execution - no fallback
                        raise
                else:
                    self.logger.log("❌ create_order_object not available", "ERROR")
                    raise ImportError("create_order_object not available - cannot place conditional orders")

            except Exception as exc:
                # No fallback - let the error propagate
                self.logger.log(f"❌ Failed to place conditional order: {repr(exc)}", "ERROR")
                raise

            if not order_result or not order_result.data or order_result.status != 'OK':
                error_msg = f'Failed to place conditional order: {order_result}'
                self.logger.log(f"❌ {error_msg}", "ERROR")
                return OrderResult(success=False, error_message=error_msg)

            order_id = order_result.data.id
            if not order_id:
                return OrderResult(success=False, error_message='No order ID in response')

            # Check order status
            await asyncio.sleep(0.1)  # Give it a moment to register
            order_info = await self.get_order_info(order_id)

            if order_info and order_info.status in ['NEW', 'OPEN', 'UNTRIGGERED', 'PARTIALLY_FILLED', 'FILLED']:
                self.logger.log(
                    f"✅ Conditional order placed: {order_id} (status: {order_info.status}) @ trigger={trigger_price}" +
                    (f", TP={take_profit_price}" if take_profit_price else ""),
                    "INFO"
                )
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    side=side,
                    size=quantity,
                    price=trigger_price,
                    status=order_info.status,
                    has_embedded_tp=False  # TP is disabled in this implementation (line 953), always place separately
                )
            else:
                return OrderResult(success=False, error_message=f'Unexpected order status: {order_info.status if order_info else "unknown"}')

        except Exception as e:
            self.logger.log(f"Error placing conditional order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return OrderResult(success=False, error_message=str(e))

    