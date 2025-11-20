"""
Base exchange client interface.
All exchange implementations should inherit from this class.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple, Type, Union, Callable
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from collections import deque
from time import time
from functools import wraps
import asyncio
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential


def track_rtt(func: Callable):
    """
    装饰器：自动追踪 API 调用的 RTT（Round-Trip Time）

    支持同步和异步函数，自动调用 self.record_rtt() 记录耗时
    """
    @wraps(func)
    async def async_wrapper(self, *args, **kwargs):
        start = time()
        try:
            result = await func(self, *args, **kwargs)
            return result
        finally:
            elapsed_ms = (time() - start) * 1000
            if hasattr(self, 'record_rtt'):
                self.record_rtt(elapsed_ms)

    @wraps(func)
    def sync_wrapper(self, *args, **kwargs):
        start = time()
        try:
            result = func(self, *args, **kwargs)
            return result
        finally:
            elapsed_ms = (time() - start) * 1000
            if hasattr(self, 'record_rtt'):
                self.record_rtt(elapsed_ms)

    # 检查是否是异步函数
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


def track_api_health(func: Callable):
    """
    装饰器：自动跟踪 API 调用的健康状态

    成功时调用 self.record_api_success()
    失败时调用 self.record_api_failure()
    支持同步和异步函数
    """
    @wraps(func)
    async def async_wrapper(self, *args, **kwargs):
        try:
            result = await func(self, *args, **kwargs)
            # API 调用成功
            if hasattr(self, 'record_api_success'):
                self.record_api_success()
            return result
        except Exception as e:
            # API 调用失败
            if hasattr(self, 'record_api_failure'):
                self.record_api_failure()
            raise

    @wraps(func)
    def sync_wrapper(self, *args, **kwargs):
        try:
            result = func(self, *args, **kwargs)
            # API 调用成功
            if hasattr(self, 'record_api_success'):
                self.record_api_success()
            return result
        except Exception as e:
            # API 调用失败
            if hasattr(self, 'record_api_failure'):
                self.record_api_failure()
            raise

    # 检查是否是异步函数
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


def query_retry(
    default_return: Any = None,
    exception_type: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,),
    max_attempts: int = 5,
    min_wait: float = 1,
    max_wait: float = 10,
    reraise: bool = False
):
    def retry_error_callback(retry_state: RetryCallState):
        print(f"Operation: [{retry_state.fn.__name__}] failed after {retry_state.attempt_number} retries, "
              f"exception: {str(retry_state.outcome.exception())}")
        return default_return

    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type(exception_type),
        retry_error_callback=retry_error_callback,
        reraise=reraise
    )


@dataclass
class OrderResult:
    """Standardized order result structure."""
    success: bool
    order_id: Optional[str] = None
    side: Optional[str] = None
    size: Optional[Decimal] = None
    price: Optional[Decimal] = None
    status: Optional[str] = None
    error_message: Optional[str] = None
    filled_size: Optional[Decimal] = None
    has_embedded_tp: bool = False  # True if order includes embedded TP


@dataclass
class OrderInfo:
    """Standardized order information structure."""
    order_id: str
    side: str
    size: Decimal
    price: Decimal
    status: str
    filled_size: Decimal = 0.0
    remaining_size: Decimal = 0.0
    cancel_reason: str = ''


class BaseExchangeClient(ABC):
    """Base class for all exchange clients."""

    # 需要自动追踪 RTT 的方法列表
    _RTT_TRACKED_METHODS = [
        'place_open_order',
        'place_close_order',
        'place_market_order',
        'cancel_order',
        'get_order_info',
        'get_active_orders',
        'get_account_positions',
        'get_signed_position',
        'get_contract_attributes',
    ]

    # 需要自动追踪健康状态的方法列表（关键 API 调用）
    _HEALTH_TRACKED_METHODS = [
        'fetch_bbo_prices',
        'place_open_order',
        'place_close_order',
        'place_post_only_order',
        'place_market_order',
        'cancel_order',
        'get_order_info',
        'get_active_orders',
        'get_account_positions',
        'get_signed_position',
    ]

    def __init_subclass__(cls, **kwargs):
        """
        当子类被创建时，自动为指定方法添加 RTT 追踪和健康监控
        """
        super().__init_subclass__(**kwargs)

        # 添加 RTT 追踪
        for method_name in cls._RTT_TRACKED_METHODS:
            if hasattr(cls, method_name):
                original_method = getattr(cls, method_name)
                # 只装饰还没被装饰过的方法
                if not getattr(original_method, '_rtt_tracked', False):
                    wrapped = track_rtt(original_method)
                    wrapped._rtt_tracked = True
                    setattr(cls, method_name, wrapped)

        # 添加健康状态追踪
        for method_name in cls._HEALTH_TRACKED_METHODS:
            if hasattr(cls, method_name):
                original_method = getattr(cls, method_name)
                # 只装饰还没被装饰过的方法
                if not getattr(original_method, '_health_tracked', False):
                    wrapped = track_api_health(original_method)
                    wrapped._health_tracked = True
                    setattr(cls, method_name, wrapped)

    def __init__(self, config: Dict[str, Any]):
        """Initialize the exchange client with configuration."""
        self.config = config
        self._validate_config()

        # RTT tracking - 只保留最近 30 次请求的耗时
        self._rtt_samples = deque(maxlen=30)
        self._last_rtt = None  # 最近一次请求的 RTT

        # API 健康状态跟踪
        self.last_api_success_time = time()  # 最后一次 API 成功时间
        self.consecutive_api_failures = 0  # 连续 API 失败次数

    def round_to_tick(self, price) -> Decimal:
        price = Decimal(price)

        tick = self.config.tick_size
        # quantize forces price to be a multiple of tick
        return price.quantize(tick, rounding=ROUND_HALF_UP)

    def record_rtt(self, rtt_ms: float) -> None:
        """
        记录 API 请求的 RTT（毫秒）

        Args:
            rtt_ms: Round-trip time in milliseconds
        """
        self._last_rtt = rtt_ms
        self._rtt_samples.append(rtt_ms)

    def get_avg_rtt(self) -> Optional[float]:
        """
        获取平均 RTT（毫秒）

        Returns:
            平均 RTT，如果没有数据则返回 None
        """
        if not self._rtt_samples:
            return None
        return sum(self._rtt_samples) / len(self._rtt_samples)

    def get_rtt_stats(self) -> Dict[str, Optional[float]]:
        """
        获取 RTT 统计信息

        Returns:
            包含 avg, min, max, last, count 的字典
        """
        if not self._rtt_samples:
            return {
                'avg': None,
                'min': None,
                'max': None,
                'last': self._last_rtt,
                'count': 0
            }

        samples = list(self._rtt_samples)
        return {
            'avg': sum(samples) / len(samples),
            'min': min(samples),
            'max': max(samples),
            'last': self._last_rtt,
            'count': len(samples)
        }

    def record_api_success(self) -> None:
        """
        记录 API 调用成功

        由 track_api_health 装饰器自动调用
        """
        self.last_api_success_time = time()
        self.consecutive_api_failures = 0

    def record_api_failure(self) -> None:
        """
        记录 API 调用失败

        由 track_api_health 装饰器自动调用
        """
        self.consecutive_api_failures += 1

    def get_api_health_status(self) -> Dict[str, any]:
        """
        获取 API 健康状态

        Returns:
            包含 last_success_time, consecutive_failures, time_since_last_success 的字典
        """
        time_since_last_success = time() - self.last_api_success_time
        return {
            'last_success_time': self.last_api_success_time,
            'consecutive_failures': self.consecutive_api_failures,
            'time_since_last_success': time_since_last_success
        }

    @abstractmethod
    def _validate_config(self) -> None:
        """Validate the exchange-specific configuration."""
        pass

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the exchange (WebSocket, etc.)."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the exchange."""
        pass

    @abstractmethod
    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order."""
        pass

    @abstractmethod
    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order."""
        pass

    @abstractmethod
    async def place_market_order(self, contract_id: str, quantity: Decimal, side: str, price_offset: Decimal = Decimal('0.0002')) -> OrderResult:
        """
        Place a market order or aggressive limit order for immediate execution.

        This order should execute immediately as a taker (non-post-only).
        Implementations can use native market orders or aggressive limit pricing.

        Args:
            contract_id: Contract identifier
            quantity: Order quantity
            side: Order side ('buy' or 'sell')
            price_offset: Price offset from BBO as a decimal (default: 0.0002 = 0.02%)
                         For buy orders: price = best_ask * (1 + price_offset)
                         For sell orders: price = best_bid * (1 - price_offset)

        Returns:
            OrderResult with order details
        """
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order."""
        pass

    @abstractmethod
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information."""
        pass

    @abstractmethod
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract."""
        pass

    @abstractmethod
    async def get_account_positions(self) -> Decimal:
        """Get account positions."""
        pass

    @abstractmethod
    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        pass

    @abstractmethod
    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        pass
