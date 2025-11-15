# 交易所止损限价单支持情况

本文档总结各交易所对止损限价单（Stop-Limit Order）的支持情况。

## 📊 支持情况总结

| 交易所 | 止损限价单支持 | 订单类型 | 状态 |
|--------|--------------|----------|------|
| **Lighter** | ✅ **完全支持** | `ORDER_TYPE_STOP_LOSS_LIMIT` (3) | 已验证 |
| **Extended** | ✅ **完全支持** | `CONDITIONAL` + trigger | 已验证 |
| EdgeX | ⚠️ 待验证 | - | 未检查 |
| Backpack | ⚠️ 待验证 | - | 未检查 |
| Paradex | ⚠️ 待验证 | - | 未检查 |
| Aster | ⚠️ 待验证 | - | 未检查 |
| GRVT | ⚠️ 待验证 | - | 未检查 |
| Apex | ⚠️ 待验证 | - | 未检查 |

---

## 🔍 详细信息

### 1. Lighter 交易所

**✅ 完全支持止损限价单**

#### 订单类型常量
```python
ORDER_TYPE_LIMIT = 0              # 普通限价单
ORDER_TYPE_MARKET = 1             # 市价单
ORDER_TYPE_STOP_LOSS = 2          # 止损单
ORDER_TYPE_STOP_LOSS_LIMIT = 3    # 止损限价单 ⭐
ORDER_TYPE_TAKE_PROFIT = 4        # 止盈单
ORDER_TYPE_TAKE_PROFIT_LIMIT = 5  # 止盈限价单
ORDER_TYPE_TWAP = 6               # TWAP 订单
```

#### 创建止损限价单示例
```python
from lighter import SignerClient

# 初始化客户端
client = SignerClient(
    url="https://mainnet.zklighter.elliot.ai",
    private_key=api_key_private_key,
    account_index=account_index,
    api_key_index=api_key_index,
)

# 创建止损限价单
# 当价格达到 trigger_price 时，以 price 作为限价单执行
order_result = await client.create_order(
    market_index=market_id,
    client_order_index=client_order_id,
    base_amount=base_amount_int,
    price=limit_price_int,              # 限价单执行价格
    is_ask=True,                        # True=卖, False=买
    order_type=client.ORDER_TYPE_STOP_LOSS_LIMIT,  # 止损限价单类型
    time_in_force=0,                    # 0=GTT, 1=IOC, 2=POST_ONLY
    reduce_only=False,
    trigger_price=stop_price_int,       # 触发价格 ⭐
    order_expiry=-1,
    nonce=-1,
)
```

#### 订单类型字段验证
Order 模型的 `type` 字段支持的值：
- `'limit'`
- `'market'`
- `'stop-loss'`
- `'stop-loss-limit'` ⭐ **止损限价单**
- `'take-profit'`
- `'take-profit-limit'`
- `'twap'`
- `'twap-sub'`
- `'liquidation'`

---

### 2. Extended 交易所

**✅ 完全支持条件订单（Conditional Order）作为止损限价单**

#### 订单类型
```python
from x10.perpetual.orders import OrderType, OrderTriggerDirection, OrderTriggerPriceType, OrderPriceType

# 订单类型枚举
class OrderType(StrEnum):
    LIMIT = "LIMIT"
    CONDITIONAL = "CONDITIONAL"  # ⭐ 条件订单（可用作止损限价单）
    MARKET = "MARKET"
    TPSL = "TPSL"
```

#### 触发方向
```python
class OrderTriggerDirection(StrEnum):
    UP = "UP"      # 向上突破触发（用于做多止损限价单）
    DOWN = "DOWN"  # 向下突破触发（用于做空止损限价单）
```

#### 触发价格类型
```python
class OrderTriggerPriceType(StrEnum):
    MARK = "MARK"    # 标记价格
    INDEX = "INDEX"  # 指数价格
    LAST = "LAST"    # 最新成交价
```

#### 执行价格类型
```python
class OrderPriceType(StrEnum):
    LIMIT = "LIMIT"   # 限价单
    MARKET = "MARKET" # 市价单
```

#### 创建止损限价单示例
```python
from x10.perpetual.trading_client import PerpetualTradingClient
from x10.perpetual.orders import (
    OrderType,
    OrderSide,
    TimeInForce,
    OrderTriggerDirection,
    OrderTriggerPriceType,
    OrderPriceType,
    CreateOrderConditionalTriggerModel
)

# 做多止损限价单：当价格向上突破 3500 时，以 3501 限价买入
trigger = CreateOrderConditionalTriggerModel(
    trigger_price=Decimal("3500"),                    # 触发价格
    trigger_price_type=OrderTriggerPriceType.LAST,   # 使用最新成交价
    direction=OrderTriggerDirection.UP,               # 向上突破触发
    execution_price_type=OrderPriceType.LIMIT         # 触发后执行限价单
)

order = await client.place_order(
    market="ETH-USD",
    order_type=OrderType.CONDITIONAL,  # ⭐ 条件订单
    side=OrderSide.BUY,
    size=Decimal("0.1"),
    price=Decimal("3501"),             # 限价单执行价格
    trigger=trigger,                   # ⭐ 触发条件
    time_in_force=TimeInForce.GTC,
    reduce_only=False,
    post_only=False,
)
```

#### 订单模型字段
```python
class NewOrderModel(X10BaseModel):
    id: str
    market: str
    type: OrderType
    side: OrderSide
    qty: Decimal
    price: Decimal
    reduce_only: bool = False
    post_only: bool = False
    time_in_force: TimeInForce
    expiry_epoch_millis: int
    fee: Decimal
    nonce: Decimal
    self_trade_protection_level: SelfTradeProtectionLevel
    cancel_id: Optional[str] = None
    settlement: Optional[StarkSettlementModel] = None
    trigger: Optional[CreateOrderConditionalTriggerModel] = None  # ⭐ 触发条件
    tp_sl_type: Optional[OrderTpslType] = None
    take_profit: Optional[CreateOrderTpslTriggerModel] = None
    stop_loss: Optional[CreateOrderTpslTriggerModel] = None
    debugging_amounts: Optional[StarkDebuggingOrderAmountsModel] = None
    builderFee: Optional[Decimal] = None
    builderId: Optional[int] = None
```

---

## 💡 使用场景对比

### 做多（Buy）止损限价单

**目标**: 当价格向上突破 3500 时，以 3501 限价买入

#### Lighter
```python
await client.create_order(
    market_index=market_id,
    base_amount=base_amount_int,
    price=3501_00000000,              # 限价 3501
    is_ask=False,                     # Buy
    order_type=client.ORDER_TYPE_STOP_LOSS_LIMIT,
    trigger_price=3500_00000000,      # 触发价 3500
    # 当价格 >= 3500 时触发，然后以 3501 限价买入
)
```

#### Extended
```python
trigger = CreateOrderConditionalTriggerModel(
    trigger_price=Decimal("3500"),
    trigger_price_type=OrderTriggerPriceType.LAST,
    direction=OrderTriggerDirection.UP,  # 向上突破
    execution_price_type=OrderPriceType.LIMIT
)

await client.place_order(
    market="ETH-USD",
    order_type=OrderType.CONDITIONAL,
    side=OrderSide.BUY,
    size=Decimal("0.1"),
    price=Decimal("3501"),
    trigger=trigger,
)
```

### 做空（Sell）止损限价单

**目标**: 当价格向下突破 3500 时，以 3499 限价卖出

#### Lighter
```python
await client.create_order(
    market_index=market_id,
    base_amount=base_amount_int,
    price=3499_00000000,              # 限价 3499
    is_ask=True,                      # Sell
    order_type=client.ORDER_TYPE_STOP_LOSS_LIMIT,
    trigger_price=3500_00000000,      # 触发价 3500
    # 当价格 <= 3500 时触发，然后以 3499 限价卖出
)
```

#### Extended
```python
trigger = CreateOrderConditionalTriggerModel(
    trigger_price=Decimal("3500"),
    trigger_price_type=OrderTriggerPriceType.LAST,
    direction=OrderTriggerDirection.DOWN,  # 向下突破
    execution_price_type=OrderPriceType.LIMIT
)

await client.place_order(
    market="ETH-USD",
    order_type=OrderType.CONDITIONAL,
    side=OrderSide.SELL,
    size=Decimal("0.1"),
    price=Decimal("3499"),
    trigger=trigger,
)
```

---

## 🚀 动量交易机器人实现建议

基于以上分析，**Lighter** 和 **Extended** 都完全支持止损限价单，可以直接用于动量交易策略。

### 推荐实现方案

1. **优先使用 Lighter**
   - API 更简单直观
   - 直接支持 `ORDER_TYPE_STOP_LOSS_LIMIT`
   - 一个参数控制触发价格

2. **Extended 作为备选**
   - 功能更强大（支持多种触发价格类型）
   - 需要配置触发方向和价格类型
   - 适合更复杂的策略

### 代码实现建议

可以在 `BaseExchangeClient` 中添加抽象方法：

```python
@abstractmethod
async def place_stop_limit_order(
    self,
    contract_id: str,
    quantity: Decimal,
    stop_price: Decimal,
    limit_price: Decimal,
    side: str,
    trigger_direction: str = "up"
) -> OrderResult:
    """
    Place a stop-limit order.

    Args:
        contract_id: Contract identifier
        quantity: Order size
        stop_price: Trigger price
        limit_price: Limit price after trigger
        side: 'buy' or 'sell'
        trigger_direction: 'up' or 'down'

    Returns:
        OrderResult with order details
    """
    pass
```

然后在 `LighterClient` 和 `ExtendedClient` 中分别实现。

---

## ✅ 结论

**Lighter** 和 **Extended** 都完全支持止损限价单，可以立即用于动量交易机器人的实现。

**推荐优先级**:
1. ✅ **Lighter** - 简单直接，适合快速实现
2. ✅ **Extended** - 功能强大，适合复杂策略
3. ⚠️ 其他交易所需要进一步验证

动量交易机器人可以直接在这两个交易所上实现真正的止损限价单功能！
