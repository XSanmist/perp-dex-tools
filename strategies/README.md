# 交易策略

此目录包含各种自动交易机器人策略。

## 可用策略

### 1. 动量交易机器人 (Momentum Bot)

**目的**: 使用止损限价单跟随价格动量，避免"接飞刀"

**核心特性**:
- 使用止损限价单（条件单）而非被动限价单
- 跟随动量而非均值回归
- 成交后立即挂出止盈单
- 通过 ExchangeFactory 支持多交易所
- 可配置化的交易参数
- 所有交易事件的 Telegram 通知

**为什么需要动量交易机器人？**

传统的限价单是被动的 - 它们只在价格朝不利方向移动时成交（例如，价格下跌时买入）。这会导致"接飞刀"，即在下行动量中买入。

动量交易机器人通过使用止损限价单来解决这个问题，这些订单只在价格朝有利方向移动时触发，让你能够跟随动量而非对抗它。

---

## 动量交易机器人使用说明

### 配置

`MomentumConfig` 数据类定义了所有交易参数：

```python
from decimal import Decimal
from strategies import MomentumBot, MomentumConfig

config = MomentumConfig(
    # 市场配置
    ticker="ETH",                    # 交易对代号
    contract_id="ETH-USD-PERP",      # 交易所合约 ID
    exchange="extended",             # 交易所名称

    # 订单规模和价格
    quantity=Decimal("0.1"),         # 每次交易的订单大小
    tick_size=Decimal("0.01"),       # 最小价格增量

    # 入场策略（止损限价单）
    direction="buy",                 # "buy" 做多, "sell" 做空
    stop_price=Decimal("3500"),      # 触发价格
    limit_price=Decimal("3501"),     # 触发后的限价

    # 出场策略（止盈）
    take_profit_pct=Decimal("0.02"), # 止盈百分比（例如 0.02%）

    # 交易控制
    max_positions=1,                 # 最大并发持仓数
    wait_time=5                      # 周期间隔秒数
)
```

### 运行机器人

```python
import asyncio
from strategies import MomentumBot, MomentumConfig

async def main():
    config = MomentumConfig(
        # ... 你的配置
    )

    bot = MomentumBot(config)

    try:
        await bot.run()
    except KeyboardInterrupt:
        await bot.graceful_shutdown("用户请求关闭")

if __name__ == "__main__":
    asyncio.run(main())
```

### 交易流程

1. **入场**: 机器人挂出止损限价单
   - 当价格达到 `stop_price` 时订单触发
   - 以 `limit_price` 作为限价单执行

2. **止盈**: 入场成交后
   - 立即计算止盈价格
   - 做多: `entry_price * (1 + take_profit_pct/100)`
   - 做空: `entry_price * (1 - take_profit_pct/100)`
   - 挂出限价止盈单

3. **出场**: 等待止盈单成交

4. **重复**: 重置并开始新周期

### 环境变量

机器人需要这些环境变量：

```bash
# 交易所凭证（Extended 示例）
EXTENDED_VAULT=your_vault_address
EXTENDED_STARK_KEY_PRIVATE=your_private_key
EXTENDED_STARK_KEY_PUBLIC=your_public_key
EXTENDED_API_KEY=your_api_key

# Telegram 通知（可选）
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

### Telegram 通知

机器人会发送以下通知：

- **启动**: 配置摘要
- **仓位开启**: 入场价格、出场价格、数量
- **仓位关闭**: 交易摘要
- **错误**: 遇到的任何交易错误
- **关闭**: 最终状态

通知示例：
```
🚀 Momentum Bot - Position Opened

Ticker: ETH
Direction: BUY
Entry Price: 3501.00
Exit Price: 3501.70
Quantity: 0.1
Take Profit: 0.02%
```

### 日志记录

所有交易活动都会记录到：
- 控制台输出（实时）
- 日志文件在 `logs/{exchange}/{ticker}/`
  - `trade.log` - 所有交易活动
  - `transaction.csv` - 交易记录用于分析

### 当前限制

**V1 (当前版本)**:
- 仅支持做多方向
- 一次只有一个仓位
- 止损限价单目前使用普通限价单（等待交易所客户端更新）

**未来版本**:
- 做空方向支持
- 双向交易
- 多个并发仓位
- 真正的止损限价单实现
- 高级风险管理

### 示例：做多 ETH 动量交易

```python
config = MomentumConfig(
    ticker="ETH",
    contract_id="ETH-USD-PERP",
    exchange="extended",
    quantity=Decimal("0.5"),
    tick_size=Decimal("0.01"),
    direction="buy",
    stop_price=Decimal("3500"),      # 当 ETH 达到 3500 时买入
    limit_price=Decimal("3502"),     # 但不高于 3502
    take_profit_pct=Decimal("0.02"), # 在 +0.02% 时止盈
    max_positions=1,
    wait_time=5
)
```

这将会：
1. 等待 ETH 达到 $3500（上行动量）
2. 以 $3502 或更好的价格买入
3. 立即以 $3502.70 挂出卖单（0.02% 利润）
4. 重复循环

---

## 架构设计

### 模块结构

```
backend/strategies/
├── __init__.py           # 模块导出
├── README.md            # 本文件
└── momentum_bot.py      # 动量交易机器人实现
```

### 与交易所层集成

动量机器人使用 `ExchangeFactory` 模式支持多个交易所：

```python
from exchanges import ExchangeFactory

# 工厂自动创建正确的交易所客户端
exchange_client = ExchangeFactory.create_exchange(
    config.exchange,  # 例如 "extended"
    config
)
```

支持的交易所：
- extended
- edgex
- backpack
- paradex
- aster
- lighter
- grvt
- apex

### 设计原则

1. **清晰的模块分离**: 交易逻辑与交易所实现分离
2. **可配置参数**: 所有交易参数在配置数据类中
3. **多交易所支持**: 通过工厂模式与交易所无关
4. **全面的日志记录**: 详细的日志和交易记录
5. **错误处理**: 优雅的错误处理与通知
6. **Async/Await**: 完全异步支持并发操作

---

## 未来策略

此文件夹结构支持添加更多交易策略：

- `grid_bot.py` - 网格交易策略
- `arbitrage_bot.py` - 跨交易所套利
- `market_making_bot.py` - 做市策略
- 等等

每个策略应遵循相同的模式：
1. 配置数据类用于参数
2. 具有清晰交易逻辑的机器人类
3. Telegram 通知
4. 全面的日志记录
5. 多交易所支持
