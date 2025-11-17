"""
双交易所对冲交易模块

支持任意两个交易所之间的对冲交易策略

主要特性:
- 支持任意两个交易所的对冲组合
- 主交易所使用 post-only 订单（maker 费率）
- 副交易所使用 IOC 市价单即时对冲（taker 费率）
- WebSocket 实时订单更新，触发增量对冲
- 增量成交跟踪，避免重复对冲
- 三步验证机制确保仓位平衡
- Telegram 通知关键事件

作者: Sanmist - X
创建时间: 2025-11
"""

import asyncio
import json
import logging
import os
from decimal import Decimal
from typing import Optional, Callable
from dataclasses import dataclass
from datetime import datetime

from exchanges.factory import ExchangeFactory
from exchanges.base import BaseExchangeClient, OrderResult
from helpers.telegram_bot import TelegramBot


class ExchangeConfig:
    """交易所配置包装类，用于将字典转换为对象属性访问"""

    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


@dataclass
class HedgeConfig:
    """对冲交易配置

    Attributes:
        primary_exchange: 主交易所名称 (使用 post-only 订单)
        secondary_exchange: 副交易所名称 (使用 IOC 市价单)
        ticker: 交易对符号，如 SOL-PERP, ETH-PERP
        quantity: 单次交易数量
        iterations: 交易轮数，默认 1
        order_timeout: 订单超时时间（秒），默认 30
        position_check_interval: 仓位检查间隔（秒），默认 5
        wait_after_open: 开仓后等待时间（秒），用于确保对冲完成，默认 10
        balance_check_retries: 仓位平衡检查重试次数，默认 3
        enable_telegram: 是否启用 Telegram 通知，默认 True
        task_id: 任务 ID，用于状态文件追踪
    """
    # 交易所配置
    primary_exchange: str
    secondary_exchange: str
    ticker: str

    # 交易参数
    quantity: Decimal
    iterations: int = 1

    # 超时配置
    order_timeout: int = 30
    position_check_interval: int = 5
    wait_after_open: int = 10
    balance_check_retries: int = 3

    # 通知配置
    enable_telegram: bool = True

    # 任务追踪
    task_id: str = ''


class DualExchangeHedge:
    """双交易所对冲交易策略

    交易流程:
        Step 1: 在主交易所下 post-only 订单，等待成交
        Step 2: WebSocket 监听成交，触发副交易所 IOC 对冲
        Step 3: 验证仓位平衡（三步验证机制）
        Step 4: 平仓并清理残余仓位

    关键机制:
        - 增量成交跟踪: 只对新增成交量进行对冲
        - 订单重挂: 价格不是最优时自动取消重挂
        - 三步验证: 等待对冲完成 -> 数据同步 -> 多次仓位检查
        - 容错处理: 对冲失败时 30 秒内持续重试
    """

    def __init__(self, config: HedgeConfig):
        self.config = config
        self.logger = logging.getLogger(
            f"DualHedge.{config.primary_exchange}-{config.secondary_exchange}"
        )

        # Telegram 配置
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

        # 交易所客户端（延迟初始化）
        self.primary_client: Optional[BaseExchangeClient] = None
        self.secondary_client: Optional[BaseExchangeClient] = None

        # 合约 ID
        self.primary_contract_id: str = ''
        self.secondary_contract_id: str = ''

        # 成交追踪
        self.current_primary_order_id: Optional[str] = None  # 当前主交易所订单 ID
        self.primary_filled_qty = Decimal('0')
        self.secondary_filled_qty = Decimal('0')
        self.total_hedged_qty = Decimal('0')  # 累计已对冲数量（防止重复对冲）
        self.cumulative_filled_qty = Decimal('0')  # 全局累计成交量（跨订单追踪）

        # 订单级别的成交去重追踪（防止 WebSocket 消息乱序/重复）
        # 格式: {order_id: 已处理的成交量}
        self.order_processed_fills: dict[str, Decimal] = {}

        # 当前订单价格（用于计算交易量）
        self.current_order_price: Optional[Decimal] = None

        # P&L 统计
        self.pnl_tracker = {
            'initial_balance_primary': None,  # 初始余额（主交易所）
            'initial_balance_secondary': None,  # 初始余额（副交易所）
            'current_balance_primary': None,  # 当前余额（主交易所）
            'current_balance_secondary': None,  # 当前余额（副交易所）
            'pnl_primary': Decimal('0'),  # 主交易所盈亏
            'pnl_secondary': Decimal('0'),  # 副交易所盈亏
            'total_pnl': Decimal('0'),  # 总盈亏
            'total_volume_usd': Decimal('0'),  # 总交易量（USDT）
        }

        # 控制标志
        self.is_running = False
        self.should_stop = False
        self.hedging_lock = asyncio.Lock()
        self.disable_hedging = False
        self.hedge_completed_event = asyncio.Event()
        self.hedge_in_progress = False

        # WebSocket 事件（用于订单取消确认）
        self.primary_order_canceled_event = asyncio.Event()

        # 任务状态
        self.task_id = config.task_id
        self.current_iteration = 0
        self.start_time = datetime.now()

    async def update_status(self):
        """更新任务状态到共享状态文件

        查询两个交易所的实时仓位，更新到 logs/process_status.json
        供前端页面读取显示
        """
        if not self.task_id:
            return

        status_file = "logs/process_status.json"
        os.makedirs("logs", exist_ok=True)

        try:
            if os.path.exists(status_file):
                with open(status_file, 'r') as f:
                    status_data = json.load(f)
            else:
                status_data = {}
        except Exception:
            status_data = {}

        runtime_seconds = int((datetime.now() - self.start_time).total_seconds())

        # 查询实时仓位（非累计成交量）
        primary_pos = Decimal('0')
        secondary_pos = Decimal('0')

        if self.primary_client:
            try:
                pos = await self.primary_client.get_signed_position()
                if pos is not None:
                    primary_pos = pos
            except Exception as e:
                self.logger.debug(f"查询主交易所仓位失败: {e}")

        if self.secondary_client:
            try:
                pos = await self.secondary_client.get_signed_position()
                if pos is not None:
                    secondary_pos = pos
            except Exception as e:
                self.logger.debug(f"查询副交易所仓位失败: {e}")

        # 获取两个交易所的平均 RTT
        primary_rtt = None
        secondary_rtt = None

        if self.primary_client:
            primary_rtt = self.primary_client.get_avg_rtt()

        if self.secondary_client:
            secondary_rtt = self.secondary_client.get_avg_rtt()

        # 计算损耗率
        total_volume = float(self.pnl_tracker['total_volume_usd'])
        total_pnl = float(self.pnl_tracker['total_pnl'])
        cost_per_10k = None
        if total_volume >= 1:
            cost_per_10k = round((abs(total_pnl) / total_volume) * 10000, 2)

        status_data[self.task_id] = {
            'current_iteration': self.current_iteration,
            'total_iterations': self.config.iterations,
            'primary_position': float(primary_pos),
            'secondary_position': float(secondary_pos),
            'primary_exchange': self.config.primary_exchange,
            'secondary_exchange': self.config.secondary_exchange,
            'runtime_seconds': runtime_seconds,
            'primary_rtt_ms': round(primary_rtt, 1) if primary_rtt is not None else None,
            'secondary_rtt_ms': round(secondary_rtt, 1) if secondary_rtt is not None else None,
            'total_volume_usd': round(total_volume, 2) if total_volume > 0 else None,
            'total_pnl': round(total_pnl, 2),
            'cost_per_10k_usd': cost_per_10k,
        }

        try:
            with open(status_file, 'w') as f:
                json.dump(status_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"更新状态文件失败: {e}")

    async def _fetch_account_info(self) -> tuple:
        """
        从交易所 API 查询账户信息（包含 P&L）

        返回:
            (primary_info, secondary_info)
            每个 info 是一个字典: {
                'balance': Decimal,  # 账户余额
                'unrealized_pnl': Decimal,  # 未实现盈亏
                'position': Decimal,  # 当前持仓
            }
        """
        primary_info = {'balance': None, 'unrealized_pnl': Decimal('0'), 'position': Decimal('0')}
        secondary_info = {'balance': None, 'unrealized_pnl': Decimal('0'), 'position': Decimal('0')}

        try:
            # 查询主交易所账户信息
            if self.primary_client:
                try:
                    # 🔍 DEBUG: 打印完整的 API 响应数据
                    self.logger.info(f"🔍 [DEBUG] 正在查询主交易所 ({self.config.primary_exchange}) 的账户信息...")

                    # 如果是 GRVT，调用 get_account_summary 获取账户余额
                    if hasattr(self.primary_client, 'rest_client'):
                        try:
                            # 获取账户摘要(包含余额、权益等信息)
                            account_summary = self.primary_client.rest_client.get_account_summary(type="sub-account")

                            if isinstance(account_summary, dict):
                                # 提取关键字段
                                primary_info['balance'] = Decimal(str(account_summary.get('total_equity', '0')))
                                primary_info['unrealized_pnl'] = Decimal(str(account_summary.get('unrealized_pnl', '0')))

                                self.logger.debug(
                                    f"主交易所 (GRVT): total_equity={primary_info['balance']}, "
                                    f"unrealized_pnl={primary_info['unrealized_pnl']}"
                                )
                        except Exception as e:
                            self.logger.error(f"查询主交易所账户摘要失败: {e}")

                    # 获取持仓（带符号）
                    position = await self.primary_client.get_signed_position()
                    if position is not None:
                        primary_info['position'] = position

                except Exception as e:
                    self.logger.error(f"查询主交易所账户信息失败: {e}")

            # 查询副交易所账户信息
            if self.secondary_client:
                try:
                    # 🔍 DEBUG: 打印完整的 API 响应数据
                    self.logger.info(f"🔍 [DEBUG] 正在查询副交易所 ({self.config.secondary_exchange}) 的账户信息...")

                    # Extended使用X10 SDK，调用 get_balance() 获取账户余额
                    if hasattr(self.secondary_client, 'perpetual_trading_client'):
                        try:
                            # X10 SDK使用 get_balance() 方法 (通过 account 模块访问)
                            client = self.secondary_client.perpetual_trading_client

                            # 调用 account.get_balance() 获取余额信息
                            balance_response = await client.account.get_balance()

                            self.logger.debug(f"🔍 [DEBUG] Extended get_balance() 返回:")
                            self.logger.debug(f"🔍 [DEBUG] Response: {balance_response}")

                            # WrappedApiResponse 包含 .data 属性
                            if hasattr(balance_response, 'data') and balance_response.data:
                                balance_data = balance_response.data

                                # BalanceModel 包含: balance, equity, unrealised_pnl, available_for_trade, etc.
                                secondary_info['balance'] = Decimal(str(balance_data.equity))
                                secondary_info['unrealized_pnl'] = Decimal(str(balance_data.unrealised_pnl))

                                self.logger.debug(
                                    f"副交易所 (Extended): equity={secondary_info['balance']}, "
                                    f"unrealised_pnl={secondary_info['unrealized_pnl']}"
                                )
                        except Exception as e:
                            self.logger.error(f"查询Extended账户余额失败: {e}", exc_info=True)

                    # 获取持仓（带符号）
                    position = await self.secondary_client.get_signed_position()
                    if position is not None:
                        secondary_info['position'] = position

                except Exception as e:
                    self.logger.error(f"查询副交易所账户信息失败: {e}")

        except Exception as e:
            self.logger.error(f"查询账户信息失败: {e}")

        return primary_info, secondary_info

    async def _update_pnl(self):
        """
        更新 P&L 统计

        通过对比当前余额与初始余额计算盈亏
        """
        primary_info, secondary_info = await self._fetch_account_info()

        # 更新当前余额
        if primary_info['balance'] is not None:
            self.pnl_tracker['current_balance_primary'] = primary_info['balance']

        if secondary_info['balance'] is not None:
            self.pnl_tracker['current_balance_secondary'] = secondary_info['balance']

        # 计算盈亏（如果有初始值）
        if self.pnl_tracker['initial_balance_primary'] is not None and primary_info['balance'] is not None:
            self.pnl_tracker['pnl_primary'] = primary_info['balance'] - self.pnl_tracker['initial_balance_primary']

        if self.pnl_tracker['initial_balance_secondary'] is not None and secondary_info['balance'] is not None:
            self.pnl_tracker['pnl_secondary'] = secondary_info['balance'] - self.pnl_tracker['initial_balance_secondary']

        # 计算总盈亏
        self.pnl_tracker['total_pnl'] = (
            self.pnl_tracker['pnl_primary'] +
            self.pnl_tracker['pnl_secondary']
        )

    async def _init_pnl_tracking(self):
        """
        初始化 P&L 追踪（记录初始余额）
        """
        self.logger.info("📊 初始化 P&L 追踪...")

        primary_info, secondary_info = await self._fetch_account_info()

        if primary_info['balance'] is not None:
            self.pnl_tracker['initial_balance_primary'] = primary_info['balance']
            self.pnl_tracker['current_balance_primary'] = primary_info['balance']
            self.logger.info(f"主交易所初始余额: ${primary_info['balance']}")
        else:
            self.logger.warning("⚠️ 无法获取主交易所初始余额")

        if secondary_info['balance'] is not None:
            self.pnl_tracker['initial_balance_secondary'] = secondary_info['balance']
            self.pnl_tracker['current_balance_secondary'] = secondary_info['balance']
            self.logger.info(f"副交易所初始余额: ${secondary_info['balance']}")
        else:
            self.logger.warning("⚠️ 无法获取副交易所初始余额")

    def _get_pnl_summary(self) -> str:
        """
        获取 P&L 汇总字符串

        返回:
            格式化的 P&L 统计信息
        """
        # 计算损耗率
        volume_stats = ""
        total_volume = self.pnl_tracker['total_volume_usd']
        if total_volume > 0:
            volume_stats = f"  总交易量: ${total_volume:,.2f}\n"
            if total_volume >= Decimal('1'):
                cost_per_10k = (abs(self.pnl_tracker['total_pnl']) / total_volume) * Decimal('10000')
                volume_stats += f"  损耗率: ${cost_per_10k:.2f}/10k USDT\n"

        summary = (
            f"\n{'='*50}\n"
            f"📊 P&L 统计\n"
            f"{'='*50}\n"
            f"【主交易所 - {self.config.primary_exchange.upper()}】\n"
            f"  初始余额: ${self.pnl_tracker['initial_balance_primary'] or 0:,.2f}\n"
            f"  当前余额: ${self.pnl_tracker['current_balance_primary'] or 0:,.2f}\n"
            f"  盈亏: ${self.pnl_tracker['pnl_primary']:+,.2f}\n"
            f"\n"
            f"【副交易所 - {self.config.secondary_exchange.upper()}】\n"
            f"  初始余额: ${self.pnl_tracker['initial_balance_secondary'] or 0:,.2f}\n"
            f"  当前余额: ${self.pnl_tracker['current_balance_secondary'] or 0:,.2f}\n"
            f"  盈亏: ${self.pnl_tracker['pnl_secondary']:+,.2f}\n"
            f"\n"
            f"【总计】\n"
            f"  总盈亏: ${self.pnl_tracker['total_pnl']:+,.2f}\n"
            f"{volume_stats}"
            f"{'='*50}\n"
        )

        return summary

    async def initialize(self):
        """初始化交易所客户端

        步骤:
            1. 创建两个交易所的客户端实例
            2. 获取合约属性（contract_id 和 tick_size）
            3. 设置 WebSocket 订单更新回调
            4. 建立 WebSocket 连接
        """
        try:
            self.logger.info(
                f"初始化交易所: {self.config.primary_exchange} (主) + "
                f"{self.config.secondary_exchange} (副)"
            )

            # 创建主交易所客户端
            primary_config_dict = {
                'ticker': self.config.ticker,
                'quantity': self.config.quantity,
                'contract_id': '',
                'tick_size': Decimal('0.01'),
            }
            primary_config = ExchangeConfig(primary_config_dict)
            self.primary_client = ExchangeFactory.create_exchange(
                self.config.primary_exchange,
                primary_config
            )

            # 创建副交易所客户端
            secondary_config_dict = {
                'ticker': self.config.ticker,
                'quantity': self.config.quantity,
                'contract_id': '',
                'tick_size': Decimal('0.01'),
            }
            secondary_config = ExchangeConfig(secondary_config_dict)
            self.secondary_client = ExchangeFactory.create_exchange(
                self.config.secondary_exchange,
                secondary_config
            )

            # 获取合约属性（必须在设置 handler 之前，WebSocket 订阅需要 contract_id）
            self.logger.info(f"获取 {self.config.primary_exchange} 合约属性...")
            self.primary_contract_id, _ = await self.primary_client.get_contract_attributes()
            self.logger.info(f"主交易所合约 ID: {self.primary_contract_id}")

            self.logger.info(f"获取 {self.config.secondary_exchange} 合约属性...")
            self.secondary_contract_id, _ = await self.secondary_client.get_contract_attributes()
            self.logger.info(f"副交易所合约 ID: {self.secondary_contract_id}")

            # 设置订单更新处理器
            self.primary_client.setup_order_update_handler(self._on_primary_order_update)
            self.secondary_client.setup_order_update_handler(self._on_secondary_order_update)

            # 启动 WebSocket 连接
            await self.primary_client.connect()
            await self.secondary_client.connect()

            self.logger.info("交易所初始化完成")
            await self._send_notification(
                f"🚀 对冲交易启动\n主: {self.config.primary_exchange}\n"
                f"副: {self.config.secondary_exchange}\n交易对: {self.config.ticker}"
            )

        except Exception as e:
            error_msg = f"交易所初始化失败: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            await self._send_notification(f"❌ {error_msg}")
            raise

    async def _on_primary_order_update(self, order_data: dict):
        """主交易所订单更新回调（WebSocket 触发）

        订单级别成交去重对冲机制:
            1. 使用 order_processed_fills 字典追踪每个订单已处理的成交量
            2. 计算真正的新增成交 = 当前成交 - 该订单已处理成交
            3. 只对真正的新增成交进行对冲
            4. 立即更新已处理成交和累计对冲量（防止竞态条件）

        订单取消事件处理:
            - 监听 CANCELED/CANCELLED 状态
            - 触发 primary_order_canceled_event 事件
            - 支持事件驱动的订单取消确认（替代轮询）

        这种设计确保：
            - 防止 WebSocket 消息乱序或重复导致的重复对冲
            - 即使订单被取消重挂，也能正确追踪每个订单的成交
            - A 实际成交多少，B 就对冲多少
            - 订单取消确认延迟低（WebSocket 推送 vs 轮询）
        """
        try:
            self.logger.debug(f"收到主交易所订单更新: {order_data}")

            order_id = order_data.get('order_id')
            status = order_data.get('status')

            # ✅ 处理订单取消事件（事件驱动，替代轮询）
            if status in ['CANCELED', 'CANCELLED']:
                self.logger.info(f"📨 WebSocket: 订单 {order_id} 已取消")
                # 如果是当前追踪的订单，触发取消事件
                if order_id == self.current_primary_order_id:
                    self.primary_order_canceled_event.set()
                    self.logger.info(f"✅ 已触发取消事件 (order_id={order_id})")

            if self.disable_hedging:
                self.logger.debug("对冲已禁用，跳过成交处理")
                return

            filled_qty = Decimal(str(order_data.get('filled_size', 0)))

            # 获取该订单已处理的成交量（默认为 0）
            processed_qty = self.order_processed_fills.get(order_id, Decimal('0'))

            # 计算真正的新增成交（去重逻辑）
            incremental_fill = filled_qty - processed_qty

            self.logger.info(
                f"订单 {order_id} 成交更新: "
                f"当前成交={filled_qty}, "
                f"已处理={processed_qty}, "
                f"增量={incremental_fill}, "
                f"全局累计={self.cumulative_filled_qty}, "
                f"已对冲={self.total_hedged_qty}"
            )

            # 只处理真正的新增成交
            if incremental_fill > Decimal('0'):
                # 立即更新该订单的已处理成交量（防止重复处理）
                self.order_processed_fills[order_id] = filled_qty

                # 更新全局累计成交量
                self.cumulative_filled_qty += incremental_fill

                # 记录交易量（数量 × 价格 = USDT 交易额）
                if self.current_order_price is not None:
                    volume_usd = incremental_fill * self.current_order_price
                    self.pnl_tracker['total_volume_usd'] += volume_usd
                    self.logger.debug(
                        f"📊 交易量统计: {incremental_fill} × {self.current_order_price} = ${volume_usd}, "
                        f"累计: ${self.pnl_tracker['total_volume_usd']}"
                    )

                # 计算需要对冲的量 = 全局累计成交 - 已对冲
                hedge_needed = self.cumulative_filled_qty - self.total_hedged_qty

                self.logger.info(
                    f"✅ 检测到新增成交: {incremental_fill}, "
                    f"全局累计成交: {self.cumulative_filled_qty}, "
                    f"需要对冲: {hedge_needed}"
                )

                if hedge_needed > Decimal('0'):
                    # 更新 primary_filled_qty 为全局累计成交（供状态显示）
                    self.primary_filled_qty = self.cumulative_filled_qty

                    # 立即更新累计对冲量（防止竞态条件）
                    # 在对冲执行之前更新，避免并发的 WebSocket 消息导致重复对冲
                    self.total_hedged_qty = self.cumulative_filled_qty

                    self.logger.info(f"🔄 触发对冲 {hedge_needed}，方向: {order_data.get('side')}")
                    await self._execute_hedge(hedge_needed, order_data.get('side'))
                    self.logger.info("✅ 对冲执行完毕")

                    await self.update_status()
                else:
                    self.logger.warning(
                        f"⚠️ 异常情况: 有新增成交但无需对冲 "
                        f"(cumulative={self.cumulative_filled_qty}, hedged={self.total_hedged_qty})"
                    )
            elif incremental_fill < Decimal('0'):
                # 检测到异常：成交量减少（可能是消息乱序）
                self.logger.warning(
                    f"⚠️ 订单 {order_id} 成交量异常减少: "
                    f"当前={filled_qty}, 已处理={processed_qty}, "
                    f"增量={incremental_fill}"
                )
                self.logger.warning("   可能是 WebSocket 消息乱序，跳过处理")
            else:
                # incremental_fill == 0，重复消息
                self.logger.debug(
                    f"订单 {order_id} 无新增成交（重复消息或状态更新）"
                )

        except Exception as e:
            self.logger.error(f"处理主交易所订单更新失败: {e}", exc_info=True)

    async def _on_secondary_order_update(self, order_data: dict):
        """副交易所订单更新回调（WebSocket 触发）

        用途:
            - 追踪对冲订单的成交情况
            - 检测订单拒绝或取消
            - 更新状态文件供前端显示
        """
        try:
            order_id = order_data.get('order_id', 'unknown')
            status = order_data.get('status', 'unknown')
            filled_qty = Decimal(str(order_data.get('filled_size', 0)))

            self.logger.info(
                f"📨 副交易所订单更新: ID={order_id}, 状态={status}, 成交={filled_qty}"
            )

            if status in ['REJECTED', 'CANCELED', 'CANCELLED']:
                self.logger.warning(f"⚠️ 副交易所订单被拒绝/取消: {order_data}")

            self.secondary_filled_qty = filled_qty
            await self.update_status()

        except Exception as e:
            self.logger.error(f"处理副交易所订单更新失败: {e}", exc_info=True)

    async def _execute_hedge(self, quantity: Decimal, primary_side: str):
        """执行对冲交易（IOC 市价单）

        Args:
            quantity: 需要对冲的数量
            primary_side: 主交易所订单方向（buy/sell）

        逻辑:
            - 主交易所买入 → 副交易所卖出
            - 主交易所卖出 → 副交易所买入
            - 使用 IOC 市价单确保快速成交
            - 追踪部分成交，只对冲剩余未成交部分

        重试机制:
            - 30 秒内持续重试
            - 追踪累计成交量，避免重复对冲
            - 超时后通知但不中断主流程
        """
        async with self.hedging_lock:
            try:
                self.hedge_in_progress = True
                self.hedge_completed_event.clear()

                hedge_side = 'sell' if primary_side == 'buy' else 'buy'
                self.logger.info(
                    f"执行对冲: {hedge_side} {quantity} on "
                    f"{self.config.secondary_exchange}"
                )

                self.secondary_filled_qty = Decimal('0')

                # 追踪本次对冲的累计成交量（防止部分成交导致重复对冲）
                cumulative_hedged = Decimal('0')

                # 30 秒内持续重试
                timeout = 30
                start_time = asyncio.get_event_loop().time()
                attempt = 0
                success = False

                while True:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed > timeout:
                        error_msg = (
                            f"⚠️ 对冲失败 (30秒超时)\n目标数量: {quantity}\n"
                            f"已成交: {cumulative_hedged}\n剩余: {quantity - cumulative_hedged}\n"
                            f"方向: {hedge_side}\n总尝试次数: {attempt}"
                        )
                        self.logger.error(error_msg)
                        await self._send_notification(error_msg)
                        break

                    # 计算剩余未对冲数量
                    remaining_qty = quantity - cumulative_hedged

                    if remaining_qty <= Decimal('0'):
                        self.logger.info(f"✅ 对冲完全成交: {cumulative_hedged}/{quantity} (尝试 {attempt} 次)")
                        success = True
                        break

                    attempt += 1
                    remaining_time = timeout - elapsed
                    self.logger.info(
                        f"对冲尝试 #{attempt} (剩余时间: {remaining_time:.1f}秒, 剩余数量: {remaining_qty})"
                    )

                    try:
                        # 只下剩余未成交的数量
                        result = await self._place_aggressive_order(
                            exchange_client=self.secondary_client,
                            contract_id=self.secondary_contract_id,
                            quantity=remaining_qty,
                            side=hedge_side,
                            price_offset=Decimal('0.005')  # 0.5% 偏移，确保立即成交
                        )

                        if result.success:
                            self.logger.info(f"对冲订单已下单: ID {result.order_id}")

                            # 等待订单成交并获取成交信息
                            filled_info = await self._wait_for_hedge_fill_with_info(
                                result.order_id, remaining_qty
                            )

                            if filled_info['success']:
                                # 累加本次成交量
                                filled_qty = filled_info['filled_qty']
                                cumulative_hedged += filled_qty
                                self.logger.info(
                                    f"本次成交: {filled_qty}, 累计成交: {cumulative_hedged}/{quantity}"
                                )

                                # 检查是否完全成交
                                if cumulative_hedged >= quantity:
                                    self.logger.info(
                                        f"✅ 对冲完全成交: {cumulative_hedged} (尝试 {attempt} 次)"
                                    )
                                    success = True
                                    break
                                else:
                                    # 部分成交，继续重试剩余部分
                                    self.logger.warning(
                                        f"⚠️ 部分成交: {filled_qty}/{remaining_qty}, "
                                        f"剩余 {quantity - cumulative_hedged} 将继续对冲"
                                    )
                            else:
                                # 订单失败，取消后重试
                                self.logger.warning(f"对冲未成交 (尝试 #{attempt})")
                                try:
                                    await self.secondary_client.cancel_order(
                                        result.order_id
                                    )
                                    self.logger.info(f"已取消未成交对冲订单: {result.order_id}")
                                except Exception as cancel_err:
                                    self.logger.warning(f"取消订单失败: {cancel_err}")
                        else:
                            self.logger.warning(
                                f"对冲下单失败 (尝试 #{attempt}): {result.error_message}"
                            )

                    except Exception as e:
                        self.logger.error(f"对冲执行异常 (尝试 #{attempt}): {e}")

                    await asyncio.sleep(0.5)

            except Exception as e:
                self.logger.error(f"对冲执行严重错误: {e}", exc_info=True)
                await self._send_notification(f"❌ 对冲异常: {str(e)}")
            finally:
                self.hedge_in_progress = False
                self.hedge_completed_event.set()
                self.logger.info(f"对冲流程结束 (成功: {success})")

    async def run_strategy(self):
        """
        运行对冲策略主流程

        流程:
        1. 初始化交易所
        2. 循环执行交易轮次
        3. 每轮: 开仓 → 等待成交 → 平仓 → 清理
        4. 完成后检查并清理残余仓位
        """
        self.is_running = True
        completed_iterations = 0  # 跟踪实际完成的轮数
        stop_reason = "初始化失败"  # 默认停止原因（如果初始化阶段失败）

        try:
            await self.initialize()
            stop_reason = "未知原因"  # 初始化成功后，重置为未知原因

            # 初始化 P&L 追踪
            await self._init_pnl_tracking()

            # 计算状态同步间隔（每 5% 同步一次）
            total_iterations = self.config.iterations
            last_notified_pct = -1  # 上次通知的百分比

            for iteration in range(1, self.config.iterations + 1):
                if self.should_stop:
                    stop_reason = "用户中断"
                    self.logger.info("收到停止信号，中断交易")
                    break

                self.current_iteration = iteration  # 更新当前轮数

                # 计算当前进度百分比
                current_pct = int((iteration / total_iterations) * 100)

                # 判断是否需要同步状态和发送通知
                # 每 5% 发送一次通知 (0%, 5%, 10%, 15%, ...)
                pct_milestone = (current_pct // 5) * 5
                should_notify = (
                    iteration == 1 or  # 第一轮
                    iteration == total_iterations or  # 最后一轮
                    pct_milestone > last_notified_pct  # 达到新的 5% 里程碑
                )

                # 更新状态文件（每轮都更新，供前端实时显示）
                await self.update_status()

                self.logger.info(f"===== 开始第 {iteration}/{self.config.iterations} 轮交易 =====")

                # 在 5% 里程碑发送通知
                if should_notify:
                    # 查询仓位和延迟信息
                    primary_pos = Decimal('0')
                    secondary_pos = Decimal('0')

                    try:
                        pos = await self.primary_client.get_signed_position()
                        if pos is not None:
                            primary_pos = pos
                    except Exception:
                        pass

                    try:
                        pos = await self.secondary_client.get_signed_position()
                        if pos is not None:
                            secondary_pos = pos
                    except Exception:
                        pass

                    primary_rtt = self.primary_client.get_avg_rtt()
                    secondary_rtt = self.secondary_client.get_avg_rtt()

                    # 计算运行时间
                    runtime_seconds = int((datetime.now() - self.start_time).total_seconds())
                    hours = runtime_seconds // 3600
                    minutes = (runtime_seconds % 3600) // 60
                    seconds = runtime_seconds % 60

                    if hours > 0:
                        runtime_str = f"{hours}h{minutes}m{seconds}s"
                    elif minutes > 0:
                        runtime_str = f"{minutes}m{seconds}s"
                    else:
                        runtime_str = f"{seconds}s"

                    # 构建通知消息（P&L 从内存读取，已在每轮完成后更新）
                    rtt_primary_str = f"{primary_rtt:.1f}ms" if primary_rtt else "N/A"
                    rtt_secondary_str = f"{secondary_rtt:.1f}ms" if secondary_rtt else "N/A"

                    # 构建余额信息
                    balance_info = ""
                    if self.pnl_tracker['current_balance_primary'] is not None:
                        balance_info += f"\n余额: {self.config.primary_exchange}=${self.pnl_tracker['current_balance_primary']:,.2f}"
                        if self.pnl_tracker['current_balance_secondary'] is not None:
                            balance_info += f", {self.config.secondary_exchange}=${self.pnl_tracker['current_balance_secondary']:,.2f}"

                    # 构建交易量和损耗率信息
                    volume_info = ""
                    total_volume = self.pnl_tracker['total_volume_usd']
                    if total_volume > 0:
                        volume_info += f"\n交易量: ${total_volume:,.2f}"
                        # 计算每 10000u 的损耗
                        if total_volume >= Decimal('1'):  # 避免除以0
                            cost_per_10k = (abs(self.pnl_tracker['total_pnl']) / total_volume) * Decimal('10000')
                            volume_info += f"\n损耗率: ${cost_per_10k:.2f}/10k USDT"

                    notify_msg = (
                        f"📊 第 {iteration}/{self.config.iterations} 轮 ({current_pct}%)\n"
                        f"运行时间: {runtime_str}\n"
                        f"仓位: {self.config.primary_exchange}={primary_pos:.4f}, "
                        f"{self.config.secondary_exchange}={secondary_pos:.4f}{balance_info}\n"
                        f"RTT: {self.config.primary_exchange}={rtt_primary_str}, "
                        f"{self.config.secondary_exchange}={rtt_secondary_str}\n"
                        f"P&L: ${self.pnl_tracker['total_pnl']:+,.2f}{volume_info}"
                    )
                    await self._send_notification(notify_msg)
                    last_notified_pct = pct_milestone

                # 检查并重置状态
                state_ok = await self._verify_and_reset_state()
                if not state_ok:
                    stop_reason = "状态检查失败"
                    error_msg = "⚠️ 状态检查失败，存在未平仓位或异常状态，需要人工介入"
                    self.logger.error(error_msg)
                    await self._send_notification(f"❌ {error_msg}\n已中断交易，请检查仓位后手动处理")
                    break

                # Step 1: 开仓
                self.logger.info("Step 1: 开仓")
                success = await self._open_position('buy')
                if not success:
                    stop_reason = "开仓失败"
                    await self._handle_step_failure("开仓")
                    break  # 开仓失败，退出循环

                # Step 2: 平仓
                self.logger.info("Step 2: 平仓")
                success = await self._close_position('sell')
                if not success:
                    stop_reason = "平仓失败"
                    await self._handle_step_failure("平仓")
                    break  # 平仓失败，退出循环

                # Step 3: 清理残余仓位
                self.logger.info("Step 3: 清理残余仓位")
                await self._cleanup_residual_positions()

                completed_iterations = iteration  # 记录完成的轮数
                self.logger.info(f"===== 第 {iteration} 轮交易完成 =====")

                # 更新 P&L 到内存（每轮交易完成后更新，供 update_status 和里程碑通知读取）
                await self._update_pnl()

            # 最终状态检查
            await self._final_position_check()

            # 更新最终 P&L
            await self._update_pnl()

            # 打印 P&L 汇总到日志
            pnl_summary = self._get_pnl_summary()
            self.logger.info(pnl_summary)

            # 发送最终完成通知
            self.logger.info(f"最终统计: 完成轮数={completed_iterations}, 总轮数={self.config.iterations}, 停止原因={stop_reason}")

            # 构建最终余额信息
            final_balance_info = ""
            if self.pnl_tracker['current_balance_primary'] is not None:
                final_balance_info = f"\n余额: {self.config.primary_exchange}=${self.pnl_tracker['current_balance_primary']:,.2f}"
                if self.pnl_tracker['current_balance_secondary'] is not None:
                    final_balance_info += f", {self.config.secondary_exchange}=${self.pnl_tracker['current_balance_secondary']:,.2f}"

            # 构建最终交易量和损耗率信息
            final_volume_info = ""
            total_volume = self.pnl_tracker['total_volume_usd']
            if total_volume > 0:
                final_volume_info += f"\n交易量: ${total_volume:,.2f}"
                # 计算每 10000u 的损耗
                if total_volume >= Decimal('1'):  # 避免除以0
                    cost_per_10k = (abs(self.pnl_tracker['total_pnl']) / total_volume) * Decimal('10000')
                    final_volume_info += f"\n损耗率: ${cost_per_10k:.2f}/10k USDT"

            if completed_iterations == self.config.iterations:
                # 全部完成
                final_msg = (
                    f"✅ 对冲交易完成\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"完成轮数: {completed_iterations}/{self.config.iterations}\n"
                    f"状态: 全部完成{final_balance_info}\n"
                    f"总盈亏: ${self.pnl_tracker['total_pnl']:+,.2f}{final_volume_info}"
                )
            else:
                # 提前中断
                final_msg = (
                    f"⚠️ 对冲交易中断\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"完成轮数: {completed_iterations}/{self.config.iterations}\n"
                    f"完成度: {int(completed_iterations/self.config.iterations*100) if self.config.iterations > 0 else 0}%\n"
                    f"中断原因: {stop_reason}{final_balance_info}\n"
                    f"总盈亏: ${self.pnl_tracker['total_pnl']:+,.2f}{final_volume_info}"
                )

            self.logger.info(final_msg.replace("\n", " | "))
            await self._send_notification(final_msg)

        except Exception as e:
            error_msg = f"策略执行失败: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            await self._send_notification(f"❌ {error_msg}")
            raise

        finally:
            await self.cleanup()
            self.is_running = False

    async def _open_position(self, side: str) -> bool:
        """开仓步骤（Step 1）

        流程:
            1. 主交易所挂 post-only 订单（最优价格）
            2. WebSocket 监听成交，自动触发副交易所对冲
            3. 每 5 秒检查挂单价格，不是最优则取消重挂
            4. 三步验证确保对冲完成

        三步验证机制:
            Step A: 等待对冲流程完成（35秒超时）
            Step B: 等待数据同步（配置的 wait_after_open 秒数）
            Step C: 多次重试验证仓位平衡（默认 3 次，间隔 2 秒）

        Returns:
            True: 开仓成功且仓位平衡
            False: 开仓失败或仓位不平衡
        """
        try:
            self.logger.info(f"Step 1: 开仓 ({side} {self.config.quantity})")

            # 重置追踪变量（包括累计对冲量和全局累计成交量）
            self.primary_filled_qty = Decimal('0')
            self.secondary_filled_qty = Decimal('0')
            self.total_hedged_qty = Decimal('0')  # 重置累计对冲量
            self.cumulative_filled_qty = Decimal('0')  # 重置全局累计成交量
            self.order_processed_fills.clear()  # 清空订单成交去重字典

            # 等待订单完全成交（包含挂单和重挂逻辑）
            success = await self._wait_for_fill_with_repricing(side, self.config.quantity)
            if not success:
                self.logger.error("开仓失败：未能完全成交")
                return False

            self.logger.info(f"主交易所订单已完全成交: {self.primary_filled_qty}")

            # === 三步验证机制 ===
            self.logger.info("=" * 60)
            self.logger.info("🔍 [开仓验证] 等待副交易所对冲完成并验证仓位平衡...")
            self.logger.info("=" * 60)

            # Step A: 等待对冲流程完成
            if self.hedge_in_progress:
                self.logger.info("⏳ 等待对冲流程完成...")
                try:
                    await asyncio.wait_for(self.hedge_completed_event.wait(), timeout=35)
                    self.logger.info("✅ 对冲流程已完成")
                except asyncio.TimeoutError:
                    self.logger.warning("⚠️ 等待对冲流程超时 (35秒)")

            # Step B: 等待数据同步
            if self.config.wait_after_open > 0:
                self.logger.info(
                    f"⏳ 等待 {self.config.wait_after_open} 秒，确保数据同步..."
                )
                await asyncio.sleep(self.config.wait_after_open)

            # Step C: 多次重试验证仓位平衡
            max_retries = self.config.balance_check_retries
            retry_delay = 2
            is_balanced = False

            for attempt in range(1, max_retries + 1):
                self.logger.info(
                    f"🔍 仓位平衡检查 (尝试 {attempt}/{max_retries}), "
                    f"等待 {retry_delay}秒..."
                )
                await asyncio.sleep(retry_delay)

                is_balanced = await self._check_position_balance()

                if is_balanced:
                    self.logger.info(f"✅ 仓位平衡确认 (尝试 {attempt}/{max_retries})")
                    break
                else:
                    self.logger.warning(f"⚠️ 尝试 {attempt}/{max_retries}: 仓位仍不平衡")

            if not is_balanced:
                self.logger.error("❌ 开仓后仓位不平衡，对冲未完全完成")
                self.logger.error("❌ 为了安全，中断交易流程")
                await self._send_notification("❌ 开仓失败\n仓位不平衡\n请检查仓位")
                return False

            self.logger.info("=" * 60)
            self.logger.info("✅ 开仓完成并验证通过")
            self.logger.info(f"   主交易所成交: {self.primary_filled_qty}")
            self.logger.info(f"   副交易所成交: {self.secondary_filled_qty}")
            self.logger.info("=" * 60)
            return True

        except Exception as e:
            self.logger.error(f"开仓异常: {e}", exc_info=True)
            return False

    async def _close_position(self, side: str) -> bool:
        """平仓步骤（Step 2）

        流程:
            1. 主交易所挂 post-only 平仓订单
            2. WebSocket 监听成交，自动触发副交易所对冲平仓
            3. 每 5 秒检查挂单价格，不是最优则取消重挂
            4. 三步验证确保仓位清空

        三步验证机制:
            Step A: 等待对冲流程完成（35秒超时）
            Step B: 等待数据同步（配置的 wait_after_open 秒数）
            Step C: 多次重试验证仓位清空（默认 3 次，间隔 2 秒）

        Returns:
            True: 平仓成功（残余仓位交由 Step 3 清理）
            False: 平仓失败
        """
        try:
            self.logger.info(f"Step 2: 平仓 ({side} {self.config.quantity})")

            # 重置追踪变量（包括累计对冲量和全局累计成交量）
            self.primary_filled_qty = Decimal('0')
            self.secondary_filled_qty = Decimal('0')
            self.total_hedged_qty = Decimal('0')  # 重置累计对冲量
            self.cumulative_filled_qty = Decimal('0')  # 重置全局累计成交量
            self.order_processed_fills.clear()  # 清空订单成交去重字典

            # 等待订单完全成交（包含挂单和重挂逻辑）
            success = await self._wait_for_fill_with_repricing(side, self.config.quantity)
            if not success:
                self.logger.error("平仓失败：未能完全成交")
                return False

            self.logger.info(f"主交易所平仓订单已完全成交: {self.primary_filled_qty}")

            # === 三步验证机制 ===
            self.logger.info("=" * 60)
            self.logger.info("🔍 [平仓验证] 等待副交易所对冲平仓完成并验证仓位清空...")
            self.logger.info("=" * 60)

            # Step A: 等待对冲流程完成
            if self.hedge_in_progress:
                self.logger.info("⏳ 等待对冲平仓流程完成...")
                try:
                    await asyncio.wait_for(self.hedge_completed_event.wait(), timeout=35)
                    self.logger.info("✅ 对冲平仓流程已完成")
                except asyncio.TimeoutError:
                    self.logger.warning("⚠️ 等待对冲平仓流程超时 (35秒)")

            # Step B: 等待数据同步
            if self.config.wait_after_open > 0:
                self.logger.info(
                    f"⏳ 等待 {self.config.wait_after_open} 秒，确保数据同步..."
                )
                await asyncio.sleep(self.config.wait_after_open)

            # Step C: 多次重试验证仓位已清空
            max_retries = self.config.balance_check_retries
            retry_delay = 2
            positions_cleared = False

            for attempt in range(1, max_retries + 1):
                self.logger.info(
                    f"🔍 仓位清空检查 (尝试 {attempt}/{max_retries}), "
                    f"等待 {retry_delay}秒..."
                )
                await asyncio.sleep(retry_delay)

                primary_pos = await self.primary_client.get_signed_position()
                secondary_pos = await self.secondary_client.get_signed_position()

                if primary_pos is None or secondary_pos is None:
                    self.logger.warning(f"⚠️ 仓位查询失败 (尝试 {attempt}/{max_retries})")
                    continue

                self.logger.info(f"   主交易所仓位: {primary_pos}")
                self.logger.info(f"   副交易所仓位: {secondary_pos}")
                self.logger.info(f"   仓位总和: {primary_pos + secondary_pos}")

                # 检查是否已清空（容忍度 0.001）
                if (abs(primary_pos) <= Decimal('0.001') and
                    abs(secondary_pos) <= Decimal('0.001')):
                    positions_cleared = True
                    self.logger.info(f"✅ 仓位已清空 (尝试 {attempt}/{max_retries})")
                    break
                else:
                    self.logger.warning(f"⚠️ 尝试 {attempt}/{max_retries}: 仓位未清空")

            if not positions_cleared:
                self.logger.warning("⚠️ 平仓后仍有残余仓位")
                self.logger.warning(f"   主交易所: {primary_pos}")
                self.logger.warning(f"   副交易所: {secondary_pos}")
                self.logger.warning("⚠️ 将由 Step 3 清理残余仓位")
                return True  # 不返回 False，让 Step 3 清理残余仓位

            self.logger.info("=" * 60)
            self.logger.info("✅ 平仓完成并验证通过")
            self.logger.info(f"   主交易所仓位: {primary_pos}")
            self.logger.info(f"   副交易所仓位: {secondary_pos}")
            self.logger.info("=" * 60)
            return True

        except Exception as e:
            self.logger.error(f"平仓异常: {e}", exc_info=True)
            return False

    async def _wait_for_fill_with_repricing(self, side: str, target_quantity: Decimal) -> bool:
        """
        等待订单完全成交，支持重新定价

        逻辑:
        1. 下 post-only 订单到最优价格
        2. 每 5 秒检查价格是否仍是最优
        3. 如果不是最优，取消订单并重挂
        4. WebSocket 会自动触发对冲
        5. 直到完全成交（没有总超时限制）

        参数:
        - side: 订单方向 (buy/sell)
        - target_quantity: 目标成交量

        返回: True=成功, False=失败
        """
        price_check_interval = 5  # 每 5 秒检查价格

        while True:
            # 检查是否已完全成交
            if self.primary_filled_qty >= target_quantity:
                self.logger.info(f"订单已完全成交: {self.primary_filled_qty}/{target_quantity}")
                return True

            # 如果没有活跃订单，下新订单
            if not self.current_primary_order_id:
                try:
                    # 获取最优价格
                    best_bid, best_ask = await self.primary_client.fetch_bbo_prices(self.primary_contract_id)
                    if side == 'buy':
                        order_price = best_ask - self.primary_client.config.tick_size
                    else:
                        order_price = best_bid + self.primary_client.config.tick_size

                    # 计算剩余需要下单的数量
                    remaining_qty = target_quantity - self.primary_filled_qty

                    self.logger.info(f"下单: {side} {remaining_qty} @ {order_price}")

                    # 下 post-only 订单
                    result = await self.primary_client.place_post_only_order(
                        contract_id=self.primary_contract_id,
                        quantity=remaining_qty,
                        price=order_price,
                        side=side
                    )

                    self.current_primary_order_id = result.order_id
                    self.current_order_price = order_price  # 保存订单价格用于计算交易量
                    self.logger.info(f"订单已下: ID {result.order_id}, 价格 {order_price}")

                except Exception as e:
                    self.logger.error(f"下单失败: {e}")
                    await asyncio.sleep(1)  # 等待后重试
                    continue

            # 等待一段时间后检查价格
            await asyncio.sleep(price_check_interval)

            # 检查当前订单价格是否仍是最优
            if self.current_primary_order_id:
                try:
                    # 获取当前最优价格
                    best_bid, best_ask = await self.primary_client.fetch_bbo_prices(self.primary_contract_id)
                    if side == 'buy':
                        optimal_price = best_ask - self.primary_client.config.tick_size
                    else:
                        optimal_price = best_bid + self.primary_client.config.tick_size

                    # 获取当前订单信息
                    order_info = await self.primary_client.get_order_info(order_id=self.current_primary_order_id)

                    if order_info and order_info.status in ['OPEN', 'PENDING']:
                        current_price = order_info.price

                        # 如果价格不是最优，取消重挂
                        if abs(current_price - optimal_price) > self.primary_client.config.tick_size / 10:
                            self.logger.info(f"价格不再最优 (当前: {current_price}, 最优: {optimal_price})，取消重挂")

                            # ✅ 确保旧订单成功取消后再重挂
                            cancel_success = await self._cancel_order_with_verification(
                                self.current_primary_order_id
                            )

                            if cancel_success:
                                self.logger.info(f"✅ 订单 {self.current_primary_order_id} 已成功取消")
                                self.current_primary_order_id = None
                                # 循环会自动重新下单
                            else:
                                self.logger.warning(f"⚠️ 订单 {self.current_primary_order_id} 取消失败或未完全取消，等待下次检查")
                                # 保留 current_primary_order_id，下次循环继续尝试取消
                        else:
                            self.logger.debug(f"价格仍是最优: {current_price}")
                    else:
                        # 订单已不存在或已成交
                        # ✅ 在清空 order_id 之前，检查是否已完全成交
                        # 防止在订单成交时重复下单
                        if self.primary_filled_qty >= target_quantity:
                            self.logger.info(f"订单已成交，准备退出循环")
                            self.current_primary_order_id = None
                            continue  # 回到循环开始，触发 line 1150 的检查并退出
                        else:
                            self.logger.info(f"订单已结束但未完全成交 ({self.primary_filled_qty}/{target_quantity})，清空order_id准备重新下单")
                            self.current_primary_order_id = None

                except Exception as e:
                    self.logger.error(f"检查价格失败: {e}")

    async def _wait_for_fill(self, target_quantity: Decimal):
        """
        等待订单完全成交（平仓使用的简化版本）

        参数:
        - target_quantity: 目标成交量

        超时处理:
        - 超时后取消未成交订单
        """
        timeout = self.config.order_timeout
        check_interval = 1
        elapsed = 0

        while elapsed < timeout:
            if self.primary_filled_qty >= target_quantity:
                self.logger.info(f"订单已完全成交: {self.primary_filled_qty}")
                return

            await asyncio.sleep(check_interval)
            elapsed += check_interval

            if elapsed % 5 == 0:
                self.logger.debug(f"等待成交: {self.primary_filled_qty}/{target_quantity} ({elapsed}s)")

        # 超时处理
        self.logger.warning(f"订单超时 ({timeout}s)，当前成交: {self.primary_filled_qty}/{target_quantity}")

        if self.current_primary_order_id:
            try:
                await self.primary_client.cancel_order(self.current_primary_order_id)
                self.logger.info(f"已取消超时订单: {self.current_primary_order_id}")
            except Exception as e:
                self.logger.error(f"取消订单失败: {e}")

    async def _cancel_order_with_verification(self, order_id: str, timeout: float = 5.0) -> bool:
        """
        取消订单并通过 WebSocket 事件验证取消成功（事件驱动方式）

        优化策略:
            1. 调用 cancel_order API
            2. 等待 WebSocket 推送取消确认事件（带超时）
            3. 超时后进行一次轮询验证作为 fallback
            4. 比轮询方式更快、更可靠

        参数:
            order_id: 要取消的订单 ID
            timeout: 等待 WebSocket 确认的超时时间（秒），默认 5.0

        返回:
            True: 订单已成功取消（通过 WebSocket 或轮询确认）
            False: 取消失败，订单仍处于活跃状态

        设计优势:
            - WebSocket 推送确认延迟低（通常 < 100ms）
            - 避免多次轮询的延迟和 API 调用开销
            - 仍保留轮询 fallback 确保可靠性
        """
        try:
            # 清除之前的事件状态
            self.primary_order_canceled_event.clear()

            self.logger.info(f"尝试取消订单 {order_id}")

            # 步骤1: 调用取消 API
            await self.primary_client.cancel_order(order_id)

            # 步骤2: 等待 WebSocket 确认取消（带超时）
            try:
                await asyncio.wait_for(
                    self.primary_order_canceled_event.wait(),
                    timeout=timeout
                )
                self.logger.info(f"✅ 订单 {order_id} 已通过 WebSocket 确认取消")
                return True

            except asyncio.TimeoutError:
                self.logger.warning(f"⚠️ 等待订单 {order_id} WebSocket 取消确认超时 ({timeout}s)")

                # 步骤3: 超时后进行一次轮询验证（fallback）
                self.logger.info(f"执行轮询验证作为 fallback...")
                order_info = await self.primary_client.get_order_info(order_id)

                if order_info is None:
                    self.logger.info(f"订单 {order_id} 已不存在，视为取消成功")
                    return True

                if order_info.status in ['CANCELLED', 'CANCELED', 'REJECTED']:
                    self.logger.info(f"订单 {order_id} 已取消，状态: {order_info.status}（轮询确认）")
                    return True

                if order_info.status == 'FILLED':
                    self.logger.info(f"订单 {order_id} 在取消前已成交（轮询确认）")
                    return True

                if order_info.status in ['OPEN', 'PENDING']:
                    self.logger.error(f"❌ 订单 {order_id} 取消失败，状态仍为 {order_info.status}")
                    return False

                # 未知状态
                self.logger.warning(f"⚠️ 订单 {order_id} 状态未知: {order_info.status}")
                return False

        except Exception as e:
            self.logger.error(f"取消订单 {order_id} 时发生错误: {e}")
            return False

    async def _cleanup_residual_positions(self):
        """
        清理两个交易所的残余仓位

        策略:
        1. 检查两个交易所的实际仓位
        2. 如果存在残余仓位，使用市价单平仓
        3. 清理时禁用对冲机制，避免产生新的不平衡
        """
        try:
            self.logger.info("检查残余仓位...")

            # 临时禁用对冲机制
            self.disable_hedging = True
            self.logger.info("已禁用对冲机制（清理残余仓位）")

            # 检查主交易所（需要带符号才能判断平仓方向）
            self.logger.info("查询主交易所仓位...")
            primary_pos = await self.primary_client.get_signed_position()

            if primary_pos is None:
                self.logger.error("❌ 主交易所仓位查询失败（API 调用失败），无法确认残余仓位状态")
            else:
                self.logger.info(f"主交易所仓位: {primary_pos}")
                if abs(primary_pos) > Decimal('0.001'):
                    self.logger.warning(f"检测到主交易所残余仓位: {primary_pos}")
                    await self._close_residual_position(self.primary_client, primary_pos, "主交易所")
                else:
                    self.logger.info("主交易所无残余仓位")

            # 检查副交易所（需要带符号才能判断平仓方向）
            self.logger.info("查询副交易所仓位...")
            secondary_pos = await self.secondary_client.get_signed_position()

            if secondary_pos is None:
                self.logger.error("❌ 副交易所仓位查询失败（API 调用失败），无法确认残余仓位状态")
            else:
                self.logger.info(f"副交易所仓位: {secondary_pos}")
                if abs(secondary_pos) > Decimal('0.001'):
                    self.logger.warning(f"检测到副交易所残余仓位: {secondary_pos}")
                    await self._close_residual_position(self.secondary_client, secondary_pos, "副交易所")
                else:
                    self.logger.info("副交易所无残余仓位")

            # 只有两个交易所查询都成功才显示"完成"
            if primary_pos is not None and secondary_pos is not None:
                self.logger.info("✅ 残余仓位清理检查完成")
            else:
                self.logger.warning("⚠️ 残余仓位清理检查未完全成功（有 API 调用失败）")

        except Exception as e:
            self.logger.error(f"清理残余仓位失败: {e}", exc_info=True)

        finally:
            # 恢复对冲机制
            self.disable_hedging = False
            self.logger.info("已恢复对冲机制")

    async def _close_residual_position(self, client: BaseExchangeClient, position: Decimal, exchange_name: str):
        """
        平掉单个交易所的残余仓位

        策略:
        1. 下市价单平仓
        2. 等待订单成交确认
        3. 验证仓位是否已清空
        """
        try:
            side = 'sell' if position > 0 else 'buy'
            quantity = abs(position)

            self.logger.info(f"平仓 {exchange_name} 残余仓位: {side} {quantity}")

            # 根据客户端选择正确的 contract_id
            contract_id = self.primary_contract_id if client == self.primary_client else self.secondary_contract_id

            result = await client.place_open_order(
                contract_id=contract_id,
                quantity=quantity,
                direction=side
            )

            if not result.success:
                self.logger.error(f"{exchange_name} 平仓失败: {result.message}")
                return

            self.logger.info(f"{exchange_name} 平仓订单已提交: {result.order_id}")

            # 等待订单成交（最多等待 10 秒）
            max_wait = 10
            check_interval = 1
            elapsed = 0

            while elapsed < max_wait:
                await asyncio.sleep(check_interval)
                elapsed += check_interval

                # 检查仓位是否已清空
                current_pos = await client.get_account_positions()
                if abs(current_pos) < Decimal('0.001'):
                    self.logger.info(f"{exchange_name} 残余仓位已清空 (耗时 {elapsed}s)")
                    return

                self.logger.debug(f"{exchange_name} 仓位: {current_pos} (等待 {elapsed}s)")

            # 超时警告
            final_pos = await client.get_account_positions()
            if abs(final_pos) > Decimal('0.001'):
                self.logger.warning(f"{exchange_name} 平仓超时，当前仓位: {final_pos}")
                await self._send_notification(f"⚠️ {exchange_name} 平仓超时\n当前仓位: {final_pos}")

        except Exception as e:
            self.logger.error(f"{exchange_name} 平仓异常: {e}", exc_info=True)

    async def _final_position_check(self):
        """
        最终仓位检查

        确保所有仓位已清空
        """
        try:
            self.logger.info("执行最终仓位检查...")

            primary_pos = await self.primary_client.get_account_positions()
            secondary_pos = await self.secondary_client.get_account_positions()

            # 检查是否有 API 调用失败
            if primary_pos is None or secondary_pos is None:
                msg = "📋 最终仓位检查\n"
                if primary_pos is None:
                    msg += "❌ 主交易所：查询失败（API 错误）\n"
                else:
                    msg += f"主交易所：{primary_pos}\n"

                if secondary_pos is None:
                    msg += "❌ 副交易所：查询失败（API 错误）"
                else:
                    msg += f"副交易所：{secondary_pos}"

                msg += "\n⚠️ 无法确认最终仓位状态，请手动检查"
                self.logger.error("最终仓位检查失败：API 调用失败")
                await self._send_notification(msg)
                return

            # API 调用成功，检查仓位
            msg = f"📋 最终仓位\n主: {primary_pos}\n副: {secondary_pos}"

            if abs(primary_pos) > Decimal('0.001') or abs(secondary_pos) > Decimal('0.001'):
                msg += "\n⚠️ 存在未平仓位，请手动处理"
                self.logger.warning("存在未平仓位")
            else:
                msg += "\n✅ 仓位已清空"
                self.logger.info("仓位检查通过")

            await self._send_notification(msg)

        except Exception as e:
            self.logger.error(f"最终仓位检查失败: {e}", exc_info=True)
            await self._send_notification(f"❌ 最终仓位检查异常\n{str(e)}\n请手动检查仓位")

    async def _place_aggressive_order(self, exchange_client, contract_id: str, quantity: Decimal, side: str, price_offset: Decimal = Decimal('0.001')) -> 'OrderResult':
        """
        下市价单或快速成交的限价单（非 post-only）

        参数:
        - exchange_client: 交易所客户端
        - contract_id: 合约 ID
        - quantity: 数量
        - side: 方向 (buy/sell)
        - price_offset: 价格偏移（默认 0.001 = 0.1%，确保快速成交）

        逻辑:
        - 调用交易所的 place_market_order 接口
        - 该接口会使用带价格偏移的非 post-only 限价单

        返回: OrderResult
        """
        try:
            from exchanges.base import OrderResult

            self.logger.info(f"对冲订单: {side} {quantity} on {self.config.secondary_exchange} (IOC 市价单)")

            # 使用 IOC 市价单接口 - 确保立即成交
            result = await exchange_client.place_market_order(
                contract_id=contract_id,
                quantity=quantity,
                side=side,
                price_offset=price_offset  # Extended 会忽略此参数，使用 IOC
            )

            return result

        except Exception as e:
            self.logger.error(f"下快速成交订单失败: {e}", exc_info=True)
            from exchanges.base import OrderResult
            return OrderResult(success=False, error_message=str(e))

    async def _wait_for_hedge_fill_with_info(self, order_id: str, expected_qty: Decimal, timeout: int = 30) -> dict:
        """
        等待对冲订单成交并返回详细信息

        参数:
        - order_id: 订单 ID
        - expected_qty: 预期成交数量
        - timeout: 超时时间（秒）

        返回: {'success': bool, 'filled_qty': Decimal}
            - success: True=完全成交, False=未成交或部分成交
            - filled_qty: 实际成交数量
        """
        start_time = asyncio.get_event_loop().time()
        check_interval = 2  # 每2秒检查一次

        self.logger.info(f"等待对冲订单成交: {order_id}, 预期数量: {expected_qty}")

        while True:
            elapsed = asyncio.get_event_loop().time() - start_time

            if elapsed > timeout:
                self.logger.warning(f"对冲订单等待超时 ({timeout}秒): {order_id}")
                return {'success': False, 'filled_qty': Decimal('0')}

            # 通过 API 主动查询订单状态
            try:
                order_info = await self.secondary_client.get_order_info(order_id)

                if order_info is None:
                    # 订单不存在 - 可能是被立即拒绝或已经被交易所清理
                    self.logger.warning(f"对冲订单不存在或已被清理: {order_id}，将触发重试")
                    return {'success': False, 'filled_qty': Decimal('0')}

                if order_info.status == 'FILLED':
                    # 完全成交
                    filled_qty = order_info.filled_size
                    self.logger.info(f"对冲订单已完全成交 (API 确认): {order_id}, 成交: {filled_qty}")
                    return {'success': True, 'filled_qty': filled_qty}
                elif order_info.status in ['CANCELLED', 'REJECTED', 'CANCELED']:
                    # IOC 订单被拒绝或取消
                    filled_qty = order_info.filled_size
                    if filled_qty > Decimal('0'):
                        # 部分成交
                        self.logger.warning(
                            f"对冲订单部分成交后被取消: {order_info.status}, "
                            f"成交: {filled_qty}/{expected_qty}"
                        )
                        return {'success': True, 'filled_qty': filled_qty}
                    else:
                        # 完全未成交
                        self.logger.warning(f"对冲订单被拒绝/取消: {order_info.status}, 未成交")
                        return {'success': False, 'filled_qty': Decimal('0')}
                else:
                    self.logger.debug(f"对冲订单状态: {order_info.status}, 成交: {order_info.filled_size}/{expected_qty}")
            except Exception as e:
                # 查询失败可能是订单不存在
                error_msg = str(e).lower()
                if 'not found' in error_msg or 'does not exist' in error_msg or '不存在' in error_msg:
                    self.logger.warning(f"对冲订单不存在: {order_id}，错误: {e}，将触发重试")
                    return {'success': False, 'filled_qty': Decimal('0')}
                else:
                    self.logger.debug(f"查询对冲订单状态失败: {e}")

            await asyncio.sleep(check_interval)

    async def _check_position_balance(self) -> bool:
        """
        检查仓位平衡

        逻辑:
        1. 获取两个交易所的实际仓位（带符号）
        2. 检查是否平衡（绝对值相等，方向相反）
        3. 如果不平衡，重试多次
        4. 打印详细信息

        返回: True=平衡, False=不平衡
        """
        for attempt in range(1, self.config.balance_check_retries + 1):
            try:
                # 获取带符号的仓位（对冲模式需要区分多空）
                primary_pos = await self.primary_client.get_signed_position()
                secondary_pos = await self.secondary_client.get_signed_position()

                # 检查 API 调用是否成功
                if primary_pos is None or secondary_pos is None:
                    self.logger.warning(f"仓位查询失败 (尝试 {attempt}/{self.config.balance_check_retries})")
                    if attempt < self.config.balance_check_retries:
                        await asyncio.sleep(2)
                        continue
                    return False

                # 计算仓位总和（对冲应该为 0）
                total_position = primary_pos + secondary_pos
                position_threshold = Decimal('0.001')

                self.logger.info(f"📊 仓位检查 (尝试 {attempt}/{self.config.balance_check_retries}):")
                self.logger.info(f"  主交易所 ({self.config.primary_exchange}): {primary_pos}")
                self.logger.info(f"  副交易所 ({self.config.secondary_exchange}): {secondary_pos}")
                self.logger.info(f"  净仓位 (应为0): {total_position}")

                # 检查是否平衡
                if abs(total_position) <= position_threshold:
                    self.logger.info(f"✅ 仓位平衡，净仓位 {total_position}")
                    return True
                else:
                    self.logger.warning(f"⚠️ 仓位不平衡，净仓位 {total_position}（阈值: {position_threshold}）")
                    if attempt < self.config.balance_check_retries:
                        self.logger.info(f"等待 2 秒后重试...")
                        await asyncio.sleep(2)
                        continue

            except Exception as e:
                self.logger.error(f"仓位检查异常 (尝试 {attempt}/{self.config.balance_check_retries}): {e}")
                if attempt < self.config.balance_check_retries:
                    await asyncio.sleep(2)
                    continue

        # 所有尝试都失败
        self.logger.error(f"❌ 仓位平衡检查失败，已重试 {self.config.balance_check_retries} 次")
        return False

    async def _verify_and_reset_state(self) -> bool:
        """
        验证并重置状态

        检查项:
        1. 两个交易所的净仓位必须在合理范围内
        2. 单个交易所残余仓位不能超过订单量的一定比例
        3. 追踪变量应该已经清空

        返回: True=状态正常可以继续, False=存在异常需要中断
        """
        # 检查主交易所仓位（带符号）
        try:
            primary_pos = await self.primary_client.get_signed_position()
            if primary_pos is None:
                self.logger.error(f"❌ 主交易所仓位查询失败（API 调用失败，返回 None）")
                return False
            self.logger.info(f"主交易所当前仓位: {primary_pos:+.4f}")
        except Exception as e:
            self.logger.error(f"❌ 主交易所仓位查询异常: {e}", exc_info=True)
            return False

        # 检查副交易所仓位（带符号）
        try:
            secondary_pos = await self.secondary_client.get_signed_position()
            if secondary_pos is None:
                self.logger.error(f"❌ 副交易所仓位查询失败（API 调用失败，返回 None）")
                return False
            self.logger.info(f"副交易所当前仓位: {secondary_pos:+.4f}")
        except Exception as e:
            self.logger.error(f"❌ 副交易所仓位查询异常: {e}", exc_info=True)
            return False

        # 计算净仓位（带符号相加）
        net_position = primary_pos + secondary_pos
        self.logger.info(f"净仓位: {net_position:+.4f}")

        # 净仓位不应超过订单量的 2 倍（说明有严重的对冲失败）
        max_acceptable_net = self.config.quantity * 2
        if abs(net_position) > max_acceptable_net:
            self.logger.error(
                f"❌ 净仓位过大: {net_position} (阈值: {max_acceptable_net})"
            )
            self.logger.error(
                f"   主交易所: {primary_pos}, 副交易所: {secondary_pos}"
            )
            self.logger.error("   可能存在严重的对冲失败，需要人工介入")
            return False

        # 检查单个交易所残余仓位是否在合理范围
        # 允许最多 20% 的订单量作为残余（比 0.001 宽松很多）
        max_single_residual = self.config.quantity * Decimal('0.2')

        if abs(primary_pos) > max_single_residual:
            self.logger.warning(
                f"⚠️ 主交易所残余仓位较大: {primary_pos} "
                f"(阈值: {max_single_residual})"
            )
            # 不直接返回 False，而是记录警告，继续检查净仓位

        if abs(secondary_pos) > max_single_residual:
            self.logger.warning(
                f"⚠️ 副交易所残余仓位较大: {secondary_pos} "
                f"(阈值: {max_single_residual})"
            )

        # 如果净仓位在合理范围内，即使有小额残余也允许继续
        # 这些残余会在下一轮的清理步骤中被处理
        net_position_threshold = self.config.quantity * Decimal('0.05')  # 5% 容忍度
        if abs(net_position) <= net_position_threshold:
            self.logger.info(
                f"✅ 净仓位在可接受范围: {net_position} "
                f"(≤ {net_position_threshold})"
            )
        else:
            # ❌ 净仓位超过阈值，必须中断，不能继续交易
            self.logger.error(
                f"❌ 净仓位超过阈值: {net_position} (阈值: {net_position_threshold})"
            )
            self.logger.error(
                f"   主交易所: {primary_pos}, 副交易所: {secondary_pos}"
            )
            self.logger.error("   存在未平掉的仓位，需要人工检查")
            return False

        # 状态检查通过，重置追踪变量
        self.primary_filled_qty = Decimal('0')
        self.secondary_filled_qty = Decimal('0')
        self.total_hedged_qty = Decimal('0')  # 也重置累计对冲量
        self.cumulative_filled_qty = Decimal('0')  # 也重置全局累计成交量
        self.order_processed_fills.clear()  # 清空订单成交去重字典

        self.logger.info("✅ 状态检查通过，已重置追踪变量")
        return True

    async def _handle_step_failure(self, step_name: str):
        """
        处理交易步骤失败

        策略:
        1. 记录并通知失败
        2. 尝试清理当前仓位
        3. 中断交易（调用方会 break 退出循环）
        """
        error_msg = f"❌ {step_name}失败"
        self.logger.error(error_msg)
        await self._send_notification(f"{error_msg}\n正在尝试清理残余仓位...")

        # 尝试清理
        await self._cleanup_residual_positions()

    async def _send_notification(self, message: str):
        """
        发送 Telegram 通知
        """
        if not self.config.enable_telegram:
            return

        if not self.telegram_token or not self.telegram_chat_id:
            return

        try:
            prefix = f"[{self.config.primary_exchange}↔{self.config.secondary_exchange}]"
            full_message = f"{prefix}\n{message}"

            with TelegramBot(self.telegram_token, self.telegram_chat_id) as tg_bot:
                tg_bot.send_text(full_message)
        except Exception as e:
            self.logger.error(f"Telegram 通知失败: {e}")

    async def cleanup(self):
        """清理资源并发送最终总结通知"""
        try:
            self.logger.info("清理资源...")

            # 发送最终总结通知
            try:
                # 查询最终仓位
                final_primary_pos = Decimal('0')
                final_secondary_pos = Decimal('0')

                if self.primary_client:
                    try:
                        pos = await self.primary_client.get_signed_position()
                        if pos is not None:
                            final_primary_pos = pos
                    except Exception as e:
                        self.logger.error(f"查询最终主交易所仓位失败: {e}")

                if self.secondary_client:
                    try:
                        pos = await self.secondary_client.get_signed_position()
                        if pos is not None:
                            final_secondary_pos = pos
                    except Exception as e:
                        self.logger.error(f"查询最终副交易所仓位失败: {e}")

                # 计算运行时间
                runtime = datetime.now() - self.start_time
                runtime_str = str(runtime).split('.')[0]  # 移除微秒

                # 计算完成百分比
                completion_pct = int((self.current_iteration / self.config.iterations * 100)) if self.config.iterations > 0 else 0

                # 更新 P&L（获取最终余额）
                await self._update_pnl()

                # 构建余额信息
                cleanup_balance_info = ""
                if self.pnl_tracker['current_balance_primary'] is not None:
                    cleanup_balance_info = f"\n\n余额:\n{self.config.primary_exchange}: ${self.pnl_tracker['current_balance_primary']:,.2f}"
                    if self.pnl_tracker['current_balance_secondary'] is not None:
                        cleanup_balance_info += f"\n{self.config.secondary_exchange}: ${self.pnl_tracker['current_balance_secondary']:,.2f}"
                    cleanup_balance_info += f"\n总盈亏: ${self.pnl_tracker['total_pnl']:+,.2f}"

                # 构建交易量和损耗率信息
                cleanup_volume_info = ""
                total_volume = self.pnl_tracker['total_volume_usd']
                if total_volume > 0:
                    cleanup_volume_info += f"\n\n交易统计:\n交易量: ${total_volume:,.2f}"
                    # 计算每 10000u 的损耗
                    if total_volume >= Decimal('1'):  # 避免除以0
                        cost_per_10k = (abs(self.pnl_tracker['total_pnl']) / total_volume) * Decimal('10000')
                        cleanup_volume_info += f"\n损耗率: ${cost_per_10k:.2f}/10k USDT"

                # 发送最终通知
                final_msg = (
                    f"🏁 对冲交易结束\n\n"
                    f"交易对: {self.config.ticker}\n"
                    f"完成轮数: {self.current_iteration}/{self.config.iterations} ({completion_pct}%)\n"
                    f"运行时间: {runtime_str}\n\n"
                    f"最终仓位:\n"
                    f"{self.config.primary_exchange}: {final_primary_pos}\n"
                    f"{self.config.secondary_exchange}: {final_secondary_pos}\n"
                    f"净仓位: {final_primary_pos + final_secondary_pos}{cleanup_balance_info}{cleanup_volume_info}"
                )

                await self._send_notification(final_msg)
                self.logger.info("📱 已发送最终总结通知")

            except Exception as e:
                self.logger.error(f"发送最终通知失败: {e}")

            # 更新最终状态文件
            await self.update_status()

            # 断开连接
            if self.primary_client:
                await self.primary_client.disconnect()

            if self.secondary_client:
                await self.secondary_client.disconnect()

            # 给异步清理任务一些时间完成，避免 "Event loop is closed" 错误
            await asyncio.sleep(0.5)

            self.logger.info("资源清理完成")

        except Exception as e:
            self.logger.error(f"清理资源失败: {e}", exc_info=True)

    async def stop(self):
        """
        停止交易策略
        """
        self.logger.info("停止交易策略...")
        self.should_stop = True
        await self._send_notification("⏹️ 收到停止信号")


# ========== 使用示例 ==========

async def example_usage():
    """
    使用示例
    """
    config = HedgeConfig(
        primary_exchange='backpack',
        secondary_exchange='paradex',
        ticker='SOL-PERP',
        quantity=Decimal('1.0'),
        iterations=5,
        order_timeout=60,
        enable_telegram=True
    )

    hedge = DualExchangeHedge(config)

    try:
        await hedge.run_strategy()
    except KeyboardInterrupt:
        await hedge.stop()
    finally:
        await hedge.cleanup()


if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 运行示例
    asyncio.run(example_usage())
