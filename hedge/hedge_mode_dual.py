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
        self.primary_filled_qty = Decimal('0')
        self.secondary_filled_qty = Decimal('0')
        self.last_primary_filled = Decimal('0')
        self.current_primary_order_id: Optional[str] = None

        # 控制标志
        self.is_running = False
        self.should_stop = False
        self.hedging_lock = asyncio.Lock()
        self.disable_hedging = False
        self.hedge_completed_event = asyncio.Event()
        self.hedge_in_progress = False

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
        }

        try:
            with open(status_file, 'w') as f:
                json.dump(status_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"更新状态文件失败: {e}")

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

        核心逻辑:
            1. 检测订单 ID 变化，识别新订单或同一订单
            2. 计算增量成交（仅对新增成交量进行对冲）
            3. 触发副交易所对冲

        增量成交计算:
            - 新订单: 重置 last_filled，全部成交量算增量
            - 同一订单: 增量 = 当前累计 - 上次累计
        """
        try:
            self.logger.debug(f"收到主交易所订单更新: {order_data}")

            if self.disable_hedging:
                self.logger.debug("对冲已禁用，跳过处理")
                return

            order_id = order_data.get('order_id')
            filled_qty = Decimal(str(order_data.get('filled_size', 0)))

            # 检测订单切换并计算增量成交
            if order_id != self.current_primary_order_id:
                self.logger.info(
                    f"🆕 检测到新订单: {order_id} "
                    f"(旧订单: {self.current_primary_order_id})"
                )
                self.current_primary_order_id = order_id
                self.last_primary_filled = Decimal('0')
                incremental_fill = filled_qty
            else:
                incremental_fill = filled_qty - self.last_primary_filled

            self.logger.info(
                f"订单 {order_id} 成交更新: 累计={filled_qty}, "
                f"增量={incremental_fill} (上次={self.last_primary_filled})"
            )

            if incremental_fill > Decimal('0'):
                self.logger.info(f"主交易所新增成交: {incremental_fill}")
                self.last_primary_filled = filled_qty
                self.primary_filled_qty = filled_qty

                self.logger.info(f"触发对冲，方向: {order_data.get('side')}")
                await self._execute_hedge(incremental_fill, order_data.get('side'))
                self.logger.info("对冲执行完毕")

                await self.update_status()
            else:
                self.logger.debug(f"无新增成交: {incremental_fill}")

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

        重试机制:
            - 30 秒内持续重试
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

                # 30 秒内持续重试
                timeout = 30
                start_time = asyncio.get_event_loop().time()
                attempt = 0
                success = False

                while True:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed > timeout:
                        error_msg = (
                            f"⚠️ 对冲失败 (30秒超时)\n数量: {quantity}\n"
                            f"方向: {hedge_side}\n总尝试次数: {attempt}"
                        )
                        self.logger.error(error_msg)
                        await self._send_notification(error_msg)
                        break

                    attempt += 1
                    remaining_time = timeout - elapsed
                    self.logger.info(
                        f"对冲尝试 #{attempt} (剩余时间: {remaining_time:.1f}秒)"
                    )

                    try:
                        result = await self._place_aggressive_order(
                            exchange_client=self.secondary_client,
                            contract_id=self.secondary_contract_id,
                            quantity=quantity,
                            side=hedge_side
                        )

                        if result.success:
                            self.logger.info(f"对冲订单已下单: ID {result.order_id}")

                            filled = await self._wait_for_hedge_fill(
                                result.order_id, quantity
                            )

                            if filled:
                                self.logger.info(
                                    f"✅ 对冲完全成交: {quantity} (尝试 {attempt} 次)"
                                )
                                success = True
                                break
                            else:
                                self.logger.warning(f"对冲未完全成交 (尝试 #{attempt})")
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

            # 计算状态同步间隔（每 5% 同步一次）
            total_iterations = self.config.iterations
            sync_interval = max(1, int(total_iterations * 0.05))  # 至少为 1
            last_sync_iteration = 0

            for iteration in range(1, self.config.iterations + 1):
                if self.should_stop:
                    stop_reason = "用户中断"
                    self.logger.info("收到停止信号，中断交易")
                    break

                self.current_iteration = iteration  # 更新当前轮数

                # 只在达到同步间隔或最后一轮时同步状态
                should_sync = (
                    iteration == 1 or  # 第一轮
                    iteration == total_iterations or  # 最后一轮
                    (iteration - last_sync_iteration) >= sync_interval  # 达到同步间隔
                )

                if should_sync:
                    await self.update_status()
                    last_sync_iteration = iteration

                self.logger.info(f"===== 开始第 {iteration}/{self.config.iterations} 轮交易 =====")

                # 只在 5% 里程碑发送通知
                if should_sync:
                    progress_pct = int((iteration / total_iterations) * 100)
                    await self._send_notification(f"📊 第 {iteration}/{self.config.iterations} 轮 ({progress_pct}%)")

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

                # 在同步点发送完成通知
                if should_sync and iteration < total_iterations:
                    progress_pct = int((iteration / total_iterations) * 100)
                    await self._send_notification(f"✅ 已完成 {iteration}/{total_iterations} 轮 ({progress_pct}%)")

            # 最终状态检查
            await self._final_position_check()

            # 发送最终完成通知
            self.logger.info(f"最终统计: 完成轮数={completed_iterations}, 总轮数={self.config.iterations}, 停止原因={stop_reason}")

            if completed_iterations == self.config.iterations:
                # 全部完成
                final_msg = (
                    f"✅ 对冲交易完成\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"完成轮数: {completed_iterations}/{self.config.iterations}\n"
                    f"状态: 全部完成"
                )
            else:
                # 提前中断
                final_msg = (
                    f"⚠️ 对冲交易中断\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"完成轮数: {completed_iterations}/{self.config.iterations}\n"
                    f"完成度: {int(completed_iterations/self.config.iterations*100) if self.config.iterations > 0 else 0}%\n"
                    f"中断原因: {stop_reason}"
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

            # 重置追踪变量
            self.primary_filled_qty = Decimal('0')
            self.secondary_filled_qty = Decimal('0')
            self.last_primary_filled = Decimal('0')
            self.current_primary_order_id = None

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

            # 重置追踪变量
            self.primary_filled_qty = Decimal('0')
            self.secondary_filled_qty = Decimal('0')
            self.last_primary_filled = Decimal('0')
            self.current_primary_order_id = None

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
                            await self.primary_client.cancel_order(self.current_primary_order_id)
                            self.current_primary_order_id = None
                            # 循环会自动重新下单
                        else:
                            self.logger.debug(f"价格仍是最优: {current_price}")
                    else:
                        # 订单已不存在或已成交
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

    async def _wait_for_hedge_fill(self, order_id: str, expected_qty: Decimal, timeout: int = 30) -> bool:
        """
        等待对冲订单完全成交

        参数:
        - order_id: 订单 ID
        - expected_qty: 预期成交数量
        - timeout: 超时时间（秒）

        返回: True=完全成交, False=未成交或部分成交
        """
        start_time = asyncio.get_event_loop().time()
        check_interval = 2  # 每2秒检查一次

        self.logger.info(f"等待对冲订单成交: {order_id}, 预期数量: {expected_qty}")

        while True:
            elapsed = asyncio.get_event_loop().time() - start_time

            if elapsed > timeout:
                self.logger.warning(f"对冲订单等待超时 ({timeout}秒): {order_id}")
                return False

            # 检查副交易所成交量（通过回调更新的 self.secondary_filled_qty）
            if self.secondary_filled_qty >= expected_qty:
                self.logger.info(f"对冲订单已完全成交: {self.secondary_filled_qty}/{expected_qty}")
                return True

            # 或者通过 API 主动查询订单状态
            try:
                order_info = await self.secondary_client.get_order_info(order_id)

                if order_info is None:
                    # 订单不存在 - 可能是被立即拒绝或已经被交易所清理
                    self.logger.warning(f"对冲订单不存在或已被清理: {order_id}，将触发重试")
                    return False

                if order_info.status == 'FILLED':
                    self.logger.info(f"对冲订单已完全成交 (API 确认): {order_id}")
                    return True
                elif order_info.status in ['CANCELLED', 'REJECTED', 'CANCELED']:
                    # IOC 订单被拒绝或取消（未成交部分），返回失败以触发重试
                    self.logger.warning(f"对冲订单被拒绝/取消: {order_info.status}, 成交: {order_info.filled_size}/{expected_qty}")
                    return False
                else:
                    self.logger.debug(f"对冲订单状态: {order_info.status}, 成交: {order_info.filled_size}/{expected_qty}")
            except Exception as e:
                # 查询失败可能是订单不存在
                error_msg = str(e).lower()
                if 'not found' in error_msg or 'does not exist' in error_msg or '不存在' in error_msg:
                    self.logger.warning(f"对冲订单不存在: {order_id}，错误: {e}，将触发重试")
                    return False
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
        1. 两个交易所的实际仓位必须接近 0
        2. 追踪变量应该已经清空

        返回: True=状态正常可以继续, False=存在异常需要中断
        """
        # 检查主交易所仓位
        try:
            primary_pos = await self.primary_client.get_account_positions()
            if primary_pos is None:
                self.logger.error(f"❌ 主交易所仓位查询失败（API 调用失败，返回 None）")
                return False
            self.logger.info(f"主交易所当前仓位: {primary_pos}")
        except Exception as e:
            self.logger.error(f"❌ 主交易所仓位查询异常: {e}", exc_info=True)
            return False

        # 检查副交易所仓位
        try:
            secondary_pos = await self.secondary_client.get_account_positions()
            if secondary_pos is None:
                self.logger.error(f"❌ 副交易所仓位查询失败（API 调用失败，返回 None）")
                return False
            self.logger.info(f"副交易所当前仓位: {secondary_pos}")
        except Exception as e:
            self.logger.error(f"❌ 副交易所仓位查询异常: {e}", exc_info=True)
            return False

        position_threshold = Decimal('0.001')  # 允许的最小残余仓位

        # 如果存在未平仓位，说明上一轮交易有问题
        if abs(primary_pos) > position_threshold:
            self.logger.error(f"❌ 主交易所存在未平仓位: {primary_pos}")
            return False

        if abs(secondary_pos) > position_threshold:
            self.logger.error(f"❌ 副交易所存在未平仓位: {secondary_pos}")
            return False

        # 状态检查通过，重置追踪变量
        self.primary_filled_qty = Decimal('0')
        self.secondary_filled_qty = Decimal('0')
        self.last_primary_filled = Decimal('0')
        self.current_primary_order_id = None

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

                # 发送最终通知
                final_msg = (
                    f"🏁 对冲交易结束\n\n"
                    f"交易对: {self.config.ticker}\n"
                    f"完成轮数: {self.current_iteration}/{self.config.iterations} ({completion_pct}%)\n"
                    f"运行时间: {runtime_str}\n\n"
                    f"最终仓位:\n"
                    f"{self.config.primary_exchange}: {final_primary_pos}\n"
                    f"{self.config.secondary_exchange}: {final_secondary_pos}\n"
                    f"净仓位: {final_primary_pos + final_secondary_pos}"
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
