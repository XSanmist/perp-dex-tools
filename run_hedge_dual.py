#!/usr/bin/env python3
"""
命令行入口脚本：双交易所对冲交易

用法:
    python run_hedge_dual.py --primary backpack --secondary paradex --ticker SOL-PERP --size 0.1 --iter 5
"""

import argparse
import asyncio
import logging
from decimal import Decimal

from hedge.hedge_mode_dual import DualExchangeHedge, HedgeConfig


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Dual Exchange Hedge Trading Bot')

    # 交易所配置
    parser.add_argument('--primary', '--primary-exchange', required=True,
                        help='Primary exchange (post-only orders)')
    parser.add_argument('--secondary', '--secondary-exchange', required=True,
                        help='Secondary exchange (hedge orders)')

    # 交易参数
    parser.add_argument('--ticker', required=True,
                        help='Trading pair (e.g., SOL-PERP, ETH-PERP)')
    parser.add_argument('--size', type=float, required=True,
                        help='Order size per iteration')
    parser.add_argument('--iter', '--iterations', type=int, default=1,
                        help='Number of iterations (default: 1)')

    # 超时配置
    parser.add_argument('--order-timeout', type=int, default=30,
                        help='Order timeout in seconds (default: 30)')
    parser.add_argument('--position-check-interval', type=int, default=5,
                        help='Position check interval in seconds (default: 5)')
    parser.add_argument('--wait-after-open', type=int, default=10,
                        help='Wait time after opening position in seconds (default: 10)')
    parser.add_argument('--balance-check-retries', type=int, default=3,
                        help='Number of retries for balance check (default: 3)')

    # 通知配置
    parser.add_argument('--no-telegram', action='store_true',
                        help='Disable Telegram notifications')

    # 任务追踪
    parser.add_argument('--task-id', type=str, default='',
                        help='Task ID for status tracking')

    return parser.parse_args()


async def main():
    """主函数"""
    args = parse_args()

    # 配置日志时区为 UTC+8
    from datetime import datetime, timezone, timedelta

    class BeijingFormatter(logging.Formatter):
        """北京时间（UTC+8）格式化器"""
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=timezone(timedelta(hours=8)))
            if datefmt:
                return dt.strftime(datefmt)
            return dt.strftime('%Y-%m-%d %H:%M:%S')

    # 配置日志
    formatter = BeijingFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 配置处理器
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(f'logs/hedge_dual_{args.primary}_{args.secondary}_{args.ticker}.log')
    file_handler.setFormatter(formatter)

    # 配置根日志记录器
    logging.basicConfig(
        level=logging.INFO,
        handlers=[stream_handler, file_handler]
    )

    # 创建配置
    config = HedgeConfig(
        primary_exchange=args.primary,
        secondary_exchange=args.secondary,
        ticker=args.ticker,
        quantity=Decimal(str(args.size)),
        iterations=args.iter,
        order_timeout=args.order_timeout,
        position_check_interval=args.position_check_interval,
        wait_after_open=args.wait_after_open,
        balance_check_retries=args.balance_check_retries,
        enable_telegram=not args.no_telegram,
        task_id=args.task_id
    )

    # 创建对冲实例
    hedge = DualExchangeHedge(config)

    try:
        # 运行策略
        await hedge.run_strategy()
    except KeyboardInterrupt:
        logging.info("收到中断信号，停止交易...")
        await hedge.stop()
    except Exception as e:
        logging.error(f"交易异常: {e}", exc_info=True)
    finally:
        await hedge.cleanup()


if __name__ == '__main__':
    asyncio.run(main())
