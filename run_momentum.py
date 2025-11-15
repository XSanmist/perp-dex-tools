"""
Run Momentum Trading Bot

This script provides a command-line interface for running the momentum trading bot.
It parses arguments and initializes the bot with the specified configuration.
"""

import asyncio
import argparse
from decimal import Decimal
import sys

from strategies import MomentumBot, MomentumConfig


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Run Momentum Trading Bot',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Market configuration
    parser.add_argument(
        '--exchange',
        type=str,
        required=True,
        choices=['extended', 'lighter', 'backpack', 'paradex', 'aster', 'edgex', 'grvt', 'apex'],
        help='Exchange to trade on'
    )
    parser.add_argument(
        '--ticker',
        type=str,
        required=True,
        help='Trading pair ticker (e.g., ETH, BTC)'
    )

    # Order sizing
    parser.add_argument(
        '--quantity',
        type=float,
        required=True,
        help='Order size per trade'
    )

    # Entry strategy
    parser.add_argument(
        '--direction',
        type=str,
        required=True,
        choices=['buy', 'sell'],
        help='Trading direction (buy for long, sell for short)'
    )
    parser.add_argument(
        '--tick-offset',
        type=int,
        required=True,
        help='Number of ticks to offset from best price (e.g., 1 means 1 tick away)'
    )

    # Exit strategy
    parser.add_argument(
        '--take-profit-pct',
        type=float,
        required=True,
        help='Take profit percentage (e.g., 0.02 for 0.02%%)'
    )

    # Trading controls
    parser.add_argument(
        '--max-positions',
        type=int,
        default=1,
        help='Maximum concurrent positions'
    )
    parser.add_argument(
        '--wait-time',
        type=int,
        default=5,
        help='Wait time between cycles in seconds'
    )

    return parser.parse_args()


async def main():
    """Main entry point for the momentum bot."""
    # Parse arguments
    args = parse_arguments()

    # Print configuration
    print("=" * 60)
    print("🚀 Momentum Trading Bot")
    print("=" * 60)
    print(f"Exchange:         {args.exchange}")
    print(f"Ticker:           {args.ticker}")
    print(f"Direction:        {args.direction.upper()}")
    print(f"Quantity:         {args.quantity}")
    print(f"Tick Offset:      {args.tick_offset} ticks")
    print(f"Take Profit:      {args.take_profit_pct}%")
    print(f"Max Positions:    {args.max_positions}")
    print(f"Wait Time:        {args.wait_time}s")
    print("=" * 60)
    print()

    # Create configuration
    config = MomentumConfig(
        ticker=args.ticker,
        exchange=args.exchange,
        quantity=Decimal(str(args.quantity)),
        direction=args.direction,
        tick_offset=args.tick_offset,
        take_profit_pct=Decimal(str(args.take_profit_pct)),
        max_positions=args.max_positions,
        wait_time=args.wait_time
    )

    # Create and run bot
    bot = MomentumBot(config)

    try:
        await bot.run()
    except KeyboardInterrupt:
        print("\n\n🛑 Keyboard interrupt received")
        await bot.graceful_shutdown("Keyboard interrupt")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        await bot.graceful_shutdown(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Exiting...")
        sys.exit(0)
