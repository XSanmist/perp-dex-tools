"""
Trading Strategies Module

This module contains various trading bot strategies for automated trading.

Available Strategies:
- MomentumBot: Momentum-based trading using stop-limit orders
"""

from .momentum_bot import MomentumBot, MomentumConfig

__all__ = ['MomentumBot', 'MomentumConfig']
