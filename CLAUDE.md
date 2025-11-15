# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-exchange cryptocurrency perpetual trading bot that supports automated trading across EdgeX, Backpack, Paradex, Aster, Lighter, GRVT, Extended, and ApeX exchanges. The bot implements a grid-like strategy that places limit orders, waits for fills, and automatically closes positions at take-profit levels.

**Primary Purpose**: Generate high trading volume through automated market-making strategies, designed for users participating in exchange incentive programs (fee rebates, points, competitions).

**Key Warning**: This bot has NO stop-loss mechanism. It's designed for zero-wear volume generation over time, assuming price will eventually return to entry points. Not suitable for traditional profit-seeking strategies.

## Development Environment

### Python Setup

**Python Version Requirements**:
- Main environment: Python 3.10-3.12 (GRVT requires >=3.10, Paradex requires 3.9-3.12)
- Most exchanges: Python >=3.8

**Virtual Environments**:
```bash
# Main environment (for EdgeX, Backpack, Aster, Lighter, GRVT, Extended, ApeX)
python3 -m venv env
source env/bin/activate  # Windows: env\Scripts\activate
pip install -r requirements.txt

# GRVT users - additional dependency:
pip install grvt-pysdk

# ApeX users - additional dependencies:
pip install -r apex_requirements.txt

# Paradex - requires separate environment:
python3 -m venv para_env
source para_env/bin/activate  # Windows: para_env\Scripts\activate
pip install -r para_requirements.txt
```

### Frontend Setup (Next.js)

The `frontend/` directory contains a Next.js 15 dashboard for monitoring trading activity.

```bash
cd frontend
npm install
npm run dev      # Development server on http://localhost:3000
npm run build    # Production build
npm start        # Production server
npm run lint     # ESLint check
```

Tech stack: Next.js 15 (App Router), TypeScript, Tailwind CSS, Shadcn/ui components, Lucide icons

## Running the Bot

### Main Trading Bot (runbot.py)

```bash
# Basic usage
python runbot.py --exchange edgex --ticker ETH --quantity 0.1 --take-profit 0.02 --max-orders 40 --wait-time 450

# With grid step control (prevents close orders from clustering)
python runbot.py --exchange backpack --ticker BTC --quantity 0.05 --grid-step 0.3

# With stop-price control (exits when price reaches threshold)
python runbot.py --exchange edgex --ticker ETH --stop-price 5500 --direction buy

# Boost mode (maker open, taker close - only for Aster and Backpack)
python runbot.py --exchange backpack --ticker ETH --boost --direction buy

# Multiple accounts on same exchange
python runbot.py --env-file account_1.env --exchange backpack --ticker BTC
```

**Key Parameters**:
- `--exchange`: Exchange to use (edgex, backpack, paradex, aster, lighter, grvt, extended, apex)
- `--ticker`: Asset symbol (ETH, BTC, SOL, etc.) - contract ID is auto-resolved
- `--quantity`: Order size
- `--take-profit`: Take profit percentage (e.g., 0.02 = 0.02%)
- `--direction`: Trading direction (buy or sell)
- `--max-orders`: Maximum concurrent active orders (risk control)
- `--wait-time`: Seconds between placing new orders
- `--grid-step`: Minimum distance (%) between close order prices (-100 = disabled)
- `--stop-price`: Exit when price reaches this level
- `--pause-price`: Pause trading when price reaches this level (resumes when price reverses)
- `--boost`: Enable boost mode (maker open → taker close, only Aster/Backpack)
- `--env-file`: Path to .env file for multi-account setup

### Hedge Mode (hedge_mode.py)

Hedge mode executes opposing trades across two exchanges (main exchange + Lighter) to reduce directional risk while generating volume on both platforms.

```bash
# Backpack + Lighter hedge
python hedge_mode.py --exchange backpack --ticker BTC --size 0.05 --iter 20

# Extended + Lighter hedge
python hedge_mode.py --exchange extended --ticker ETH --size 0.1 --iter 20

# With sleep after each iteration
python hedge_mode.py --exchange apex --ticker BTC --size 0.05 --iter 20 --sleep 10
```

**Parameters**:
- `--exchange`: Primary exchange (backpack, extended, apex, grvt, edgex)
- `--ticker`: Asset symbol
- `--size`: Order quantity per trade
- `--iter`: Number of trading cycles
- `--fill-timeout`: Seconds to wait for maker order fill (default: 5)
- `--sleep`: Sleep after each step to increase position holding time

## Architecture

### Modular Exchange System

The codebase uses a **factory pattern** with a unified `BaseExchangeClient` interface. All exchange implementations inherit from this base class.

**Key Files**:
- `exchanges/base.py`: Abstract base class defining required methods (`place_open_order`, `place_close_order`, `cancel_order`, `get_active_orders`, `get_account_positions`, etc.)
- `exchanges/factory.py`: `ExchangeFactory` class with registry of all supported exchanges
- `exchanges/[exchange_name].py`: Individual exchange implementations
- `trading_bot.py`: Core trading logic that uses exchange clients polymorphically

**Important Data Structures** (defined in `base.py`):
- `OrderResult`: Standardized order response (success, order_id, side, size, price, status, error_message)
- `OrderInfo`: Order details (order_id, side, size, price, status, filled_size, remaining_size)
- `@query_retry` decorator: Automatic retry with exponential backoff for API calls

### Trading Bot Flow

1. **Initialization** (`trading_bot.py`):
   - `TradingBot.__init__()` creates exchange client via factory
   - Sets up WebSocket handlers for real-time order updates
   - Initializes logging, notification bots (Telegram/Lark)

2. **Main Loop** (`TradingBot.run()`):
   - Connects to exchange, retrieves contract attributes
   - Enters infinite loop:
     - Check price thresholds (stop_price, pause_price)
     - Check max concurrent orders limit
     - Place open order → Wait for fill → Place close order
     - Monitor active close orders and positions
     - Apply wait_time between new orders

3. **Order Lifecycle**:
   - Open order: Places limit order near best bid/ask (aims for maker rebate)
   - Fill detection: WebSocket callback sets `order_filled_event`
   - Close order: Places limit order at take_profit distance
   - Grid step logic: Validates close price doesn't violate minimum distance from existing close orders

### Hedge Mode Flow

Hedge implementations are in `hedge/hedge_mode_[exchange].py`. Each HedgeBot:

1. **Step 1**: Place maker order on primary exchange (e.g., Backpack)
2. **Step 2**: Wait for fill → immediately place market order on Lighter (opposite direction)
3. **Step 3**: Place maker order to close primary exchange position
4. **Step 4**: Wait for fill → market order to close Lighter position
5. Repeat for N iterations

**Key Concept**: Two-exchange hedging neutralizes directional risk while maximizing volume and points on both platforms.

### Logging and Helpers

- `helpers/logger.py`: `TradingLogger` class - CSV transaction logs + debug logs with timestamps
- `helpers/telegram_bot.py`: Optional Telegram notifications for order fills
- `helpers/lark_bot.py`: Optional Lark/Feishu notifications

Logs are automatically organized by exchange and ticker.

## Adding a New Exchange

Comprehensive guide in `docs/ADDING_EXCHANGES.md`. Summary:

1. **Create `exchanges/your_exchange.py`**:
   - Inherit from `BaseExchangeClient`
   - Implement all abstract methods
   - Use `@query_retry` decorator for API calls
   - Use `self.round_to_tick()` for price rounding
   - Set up WebSocket handlers via `setup_order_update_handler()`

2. **Register in `exchanges/factory.py`**:
   - Add entry to `_registered_exchanges` dict: `'your_exchange': 'exchanges.your_exchange.YourExchangeClient'`

3. **Update `exchanges/__init__.py`**:
   - Import and export the new client class

4. **Environment Variables**:
   - Add required API keys/secrets to `.env` file
   - Validate in `_validate_config()` method

5. **Test thoroughly**:
   - Use small quantities first
   - Verify order placement, cancellation, position tracking
   - Check WebSocket order update handling

## Configuration

### Environment Variables

Create `.env` file in project root (see `env_example.txt` for template).

**Common**:
- `ACCOUNT_NAME`: Optional account identifier for logging

**Exchange-specific** (see README_EN.md for full list):
- EdgeX: `EDGEX_ACCOUNT_ID`, `EDGEX_STARK_PRIVATE_KEY`
- Backpack: `BACKPACK_PUBLIC_KEY`, `BACKPACK_SECRET_KEY`
- Paradex: `PARADEX_L1_ADDRESS`, `PARADEX_L2_PRIVATE_KEY`
- Aster: `ASTER_API_KEY`, `ASTER_SECRET_KEY`
- Lighter: `API_KEY_PRIVATE_KEY`, `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_INDEX`
- GRVT: `GRVT_TRADING_ACCOUNT_ID`, `GRVT_PRIVATE_KEY`, `GRVT_API_KEY`
- Extended: `EXTENDED_API_KEY`, `EXTENDED_STARK_KEY_PUBLIC`, `EXTENDED_STARK_KEY_PRIVATE`, `EXTENDED_VAULT`
- ApeX: `APEX_API_KEY`, `APEX_API_KEY_PASSPHRASE`, `APEX_API_KEY_SECRET`, `APEX_OMNI_KEY_SEED`

**Notifications** (optional):
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

**Multi-Account Setup**:
- Create separate .env files (e.g., `account_1.env`, `account_2.env`)
- Set `ACCOUNT_NAME` in each file
- Use `--env-file` parameter to select account

## Testing

`tests/test_query_retry.py` contains tests for the retry decorator logic.

```bash
# Run tests
python -m pytest tests/

# Run specific test
python -m pytest tests/test_query_retry.py
```

## Important Implementation Notes

### Price Precision
- Always use `Decimal` for prices and quantities (never `float`)
- Use `self.round_to_tick(price)` to round to exchange tick size
- Tick size is fetched from exchange via `get_contract_attributes()`

### Order Types
- Default strategy uses POST_ONLY limit orders to avoid taker fees
- Boost mode intentionally uses taker orders for fast execution
- Most exchanges reject POST_ONLY orders that would match immediately

### WebSocket Handling
- Each exchange implements `setup_order_update_handler()` for real-time order updates
- The handler callback receives order updates and sets `order_filled_event` in TradingBot
- Critical for detecting fills without polling

### Async/Await
- All exchange methods are async
- Main bot uses `asyncio.Event()` for synchronization between WebSocket callbacks and main loop
- Use `await asyncio.sleep()` not `time.sleep()`

### Error Handling
- `@query_retry` decorator handles transient API failures
- Always return `OrderResult` objects with appropriate error messages
- Bot performs graceful shutdown on critical errors

### Grid Step Logic
When `--grid-step` is set (e.g., 0.5):
- Bot checks existing close order prices
- New close order must be at least 0.5% away from nearest close order
- Direction matters: for buy direction (long), new close price must be lower by grid_step%
- Prevents order clustering and improves fill probability

## Common Development Tasks

### Running a Single Test
```bash
python -m pytest tests/test_query_retry.py::test_specific_function -v
```

### Debugging WebSocket Issues
- Set log level to DEBUG in `runbot.py` line 91: `setup_logging("DEBUG")`
- Check WebSocket connection in exchange client's `connect()` method
- Verify order update handler is properly registered

### Testing a New Exchange
```bash
# Use small quantities and short wait times for testing
python runbot.py --exchange your_exchange --ticker BTC --quantity 0.001 --take-profit 0.05 --max-orders 1 --wait-time 10
```

### Monitoring Logs
Logs are created in project root with format: `[exchange]_[ticker]_[timestamp].csv` and `.log`

## Strategy Notes (from README)

**Author's Philosophy**:
- `--quantity`: Recommended 40-60 (not too small)
- `--wait-time`: Recommended 450-650 seconds (7-11 minutes)
- Goal: Ensure bot continues placing orders even in adverse market conditions
- Long-term zero-wear strategy: If price returns to highest trapped level after 1 month, all volume generated has zero cost

**Risk Warning**:
- No stop-loss mechanism
- Can accumulate large positions in trending markets
- Designed for incentive farming, not profit generation
- Author emphasizes: "Understand the logic and risks before setting parameters"

## Dependencies

Main dependencies (see `requirements.txt`):
- `python-dotenv`: Environment variable management
- `aiohttp`, `websockets`: Async HTTP and WebSocket
- `pydantic`: Data validation
- `tenacity`: Retry logic
- `bpx-py`: Backpack SDK
- `edgex-python-sdk`: Forked EdgeX SDK with POST_ONLY support
- `lighter-python`: Lighter exchange SDK
- `x10-python-trading-starknet`: Extended exchange SDK
- `grvt-pysdk`: GRVT SDK (optional, install separately)

## Frontend API Integration

The Next.js frontend communicates with a backend API (expected at `NEXT_PUBLIC_API_URL`).

Frontend structure:
- `app/`: Next.js 15 App Router pages
- `components/ui/`: Shadcn/ui components
- `lib/api.ts`: API client with `get()`, `post()`, `healthCheck()` methods
- `lib/utils.ts`: Utility functions

Add Shadcn components: `npx shadcn@latest add [component-name]`
