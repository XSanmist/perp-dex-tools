"""
FastAPI server for running trading bots via HTTP API
Supports both runbot.py and hedge_mode.py operations
"""

import asyncio
import subprocess
import os
import re
import json
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect, Security, Depends
from fastapi.security import APIKeyHeader
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# ==================== API Key Authentication ====================

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

def get_api_keys() -> set:
    """Load API keys from environment variable"""
    api_keys_str = os.getenv("API_KEYS", "")
    if not api_keys_str:
        return set()
    # Split by comma and strip whitespace
    return {key.strip() for key in api_keys_str.split(",") if key.strip()}

async def verify_api_key(api_key: str = Security(API_KEY_HEADER)):
    """Verify API key from request header"""
    valid_api_keys = get_api_keys()

    # API keys must be configured
    if not valid_api_keys:
        raise HTTPException(
            status_code=500,
            detail="Server configuration error: API_KEYS not configured. Please contact administrator."
        )

    # Require valid key
    if not api_key or api_key not in valid_api_keys:
        raise HTTPException(
            status_code=403,
            detail="Invalid or missing API key. Please provide a valid X-API-Key header."
        )

    return api_key


app = FastAPI(
    title="Perp DEX Trading Bot API",
    description="API for managing perpetual trading bots across multiple exchanges",
    version="1.0.0"
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "*"],  # 允许前端访问
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有请求头
)


# ==================== Models ====================

class ExchangeType(str, Enum):
    BACKPACK = "backpack"
    EXTENDED = "extended"


class DirectionType(str, Enum):
    BUY = "buy"
    SELL = "sell"


class RunBotRequest(BaseModel):
    exchange: ExchangeType = Field(..., description="Exchange to trade on")
    ticker: str = Field(..., description="Trading pair ticker (e.g., ETH, BTC)")
    direction: DirectionType = Field(..., description="Trade direction: buy or sell")
    quantity: float = Field(..., gt=0, description="Quantity to trade")
    boost: bool = Field(False, description="Enable boost mode")


class HedgeModeRequest(BaseModel):
    exchange: ExchangeType = Field(..., description="Exchange to trade on (backpack or extended)")
    ticker: str = Field(..., description="Trading pair ticker (e.g., ETH, BTC)")
    size: float = Field(..., gt=0, description="Order size per iteration")
    iterations: int = Field(..., alias="iter", gt=0, description="Number of iterations to run")
    sleep: int = Field(0, ge=0, description="Sleep time in seconds after each step")
    fill_timeout: int = Field(5, ge=1, description="Timeout in seconds for maker order fills")


class MomentumBotRequest(BaseModel):
    exchange: ExchangeType = Field(..., description="Exchange to trade on (extended or lighter)")
    ticker: str = Field(..., description="Trading pair ticker (e.g., ETH, BTC)")
    quantity: float = Field(..., gt=0, description="Order size per trade")
    direction: DirectionType = Field(..., description="Trade direction: buy or sell")
    tick_offset: int = Field(..., gt=0, description="Number of ticks to offset from best price (e.g., 1 means 1 tick away)")
    take_profit_pct: float = Field(..., gt=0, description="Take profit percentage (e.g., 0.02 for 0.02%)")
    max_positions: int = Field(1, ge=1, description="Maximum concurrent positions")
    wait_time: int = Field(5, ge=1, description="Wait time between cycles in seconds")


class BotStatus(BaseModel):
    status: str
    message: str
    timestamp: str


class ProcessInfo(BaseModel):
    pid: Optional[int] = None
    command: str
    start_time: str
    current_iteration: Optional[int] = None
    total_iterations: Optional[int] = None
    extended_position: Optional[float] = None
    lighter_position: Optional[float] = None
    runtime_seconds: Optional[int] = None


# ==================== Global State ====================

running_processes: Dict[str, subprocess.Popen] = {}
process_info: Dict[str, ProcessInfo] = {}


# ==================== Helper Functions ====================

def get_current_timestamp() -> str:
    """Get current timestamp in ISO format (UTC)"""
    return datetime.utcnow().isoformat() + 'Z'


def load_process_status() -> Dict[str, Any]:
    """Load process status from shared JSON file"""
    status_file = "logs/process_status.json"
    if not os.path.exists(status_file):
        return {}

    try:
        with open(status_file, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def run_command_background(command: list, task_id: str) -> ProcessInfo:
    """Run a command in background and track its process"""
    try:
        # Prepare environment with task_id
        env = os.environ.copy()
        env['TASK_ID'] = task_id

        # Start the process
        # Don't capture stdout/stderr so logs go to Docker logs
        process = subprocess.Popen(
            command,
            stdout=None,  # Inherit from parent (Docker)
            stderr=None,  # Inherit from parent (Docker)
            env=env
        )

        # Store process and info
        running_processes[task_id] = process
        info = ProcessInfo(
            pid=process.pid,
            command=" ".join(command),
            start_time=get_current_timestamp()
        )
        process_info[task_id] = info

        return info

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute command: {str(e)}"
        )


# ==================== API Endpoints ====================

@app.get("/", response_model=BotStatus)
async def health_check():
    """Health check endpoint"""
    return BotStatus(
        status="ok",
        message="Perp DEX Trading Bot API is running",
        timestamp=get_current_timestamp()
    )


@app.post("/runbot", response_model=BotStatus)
async def run_bot(request: RunBotRequest, api_key: str = Depends(verify_api_key)):
    """
    Run a single trade using runbot.py

    Example:
    ```
    POST /runbot
    {
        "exchange": "backpack",
        "ticker": "ETH",
        "direction": "buy",
        "quantity": 0.1,
        "boost": true
    }
    ```
    """
    # Build command
    command = [
        "python3", "runbot.py",
        "--exchange", request.exchange.value,
        "--ticker", request.ticker,
        "--direction", request.direction.value,
        "--quantity", str(request.quantity)
    ]

    if request.boost:
        command.append("--boost")

    # Generate task ID
    task_id = f"runbot_{request.exchange.value}_{request.ticker}_{get_current_timestamp()}"

    # Run in background
    run_command_background(command, task_id)

    return BotStatus(
        status="started",
        message=f"RunBot task started: {' '.join(command)}",
        timestamp=get_current_timestamp()
    )


@app.post("/hedge", response_model=BotStatus)
async def run_hedge_mode(request: HedgeModeRequest, api_key: str = Depends(verify_api_key)):
    """
    Run hedge mode trading using hedge_mode.py

    Example:
    ```
    POST /hedge
    {
        "exchange": "extended",
        "ticker": "ETH",
        "size": 0.1,
        "iter": 20,
        "sleep": 5,
        "fill_timeout": 5
    }
    ```
    """
    # Build command
    command = [
        "python3", "hedge_mode.py",
        "--exchange", request.exchange.value,
        "--ticker", request.ticker,
        "--size", str(request.size),
        "--iter", str(request.iterations),
        "--sleep", str(request.sleep),
        "--fill-timeout", str(request.fill_timeout)
    ]

    # Generate task ID
    task_id = f"hedge_{request.exchange.value}_{request.ticker}_{get_current_timestamp()}"

    # Run in background
    run_command_background(command, task_id)

    return BotStatus(
        status="started",
        message=f"Hedge mode task started: {' '.join(command)}",
        timestamp=get_current_timestamp()
    )


@app.post("/momentum", response_model=BotStatus)
async def run_momentum_bot(request: MomentumBotRequest, api_key: str = Depends(verify_api_key)):
    """
    Run momentum trading bot using run_momentum.py

    Example:
    ```
    POST /momentum
    {
        "exchange": "extended",
        "ticker": "ETH",
        "quantity": 0.1,
        "direction": "buy",
        "tick_offset": 1,
        "take_profit_pct": 0.02,
        "max_positions": 1,
        "wait_time": 5
    }
    ```
    """
    # Build command
    command = [
        "python3", "run_momentum.py",
        "--exchange", request.exchange.value,
        "--ticker", request.ticker,
        "--quantity", str(request.quantity),
        "--direction", request.direction.value,
        "--tick-offset", str(request.tick_offset),
        "--take-profit-pct", str(request.take_profit_pct),
        "--max-positions", str(request.max_positions),
        "--wait-time", str(request.wait_time)
    ]

    # Generate task ID
    task_id = f"momentum_{request.exchange.value}_{request.ticker}_{get_current_timestamp()}"

    # Run in background
    run_command_background(command, task_id)

    return BotStatus(
        status="started",
        message=f"Momentum bot task started: {' '.join(command)}",
        timestamp=get_current_timestamp()
    )


@app.get("/processes", response_model=Dict[str, ProcessInfo])
async def list_processes(api_key: str = Depends(verify_api_key)):
    """List all currently running bot processes"""
    # Clean up terminated processes
    terminated_ids = []
    for task_id, process in running_processes.items():
        if process.poll() is not None:  # Process has terminated
            terminated_ids.append(task_id)

    for task_id in terminated_ids:
        del running_processes[task_id]
        if task_id in process_info:
            del process_info[task_id]

    # Load status from shared file and merge with process_info
    status_data = load_process_status()

    # Merge status data into process_info
    result = {}
    for task_id, info in process_info.items():
        info_dict = info.dict()

        # If we have additional status info from the bot, merge it
        if task_id in status_data:
            status = status_data[task_id]
            info_dict['current_iteration'] = status.get('current_iteration')
            info_dict['total_iterations'] = status.get('total_iterations')
            info_dict['extended_position'] = status.get('extended_position')
            info_dict['lighter_position'] = status.get('lighter_position')
            info_dict['runtime_seconds'] = status.get('runtime_seconds')

        result[task_id] = ProcessInfo(**info_dict)

    return result


@app.delete("/processes/{task_id}", response_model=BotStatus)
async def stop_process(task_id: str, api_key: str = Depends(verify_api_key)):
    """Stop a running process by task ID"""
    if task_id not in running_processes:
        raise HTTPException(
            status_code=404,
            detail=f"Task {task_id} not found or already stopped"
        )

    try:
        process = running_processes[task_id]

        # Terminate the process
        process.terminate()

        # Wait a bit for graceful termination
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            # Force kill if it doesn't terminate gracefully
            process.kill()
            process.wait()

        # Remove from tracking
        del running_processes[task_id]
        if task_id in process_info:
            del process_info[task_id]

        return BotStatus(
            status="stopped",
            message=f"Task {task_id} has been stopped",
            timestamp=get_current_timestamp()
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop task: {str(e)}"
        )


@app.post("/processes/{task_id}/stop", response_model=BotStatus)
async def stop_process_post(task_id: str, api_key: str = Depends(verify_api_key)):
    """Stop a running process by task ID (POST alternative for frontend compatibility)"""
    return await stop_process(task_id)


@app.get("/logs/{exchange}/{ticker}")
async def get_logs(
    exchange: ExchangeType,
    ticker: str,
    lines: int = 100
):
    """
    Get recent logs for a specific exchange and ticker

    Parameters:
    - exchange: Exchange name (backpack or extended)
    - ticker: Trading pair ticker
    - lines: Number of recent lines to return (default: 100)
    """
    # Construct log file path
    log_file = f"logs/{exchange.value}_{ticker}_hedge_mode_log.txt"

    if not os.path.exists(log_file):
        raise HTTPException(
            status_code=404,
            detail=f"Log file not found: {log_file}"
        )

    try:
        # Read last N lines
        with open(log_file, 'r') as f:
            all_lines = f.readlines()
            recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines

        return {
            "exchange": exchange.value,
            "ticker": ticker,
            "log_file": log_file,
            "lines_returned": len(recent_lines),
            "logs": "".join(recent_lines)
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read log file: {str(e)}"
        )


@app.get("/trades/{exchange}/{ticker}")
async def get_trades(exchange: ExchangeType, ticker: str):
    """
    Get trade history from CSV file

    Parameters:
    - exchange: Exchange name (backpack or extended)
    - ticker: Trading pair ticker
    """
    # Construct CSV file path
    csv_file = f"logs/{exchange.value}_{ticker}_hedge_mode_trades.csv"

    if not os.path.exists(csv_file):
        raise HTTPException(
            status_code=404,
            detail=f"Trade history file not found: {csv_file}"
        )

    try:
        with open(csv_file, 'r') as f:
            content = f.read()

        return {
            "exchange": exchange.value,
            "ticker": ticker,
            "csv_file": csv_file,
            "content": content
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read trade history: {str(e)}"
        )


@app.get("/logs/{exchange}/{ticker}/stream")
async def stream_logs(
    exchange: ExchangeType,
    ticker: str,
    follow: bool = True
):
    """
    Stream logs in real-time (like tail -f)

    Parameters:
    - exchange: Exchange name (backpack or extended)
    - ticker: Trading pair ticker
    - follow: If true, continuously stream new lines (default: true)

    Usage:
    ```bash
    # Stream logs in real-time
    curl "http://localhost:8000/logs/extended/ETH/stream"

    # Get existing logs without following
    curl "http://localhost:8000/logs/extended/ETH/stream?follow=false"
    ```
    """
    log_file = f"logs/{exchange.value}_{ticker}_hedge_mode_log.txt"

    if not os.path.exists(log_file):
        raise HTTPException(
            status_code=404,
            detail=f"Log file not found: {log_file}"
        )

    async def log_generator():
        """Generator that yields log lines"""
        try:
            # First, yield existing content
            with open(log_file, 'r') as f:
                for line in f:
                    yield line

            # If follow mode, continue watching for new lines
            if follow:
                with open(log_file, 'r') as f:
                    # Seek to end
                    f.seek(0, 2)

                    while True:
                        line = f.readline()
                        if line:
                            yield line
                        else:
                            # No new line, wait a bit
                            await asyncio.sleep(0.1)

        except Exception as e:
            yield f"Error reading log file: {str(e)}\n"

    return StreamingResponse(
        log_generator(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


@app.websocket("/ws/logs/{exchange}/{ticker}")
async def websocket_logs(websocket: WebSocket, exchange: str, ticker: str):
    """
    WebSocket endpoint for real-time log streaming

    Usage with websocat:
    ```bash
    websocat ws://localhost:8000/ws/logs/extended/ETH
    ```

    Usage with JavaScript:
    ```javascript
    const ws = new WebSocket('ws://localhost:8000/ws/logs/extended/ETH');
    ws.onmessage = (event) => {
        console.log(event.data);
    };
    ```
    """
    await websocket.accept()

    log_file = f"logs/{exchange}_{ticker}_hedge_mode_log.txt"

    if not os.path.exists(log_file):
        await websocket.send_text(f"Error: Log file not found: {log_file}")
        await websocket.close()
        return

    try:
        # Send existing content first
        with open(log_file, 'r') as f:
            for line in f:
                await websocket.send_text(line.rstrip('\n'))

        # Then follow new lines
        with open(log_file, 'r') as f:
            # Seek to end
            f.seek(0, 2)

            while True:
                line = f.readline()
                if line:
                    await websocket.send_text(line.rstrip('\n'))
                else:
                    # No new line, wait a bit
                    await asyncio.sleep(0.1)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        await websocket.send_text(f"Error: {str(e)}")
        await websocket.close()


# ==================== Main ====================

if __name__ == "__main__":
    # Run uvicorn server
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
