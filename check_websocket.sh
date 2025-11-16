#!/bin/bash

echo "=== WebSocket 诊断工具 ==="
echo ""

# 1. 检查 API 服务器是否在运行
echo "1. 检查 API 服务器状态..."
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "   ✓ API 服务器正在运行"
else
    echo "   ✗ API 服务器未响应"
    echo "   请检查 Docker 容器是否正在运行"
    exit 1
fi

# 2. 检查最新任务
echo ""
echo "2. 最新的任务 ID:"
python3 << 'EOF'
import json
with open('logs/process_status.json') as f:
    data = json.load(f)
    latest_tasks = list(data.keys())[-3:]
    for task in latest_tasks:
        print(f"   - {task}")
        print(f"     轮数: {data[task]['current_iteration']}/{data[task]['total_iterations']}")
EOF

# 3. 检查日志文件
echo ""
echo "3. 检查日志文件:"
LOG_FILE="logs/hedge_dual_grvt_extended_ETH.log"
if [ -f "$LOG_FILE" ]; then
    SIZE=$(ls -lh "$LOG_FILE" | awk '{print $5}')
    LINES=$(wc -l < "$LOG_FILE")
    echo "   ✓ 文件存在: $LOG_FILE"
    echo "   ✓ 大小: $SIZE"
    echo "   ✓ 行数: $LINES"
    echo ""
    echo "   最后 3 行:"
    tail -3 "$LOG_FILE" | sed 's/^/     /'
else
    echo "   ✗ 文件不存在"
fi

# 4. 测试 WebSocket 连接
echo ""
echo "4. 测试 WebSocket 连接 (需要 wscat 或 websocat)..."
echo "   如果安装了 wscat，运行:"
echo "   wscat -c ws://localhost:8000/ws/logs/hedge_dual_grvt_extended_ETH_2025-11-16T12:17:48.822515Z"
echo ""
echo "   或使用 Python 测试:"
echo "   python3 test_websocket_simple.py"

echo ""
echo "=== 诊断完成 ==="
