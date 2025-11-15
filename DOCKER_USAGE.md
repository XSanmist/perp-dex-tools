# Docker 使用指南

## 快速开始

### 1. 配置环境变量

```bash
# 复制配置模板
cp .env.example .env

# 生成 API Key
python -c "import secrets; print(secrets.token_urlsafe(32))"

# 编辑 .env 文件，配置：
# - API_KEYS=<生成的key>
# - 交易所 API 凭证
```

### 2. 启动服务

```bash
# 启动前端 + 后端（默认）
docker-compose up -d --build

# 仅启动后端
docker-compose -f docker-compose.bk.yml up -d --build
```

### 3. 访问服务

- **前端界面**: http://localhost:3000
- **后端 API**: http://localhost:8000
- **API 文档**: http://localhost:8000/docs

### 4. 配置前端 API Key

1. 打开前端 http://localhost:3000
2. 点击右上角 **Settings** 按钮
3. 输入 `.env` 中配置的 `API_KEYS`

## 常用命令

```bash
# 查看日志
docker-compose logs -f backend
docker-compose logs -f frontend

# 停止服务
docker-compose down

# 重启服务
docker-compose restart backend
docker-compose restart frontend

# 进入容器调试
docker exec -it perp-dex-backend /bin/bash
docker exec -it perp-dex-frontend /bin/sh
```

## 故障排查

### API 返回 403 错误
**原因**: API Key 未配置或不正确
**解决**:
- 确保 `.env` 中配置了 `API_KEYS`
- 前端 Settings 中输入正确的 API Key

### API 返回 500 错误
**原因**: 后端未配置 `API_KEYS`
**解决**:
```bash
# 在 .env 文件中添加
API_KEYS=your_api_key_here

# 重启后端
docker-compose restart backend
```

### 前端无法连接后端
**解决**:
```bash
# 检查后端是否运行
docker ps | grep perp-dex-backend

# 查看后端日志
docker-compose logs backend
```

## API 使用示例

### curl

```bash
# 获取任务列表
curl -H "X-API-Key: your_api_key" http://localhost:8000/processes

# 创建任务
curl -X POST http://localhost:8000/runbot \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "exchange": "backpack",
    "ticker": "SOL-PERP",
    "direction": "long",
    "quantity": 0.1,
    "boost": false
  }'

# 停止任务
curl -X POST http://localhost:8000/processes/{task_id}/stop \
  -H "X-API-Key: your_api_key"
```

### Python

```python
import requests

headers = {
    "Content-Type": "application/json",
    "X-API-Key": "your_api_key"
}

# 获取任务列表
response = requests.get("http://localhost:8000/processes", headers=headers)

# 创建任务
payload = {
    "exchange": "backpack",
    "ticker": "SOL-PERP",
    "direction": "long",
    "quantity": 0.1,
    "boost": False
}
response = requests.post("http://localhost:8000/runbot", json=payload, headers=headers)
```
