# 使用 Python 3.10 Linux AMD64 镜像（支持 lighter 依赖）
FROM --platform=linux/amd64 python:3.10-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    git \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# 复制项目文件（排除 frontend 和不必要的文件）
COPY requirements.txt .
COPY apex_requirements.txt .
COPY para_requirements.txt .
COPY *.py ./
COPY exchanges ./exchanges
COPY helpers ./helpers
COPY hedge ./hedge
COPY strategies ./strategies
COPY bot ./bot

# 创建日志目录
RUN mkdir -p /app/logs

# 设置日志目录为数据卷挂载点
VOLUME ["/app/logs"]

# 暴露端口（为未来的 API 服务预留）
EXPOSE 8000

# 默认命令：使用 uvicorn 启动 FastAPI 服务
CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]
