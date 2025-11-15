# 阶段 1: 依赖安装和构建
FROM node:20-alpine AS builder

WORKDIR /app

# 复制 package 文件并安装所有依赖(包括 devDependencies)
COPY package*.json ./
RUN npm ci

# 复制项目文件并构建
COPY . .
RUN npm run build

# 阶段 2: 生产运行
FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

# 复制 package 文件并只安装生产依赖
COPY package*.json ./
RUN npm ci --only=production

# 从构建阶段复制构建产物
COPY --from=builder /app/.next ./.next

# 暴露端口并启动应用
EXPOSE 3000
CMD ["npm", "start"]
