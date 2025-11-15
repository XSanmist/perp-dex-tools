# Perp DEX Tools - Frontend

基于 Next.js 14 和 Shadcn/ui 构建的永续合约交易工具前端界面。

## 技术栈

- **Framework**: Next.js 16 (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **UI Components**: Shadcn/ui
- **Icons**: Lucide React

## 开发指南

### 环境要求

- Node.js 20+
- npm 或 yarn

### 安装依赖

```bash
npm install
```

### 开发模式

```bash
npm run dev
```

访问 http://localhost:3000 查看应用。

### 构建生产版本

```bash
npm run build
npm start
```

### 代码检查

```bash
npm run lint
```

## 目录结构

```
frontend/
├── app/                 # Next.js App Router
│   ├── layout.tsx      # 根布局
│   ├── page.tsx        # 首页
│   └── globals.css     # 全局样式
├── components/          # React 组件
│   └── ui/             # Shadcn/ui 组件
├── lib/                # 工具库
│   ├── utils.ts        # 通用工具函数
│   └── api.ts          # API 客户端
└── public/             # 静态资源
```

## 环境变量

创建 `.env.local` 文件：

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
```

## 添加 UI 组件

使用 Shadcn/ui CLI 添加组件：

```bash
# 添加按钮组件
npx shadcn@latest add button

# 添加卡片组件
npx shadcn@latest add card

# 添加表格组件
npx shadcn@latest add table

# 查看所有可用组件
npx shadcn@latest add
```

## Docker 部署

### 构建镜像

```bash
docker build -t perp-dex-frontend .
```

### 运行容器

```bash
docker run -p 3000:3000 perp-dex-frontend
```

### 使用 Docker Compose

```bash
docker-compose up -d
```

## API 集成

前端通过 `lib/api.ts` 中的 ApiClient 与后端通信：

```typescript
import { apiClient } from '@/lib/api';

// GET 请求示例
const data = await apiClient.get('/api/endpoint');

// POST 请求示例
const result = await apiClient.post('/api/endpoint', { key: 'value' });

// 健康检查
const health = await apiClient.healthCheck();
```

## 开发提示

1. **组件开发**: 优先使用 Shadcn/ui 组件，保持 UI 一致性
2. **样式管理**: 使用 Tailwind CSS 类名，避免自定义 CSS
3. **类型安全**: 充分利用 TypeScript 类型系统
4. **服务端渲染**: 合理使用 Next.js 的 SSR/SSG 特性
5. **性能优化**: 使用 Next.js Image 组件优化图片加载

## 常用命令

```bash
# 开发
npm run dev

# 构建
npm run build

# 生产运行
npm start

# 代码检查
npm run lint

# 添加 UI 组件
npx shadcn@latest add [component-name]
```

## 相关链接

- [Next.js 文档](https://nextjs.org/docs)
- [Shadcn/ui 文档](https://ui.shadcn.com)
- [Tailwind CSS 文档](https://tailwindcss.com/docs)
- [TypeScript 文档](https://www.typescriptlang.org/docs)
