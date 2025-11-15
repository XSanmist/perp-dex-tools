# Task Monitor 前端界面

## 功能特性

### 📊 任务监控
- **实时任务列表**：显示所有正在运行、暂停或停止的任务
- **任务控制**：支持启动、暂停、停止和重启任务
- **状态指示器**：通过颜色和图标直观显示任务状态

### 📝 实时日志
- **实时日志流**：WebSocket 连接，实时接收任务日志
- **日志级别高亮**：
  - 🔴 ERROR - 红色
  - 🟡 WARN - 黄色
  - 🟢 SUCCESS - 绿色
  - ⚪ INFO - 灰色
- **自动滚动**：新日志自动滚动到底部
- **日志缓存**：保留最近 200 条日志

### 📈 任务统计
- 总交易数
- 成功率
- 累计盈亏

## UI 风格

采用类似 Lighter.xyz 的暗黑主题设计：
- 背景色：`#0a0a0a` (纯黑)
- 卡片背景：`bg-gray-950/50` (半透明灰黑)
- 边框：`border-gray-800`
- 悬停效果：`hover:border-gray-700`

## API 集成

### HTTP 端点
```typescript
GET  /api/tasks              // 获取所有任务
GET  /api/tasks/:id/logs     // 获取任务日志
POST /api/tasks/:id/start    // 启动任务
POST /api/tasks/:id/pause    // 暂停任务
POST /api/tasks/:id/stop     // 停止任务
POST /api/tasks/:id/restart  // 重启任务
```

### WebSocket
```typescript
ws://localhost:8000/ws/tasks/:id/logs  // 订阅实时日志
```

## 使用的 Hooks

### `useTasks()`
管理任务列表的自定义 Hook：
- 自动每 5 秒刷新任务列表
- 提供任务操作方法（启动、暂停、停止、重启）

### `useTaskLogs(taskId)`
管理任务日志的自定义 Hook：
- 获取历史日志
- 订阅实时日志流
- 自动管理 WebSocket 连接

## 开发环境配置

在 `.env.local` 中配置：
```env
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000
```

## 组件结构

```
app/
├── page.tsx          # 主页面（任务监控）
├── layout.tsx        # 根布局

lib/
├── api.ts           # API 客户端基类
└── api/
    └── tasks.ts     # 任务 API 模块

hooks/
└── useTasks.ts      # 任务相关 Hooks

components/
└── theme-toggle.tsx # 主题切换组件
```

## 待实现功能

1. **新建任务对话框**：点击 "New Task" 按钮创建新任务
2. **任务配置编辑**：编辑任务参数
3. **日志过滤**：按级别过滤日志
4. **日志导出**：导出日志到文件
5. **任务历史**：查看已完成的任务
6. **性能图表**：实时显示任务性能指标

## 后端集成要求

后端需要实现以下功能：

1. **FastAPI 端点**：提供任务 CRUD 操作
2. **WebSocket 支持**：实时推送日志
3. **任务管理**：支持启动、暂停、停止、重启
4. **日志格式**：
   ```json
   {
     "timestamp": "2024-11-08 10:30:00",
     "level": "INFO",
     "message": "Task started successfully"
   }
   ```

## 启动项目

```bash
cd frontend
npm run dev
```

访问 http://localhost:3000 查看任务监控界面。