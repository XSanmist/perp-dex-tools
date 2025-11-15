"use client";

import { useState, useEffect, useRef } from "react";
import { Pause, Square, RefreshCw, Terminal, Activity, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { NewTaskDialog } from "@/components/NewTaskDialog";
import { cn } from "@/lib/utils";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";

interface Process {
  id: string;
  pid: number;
  command: string;
  start_time: string;
  status: string;
  name: string;
  exchange?: string;
  ticker?: string;
  current_iteration?: number;
  total_iterations?: number;
  extended_position?: number;
  lighter_position?: number;
  runtime_seconds?: number;
}

export default function Home() {
  const [tasks, setTasks] = useState<Process[]>([]);
  const [selectedTask, setSelectedTask] = useState<number | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [autoScroll, setAutoScroll] = useState(true);
  const [loading, setLoading] = useState(true);
  const [showNewTaskModal, setShowNewTaskModal] = useState(false);
  const [taskToStop, setTaskToStop] = useState<{ id: string; name: string } | null>(null);
  const [, setCurrentTime] = useState(Date.now()); // 用于触发运行时间的更新
  const logsEndRef = useRef<HTMLDivElement>(null);
  const logIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // 获取任务列表
  const fetchProcesses = async () => {
    try {
      const response = await fetch('http://localhost:8000/processes');
      if (response.ok) {
        const data = await response.json();
        // 将对象转换为数组
        const processList: Process[] = Object.entries(data).map(([id, info]: [string, any]) => {
          // 从命令中解析exchange和ticker
          const commandParts = info.command.split(' ');
          const exchangeIndex = commandParts.indexOf('--exchange');
          const tickerIndex = commandParts.indexOf('--ticker');
          const exchange = exchangeIndex !== -1 ? commandParts[exchangeIndex + 1] : undefined;
          const ticker = tickerIndex !== -1 ? commandParts[tickerIndex + 1] : undefined;

          // 从任务ID中提取名称
          const nameParts = id.split('_');
          const taskType = nameParts[0]; // 'hedge' or 'runbot'

          return {
            id,
            pid: info.pid,
            command: info.command,
            start_time: info.start_time,
            status: 'running', // 假设所有在列表中的任务都是运行中
            name: `${taskType.toUpperCase()} ${ticker || ''}`.trim(),
            exchange,
            ticker,
            current_iteration: info.current_iteration,
            total_iterations: info.total_iterations,
            extended_position: info.extended_position,
            lighter_position: info.lighter_position,
            runtime_seconds: info.runtime_seconds
          };
        });

        setTasks(processList);

        // 如果有任务且没有选中的，选中第一个
        if (processList.length > 0 && !selectedTask) {
          setSelectedTask(processList[0].pid);
        }
      }
    } catch (error) {
      console.error('Failed to fetch processes:', error);
    } finally {
      setLoading(false);
    }
  };

  // 获取日志
  const fetchLogs = async (pid: number) => {
    try {
      // 找到对应的任务获取exchange和ticker信息
      const task = tasks.find(t => t.pid === pid);
      if (!task || !task.exchange || !task.ticker) {
        console.error('Task not found or missing exchange/ticker');
        return;
      }

      const response = await fetch(`http://localhost:8000/logs/${task.exchange}/${task.ticker}?lines=100`);
      if (response.ok) {
        const data = await response.json();
        if (data.logs) {
          // 将日志文本按行分割
          const logLines = data.logs.split('\n').filter((line: string) => line.trim());
          setLogs(logLines);
        }
      }
    } catch (error) {
      console.error('Failed to fetch logs:', error);
    }
  };

  // 停止任务
  const stopTask = async (taskId: string) => {
    try {
      const response = await fetch(`http://localhost:8000/processes/${taskId}/stop`, {
        method: 'POST'
      });

      if (response.ok) {
        // 刷新任务列表
        await fetchProcesses();
        console.log(`Task ${taskId} stopped successfully`);

        // 如果停止的是当前选中的任务，清除选中状态
        const stoppedTask = tasks.find(t => t.id === taskId);
        if (stoppedTask && selectedTask === stoppedTask.pid) {
          setSelectedTask(null);
          setLogs([]);
        }
      } else {
        const error = await response.text();
        console.error('Failed to stop task:', error);
      }
    } catch (error) {
      console.error('Failed to stop task:', error);
    } finally {
      setTaskToStop(null);
    }
  };

  // 创建新任务
  const createNewTask = async (formData: any) => {
    try {
      let endpoint = '/runbot';
      let body: any = {};

      if (formData.type === 'hedge') {
        endpoint = '/hedge';
        body = {
          exchange: formData.exchange,
          ticker: formData.ticker,
          size: parseFloat(formData.size),
          iter: parseInt(formData.iterations),
          sleep: parseInt(formData.sleep),
          fill_timeout: parseInt(formData.fillTimeout)
        };
      } else if (formData.type === 'momentum') {
        endpoint = '/momentum';
        body = {
          exchange: formData.exchange,
          ticker: formData.ticker,
          quantity: parseFloat(formData.quantity),
          direction: formData.direction,
          tick_offset: parseInt(formData.tickOffset),
          take_profit_pct: parseFloat(formData.takeProfitPct),
          max_positions: parseInt(formData.maxPositions),
          wait_time: parseInt(formData.waitTime)
        };
      } else {
        // runbot
        body = {
          exchange: formData.exchange,
          ticker: formData.ticker,
          direction: formData.direction,
          quantity: parseFloat(formData.quantity),
          boost: formData.boost
        };
      }

      const response = await fetch(`http://localhost:8000${endpoint}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
      });

      if (response.ok) {
        // 刷新任务列表
        await fetchProcesses();
      } else {
        const error = await response.text();
        throw new Error(error);
      }
    } catch (error) {
      console.error('Failed to create task:', error);
      throw error;
    }
  };

  // 初始化时获取任务列表
  useEffect(() => {
    fetchProcesses();
    // 每5秒刷新任务列表
    const interval = setInterval(fetchProcesses, 5000);
    return () => clearInterval(interval);
  }, []);

  // 每秒更新运行时间显示
  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(Date.now());
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  // 当选中任务改变时，获取日志
  useEffect(() => {
    if (selectedTask && tasks.length > 0) {
      // 立即获取日志
      fetchLogs(selectedTask);

      // 清理之前的定时器
      if (logIntervalRef.current) {
        clearInterval(logIntervalRef.current);
      }

      // 每2秒获取新日志
      logIntervalRef.current = setInterval(() => {
        fetchLogs(selectedTask);
      }, 2000);
    }

    return () => {
      if (logIntervalRef.current) {
        clearInterval(logIntervalRef.current);
      }
    };
  }, [selectedTask, tasks]);

  // Auto-scroll to bottom when new logs arrive
  useEffect(() => {
    if (autoScroll && logsEndRef.current) {
      // 使用 scrollTop 只滚动日志容器，而不是整个页面
      const logContainer = logsEndRef.current.parentElement;
      if (logContainer) {
        logContainer.scrollTop = logContainer.scrollHeight;
      }
    }
  }, [logs, autoScroll]);

  function getLogStyle(log: string) {
    if (log.includes("[ERROR]")) return "text-red-400";
    if (log.includes("[WARN]")) return "text-yellow-400";
    if (log.includes("[SUCCESS]")) return "text-green-400";
    return "text-gray-300";
  }

  // 计算任务运行时间（秒）
  // startTime 格式: "2025-11-08T06:09:50.225285Z" (UTC时间，带Z后缀)
  // JavaScript 会自动将 UTC 时间转换为本地时间进行计算
  function calculateRuntime(startTime: string): number {
    const start = new Date(startTime); // 解析 UTC 时间
    const now = new Date(); // 当前本地时间
    // 计算时间差（毫秒），与时区无关
    return Math.floor((now.getTime() - start.getTime()) / 1000);
  }

  // 格式化运行时间显示
  function formatRuntime(seconds?: number): string {
    if (!seconds) return "0s";
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;

    if (hours > 0) {
      return `${hours}h ${minutes}m ${secs}s`;
    } else if (minutes > 0) {
      return `${minutes}m ${secs}s`;
    } else {
      return `${secs}s`;
    }
  }

  // 获取任务的运行时间
  function getTaskRuntime(task: Process): string {
    const runtimeSeconds = calculateRuntime(task.start_time);
    return formatRuntime(runtimeSeconds);
  }

  return (
    <div className="min-h-screen bg-background p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex justify-between items-center mb-8">
          <h1 className="text-3xl font-bold text-white">Task Monitor</h1>
          <Button
            onClick={() => setShowNewTaskModal(true)}
          >
            <Plus className="w-4 h-4 mr-2" />
            New Task
          </Button>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Task List */}
          <div className="lg:col-span-1">
            <Card className="bg-card border-gray-800">
              <CardHeader>
                <CardTitle className="text-lg text-white">Active Tasks</CardTitle>
                <CardDescription className="text-gray-400">
                  Click on a task to view logs
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                {loading ? (
                  <div className="text-center py-8 text-gray-500">
                    Loading tasks...
                  </div>
                ) : tasks.length === 0 ? (
                  <div className="text-center py-8 text-gray-500">
                    No running tasks
                  </div>
                ) : (
                  tasks.map((task) => (
                    <Card
                      key={task.id}
                      className={cn(
                        "bg-secondary border cursor-pointer transition-all hover:bg-secondary/80",
                        selectedTask === task.pid
                          ? "border-primary bg-secondary/80"
                          : "border-gray-700"
                      )}
                      onClick={() => setSelectedTask(task.pid)}
                    >
                      <CardContent className="p-4">
                        <div className="flex items-center justify-between mb-2">
                          <div className="flex items-center gap-2">
                            <Activity className="w-4 h-4 text-green-400" />
                            <span className="font-medium text-white">{task.name}</span>
                          </div>
                          <span className="text-xs px-2 py-1 bg-green-900 text-green-400 rounded">
                            {task.status}
                          </span>
                        </div>

                        <div className="space-y-1 text-sm text-gray-400">
                          <div className="flex justify-between">
                            <span>Runtime:</span>
                            <span className="text-gray-300">{getTaskRuntime(task)}</span>
                          </div>
                          {task.current_iteration !== undefined && task.total_iterations !== undefined && (
                            <div className="space-y-1">
                              <div className="flex justify-between">
                                <span>Progress:</span>
                                <span className="text-gray-300">{task.current_iteration}/{task.total_iterations}</span>
                              </div>
                              <div className="w-full bg-gray-700 rounded-full h-1.5">
                                <div
                                  className="bg-primary h-1.5 rounded-full transition-all duration-300"
                                  style={{ width: `${(task.current_iteration / task.total_iterations) * 100}%` }}
                                ></div>
                              </div>
                            </div>
                          )}
                          {task.extended_position !== undefined && task.extended_position !== null && (
                            <div className="flex justify-between">
                              <span>Extended:</span>
                              <span className="text-gray-300">{task.extended_position.toFixed(4)}</span>
                            </div>
                          )}
                          {task.lighter_position !== undefined && task.lighter_position !== null && (
                            <div className="flex justify-between">
                              <span>Lighter:</span>
                              <span className="text-gray-300">{task.lighter_position.toFixed(4)}</span>
                            </div>
                          )}
                        </div>

                        <div className="flex gap-2 mt-3">
                          <Button
                            size="sm"
                            variant="outline"
                            className="flex-1"
                            onClick={(e) => {
                              e.stopPropagation();
                              // Handle pause/resume
                            }}
                          >
                            <Pause className="w-3 h-3" />
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            className="flex-1"
                            onClick={(e) => {
                              e.stopPropagation();
                              setTaskToStop({ id: task.id, name: task.name });
                            }}
                          >
                            <Square className="w-3 h-3" />
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            className="flex-1"
                            onClick={(e) => {
                              e.stopPropagation();
                              // Handle restart
                            }}
                          >
                            <RefreshCw className="w-3 h-3" />
                          </Button>
                        </div>
                      </CardContent>
                    </Card>
                  ))
                )}
              </CardContent>
            </Card>
          </div>

          {/* Right Column */}
          <div className="lg:col-span-2">
            {/* Stats Cards */}
            <div className="grid grid-cols-3 gap-4 mb-6">
              <Card className="bg-card border-gray-800">
                <CardContent className="p-4">
                  <div className="text-sm text-gray-400 mb-1">Runtime</div>
                  <div className="text-2xl font-bold text-white">
                    {selectedTask
                      ? (() => {
                          const task = tasks.find(t => t.pid === selectedTask);
                          return task ? getTaskRuntime(task) : "0s";
                        })()
                      : "0s"}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">Current session</div>
                </CardContent>
              </Card>
              <Card className="bg-card border-gray-800">
                <CardContent className="p-4">
                  <div className="text-sm text-gray-400 mb-1">Progress</div>
                  <div className="text-2xl font-bold text-white">
                    {selectedTask
                      ? (() => {
                          const task = tasks.find(t => t.pid === selectedTask);
                          if (task?.current_iteration !== undefined && task?.total_iterations !== undefined) {
                            return `${task.current_iteration}/${task.total_iterations}`;
                          }
                          return "N/A";
                        })()
                      : "N/A"}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    {selectedTask
                      ? (() => {
                          const task = tasks.find(t => t.pid === selectedTask);
                          if (task?.current_iteration !== undefined && task?.total_iterations !== undefined) {
                            const percentage = ((task.current_iteration / task.total_iterations) * 100).toFixed(1);
                            return `${percentage}% complete`;
                          }
                          return "Waiting...";
                        })()
                      : "Select a task"}
                  </div>
                </CardContent>
              </Card>
              <Card className={(() => {
                const task = tasks.find(t => t.pid === selectedTask);
                if (task?.extended_position !== undefined && task?.lighter_position !== undefined) {
                  const delta = (task.extended_position ?? 0) + (task.lighter_position ?? 0);
                  return delta !== 0 ? "bg-card border-red-500" : "bg-card border-gray-800";
                }
                return "bg-card border-gray-800";
              })()}>
                <CardContent className="p-4">
                  <div className="text-sm text-gray-400 mb-1">Positions</div>
                  <div className="text-lg font-bold text-white">
                    {selectedTask
                      ? (() => {
                          const task = tasks.find(t => t.pid === selectedTask);
                          if (task?.extended_position !== undefined && task?.lighter_position !== undefined) {
                            const delta = (task.extended_position ?? 0) + (task.lighter_position ?? 0);
                            return (
                              <div className="space-y-1">
                                <div className="flex justify-between text-sm">
                                  <span className="text-gray-400">Extended:</span>
                                  <span>{(task.extended_position ?? 0).toFixed(4)}</span>
                                </div>
                                <div className="flex justify-between text-sm">
                                  <span className="text-gray-400">Lighter:</span>
                                  <span>{(task.lighter_position ?? 0).toFixed(4)}</span>
                                </div>
                                <div className="flex justify-between text-sm border-t border-gray-700 pt-1 mt-1">
                                  <span className="text-gray-400">Delta:</span>
                                  <span className={delta !== 0 ? "text-red-400" : ""}>{delta.toFixed(4)}</span>
                                </div>
                              </div>
                            );
                          }
                          return "N/A";
                        })()
                      : "N/A"}
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Log Viewer */}
            <Card className="bg-card border-gray-800 h-[600px] flex flex-col">
              <CardHeader className="flex-shrink-0">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Terminal className="w-5 h-5 text-gray-400" />
                    <CardTitle className="text-lg text-white">Live Logs</CardTitle>
                    <span className="px-2 py-1 text-xs bg-green-900 text-green-400 rounded animate-pulse">
                      STREAMING
                    </span>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      variant={autoScroll ? "default" : "secondary"}
                      onClick={() => setAutoScroll(!autoScroll)}
                      className="text-xs"
                    >
                      Auto-scroll
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      onClick={() => setLogs([])}
                      className="text-xs"
                    >
                      Clear
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="flex-1 overflow-hidden p-0">
                <div className="h-full overflow-y-auto bg-black p-4 font-mono text-xs">
                  {logs.length === 0 ? (
                    <div className="text-center text-gray-500 mt-8">
                      Waiting for logs...
                    </div>
                  ) : (
                    logs.map((log, index) => (
                      <div key={index} className={cn("py-0.5", getLogStyle(log))}>
                        {log}
                      </div>
                    ))
                  )}
                  <div ref={logsEndRef} />
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>

      {/* New Task Modal */}
      <NewTaskDialog
        open={showNewTaskModal}
        onOpenChange={setShowNewTaskModal}
        onCreateTask={createNewTask}
      />

      {/* Stop Task Confirmation Dialog */}
      <AlertDialog open={!!taskToStop} onOpenChange={(open) => !open && setTaskToStop(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Stop Task</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to stop <span className="font-semibold text-foreground">{taskToStop?.name}</span>?
              This action cannot be undone and the task will be terminated immediately.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => taskToStop && stopTask(taskToStop.id)}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Stop Task
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}