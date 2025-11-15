import { useState, useEffect } from "react";
import { Task, LogEntry, tasksAPI } from "@/lib/api/tasks";

export function useTasks() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchTasks();
    // Refresh tasks every 5 seconds
    const interval = setInterval(fetchTasks, 5000);
    return () => clearInterval(interval);
  }, []);

  const fetchTasks = async () => {
    try {
      const data = await tasksAPI.getTasks();
      setTasks(data);
      setError(null);
    } catch (err) {
      setError("Failed to fetch tasks");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const startTask = async (taskId: number) => {
    const success = await tasksAPI.startTask(taskId);
    if (success) {
      await fetchTasks();
    }
    return success;
  };

  const pauseTask = async (taskId: number) => {
    const success = await tasksAPI.pauseTask(taskId);
    if (success) {
      await fetchTasks();
    }
    return success;
  };

  const stopTask = async (taskId: number) => {
    const success = await tasksAPI.stopTask(taskId);
    if (success) {
      await fetchTasks();
    }
    return success;
  };

  const restartTask = async (taskId: number) => {
    const success = await tasksAPI.restartTask(taskId);
    if (success) {
      await fetchTasks();
    }
    return success;
  };

  return {
    tasks,
    loading,
    error,
    refetch: fetchTasks,
    startTask,
    pauseTask,
    stopTask,
    restartTask,
  };
}

export function useTaskLogs(taskId: number) {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    if (!taskId) return;

    // Fetch initial logs
    tasksAPI.getTaskLogs(taskId).then(setLogs);

    // Subscribe to real-time logs
    const unsubscribe = tasksAPI.subscribeToLogs(taskId, (newLog) => {
      setLogs((prev) => [...prev.slice(-200), newLog]); // Keep last 200 logs
      setConnected(true);
    });

    return () => {
      unsubscribe();
      setConnected(false);
    };
  }, [taskId]);

  return { logs, connected };
}