import { useMemo } from 'react';

export interface Process {
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
  primary_position?: number;
  secondary_position?: number;
  primary_exchange?: string;
  secondary_exchange?: string;
  runtime_seconds?: number;
  primary_rtt_ms?: number | null;
  secondary_rtt_ms?: number | null;
  total_volume_usd?: number | null;
  total_pnl?: number | null;
  cost_per_10k_usd?: number | null;
}

/**
 * Hook to get the currently selected task
 */
export function useSelectedTask(tasks: Process[], selectedTaskPid: number | null) {
  return useMemo(() => {
    if (!selectedTaskPid) return null;
    return tasks.find(t => t.pid === selectedTaskPid) || null;
  }, [tasks, selectedTaskPid]);
}

/**
 * Hook to calculate position delta
 */
export function usePositionDelta(task: Process | null) {
  return useMemo(() => {
    if (!task || task.primary_position === undefined || task.secondary_position === undefined) {
      return null;
    }
    return (task.primary_position ?? 0) + (task.secondary_position ?? 0);
  }, [task]);
}

/**
 * Hook to check if task has RTT data
 */
export function useHasRttData(task: Process | null) {
  return useMemo(() => {
    return task && (task.primary_rtt_ms !== undefined || task.secondary_rtt_ms !== undefined);
  }, [task]);
}

/**
 * Hook to check if task has trading performance data
 */
export function useHasTradingData(task: Process | null) {
  return useMemo(() => {
    if (!task) return false;
    const hasVolume = task.total_volume_usd !== undefined && task.total_volume_usd !== null;
    const hasPnl = task.total_pnl !== undefined && task.total_pnl !== null;
    const hasCost = task.cost_per_10k_usd !== undefined && task.cost_per_10k_usd !== null;
    return hasVolume || hasPnl || hasCost;
  }, [task]);
}
