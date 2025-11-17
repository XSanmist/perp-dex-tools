import { Card, CardContent } from "@/components/ui/card";
import { Process } from "@/hooks/useSelectedTask";
import { MetricRow } from "@/components/MetricRow";
import { formatUSD } from "@/lib/formatters";

interface NetworkLatencyCardProps {
  task: Process;
}

export function NetworkLatencyCard({ task }: NetworkLatencyCardProps) {
  const hasPrimary = task.primary_rtt_ms !== undefined && task.primary_rtt_ms !== null;
  const hasSecondary = task.secondary_rtt_ms !== undefined && task.secondary_rtt_ms !== null;

  if (!hasPrimary && !hasSecondary) return null;

  return (
    <Card className="bg-card border-gray-800">
      <CardContent className="p-4">
        <div className="text-sm text-gray-400 mb-2">Network Latency</div>
        <div className="space-y-2">
          {hasPrimary && (
            <MetricRow
              label={`${task.primary_exchange || 'Primary'}:`}
              value={`${task.primary_rtt_ms!.toFixed(1)} ms`}
            />
          )}
          {hasSecondary && (
            <MetricRow
              label={`${task.secondary_exchange || 'Secondary'}:`}
              value={`${task.secondary_rtt_ms!.toFixed(1)} ms`}
            />
          )}
        </div>
      </CardContent>
    </Card>
  );
}

interface TradingPerformanceCardProps {
  task: Process;
}

export function TradingPerformanceCard({ task }: TradingPerformanceCardProps) {
  const hasVolume = task.total_volume_usd !== undefined && task.total_volume_usd !== null;
  const hasPnl = task.total_pnl !== undefined && task.total_pnl !== null;
  const hasCost = task.cost_per_10k_usd !== undefined && task.cost_per_10k_usd !== null;

  if (!hasVolume && !hasPnl && !hasCost) return null;

  return (
    <Card className="bg-card border-gray-800">
      <CardContent className="p-4">
        <div className="text-sm text-gray-400 mb-2">Trading Performance</div>
        <div className="space-y-2">
          {hasVolume && (
            <MetricRow
              label="Volume:"
              value={`$${formatUSD(task.total_volume_usd!)}`}
            />
          )}
          {hasPnl && (
            <MetricRow
              label="P&L:"
              value={`${task.total_pnl! >= 0 ? '+' : ''}$${formatUSD(task.total_pnl!)}`}
              valueClassName={task.total_pnl! >= 0 ? 'text-green-400' : 'text-red-400'}
            />
          )}
          {hasCost && (
            <MetricRow
              label="Cost Rate:"
              value={`$${formatUSD(task.cost_per_10k_usd!)} / 10k`}
              valueClassName="text-orange-400"
            />
          )}
        </div>
      </CardContent>
    </Card>
  );
}

interface PositionsCardProps {
  task: Process;
  delta: number;
}

export function PositionsCard({ task, delta }: PositionsCardProps) {
  const borderClass = delta !== 0 ? "border-red-500" : "border-gray-800";

  return (
    <Card className={`bg-card ${borderClass}`}>
      <CardContent className="p-4">
        <div className="text-sm text-gray-400 mb-2">Positions</div>
        <div className="space-y-1.5">
          <div className="flex justify-between items-center">
            <span className="text-xs text-gray-400">{task.primary_exchange || 'Primary'}:</span>
            <span className="text-base font-semibold text-white">{(task.primary_position ?? 0).toFixed(4)}</span>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-xs text-gray-400">{task.secondary_exchange || 'Secondary'}:</span>
            <span className="text-base font-semibold text-white">{(task.secondary_position ?? 0).toFixed(4)}</span>
          </div>
          <div className="flex justify-between items-center border-t border-gray-700 pt-1.5 mt-1">
            <span className="text-xs text-gray-400">Delta:</span>
            <span className={`text-base font-semibold ${delta !== 0 ? "text-red-400" : "text-white"}`}>
              {delta.toFixed(4)}
            </span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
