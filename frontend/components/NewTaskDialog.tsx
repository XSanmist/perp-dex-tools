"use client";

import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface NewTaskDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreateTask: (taskData: any) => Promise<void>;
}

export function NewTaskDialog({ open, onOpenChange, onCreateTask }: NewTaskDialogProps) {
  const [taskType, setTaskType] = useState<'runbot' | 'hedge' | 'momentum'>('hedge');
  const [formData, setFormData] = useState({
    exchange: 'extended',
    ticker: 'ETH',
    // RunBot fields
    direction: 'buy',
    quantity: '0.1',
    boost: false,
    // Hedge Mode fields
    size: '0.03',
    iterations: '100',
    sleep: '3',
    fillTimeout: '5',
    // Momentum Bot fields (simplified - no manual prices)
    tickOffset: '1',
    takeProfitPct: '0.02',
    maxPositions: '1',
    waitTime: '5',
  });
  const [loading, setLoading] = useState(false);

  const handleSubmit = async () => {
    setLoading(true);
    try {
      const taskData = {
        type: taskType,
        ...formData,
      };
      await onCreateTask(taskData);
      onOpenChange(false);
      // Reset form
      setFormData({
        exchange: 'extended',
        ticker: 'ETH',
        direction: 'buy',
        quantity: '0.1',
        boost: false,
        size: '0.03',
        iterations: '100',
        sleep: '3',
        fillTimeout: '5',
        tickOffset: '1',
        takeProfitPct: '0.02',
        maxPositions: '1',
        waitTime: '5',
      });
      setTaskType('hedge');
    } catch (error) {
      console.error('Failed to create task:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle>Create New Trading Task</DialogTitle>
          <DialogDescription>
            Configure your trading bot parameters. Click create when you're done.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-4">
          {/* Task Type */}
          <div className="grid gap-3">
            <Label htmlFor="taskType">Task Type</Label>
            <Select value={taskType} onValueChange={(value: 'runbot' | 'hedge' | 'momentum') => setTaskType(value)}>
              <SelectTrigger id="taskType">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="hedge">Hedge Mode</SelectItem>
                <SelectItem value="runbot">Run Bot</SelectItem>
                <SelectItem value="momentum">Momentum Bot</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {/* Exchange */}
          <div className="grid gap-3">
            <Label htmlFor="exchange">Exchange</Label>
            <Select
              value={formData.exchange}
              onValueChange={(value) => setFormData({ ...formData, exchange: value })}
            >
              <SelectTrigger id="exchange">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="backpack">Backpack</SelectItem>
                <SelectItem value="extended">Extended</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {/* Ticker */}
          <div className="grid gap-3">
            <Label htmlFor="ticker">Ticker</Label>
            <Input
              id="ticker"
              value={formData.ticker}
              onChange={(e) => setFormData({ ...formData, ticker: e.target.value })}
              placeholder="ETH, BTC, SOL..."
            />
          </div>

          {/* RunBot specific fields */}
          {taskType === 'runbot' && (
            <>
              <div className="grid gap-3">
                <Label htmlFor="direction">Direction</Label>
                <Select
                  value={formData.direction}
                  onValueChange={(value) => setFormData({ ...formData, direction: value })}
                >
                  <SelectTrigger id="direction">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="buy">Buy</SelectItem>
                    <SelectItem value="sell">Sell</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="grid gap-3">
                <Label htmlFor="quantity">Quantity</Label>
                <Input
                  id="quantity"
                  type="number"
                  step="0.001"
                  value={formData.quantity}
                  onChange={(e) => setFormData({ ...formData, quantity: e.target.value })}
                  placeholder="0.1"
                />
              </div>

              <div className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  id="boost"
                  checked={formData.boost}
                  onChange={(e) => setFormData({ ...formData, boost: e.target.checked })}
                  className="h-4 w-4 rounded border-input"
                />
                <Label htmlFor="boost" className="cursor-pointer font-normal">
                  Enable Boost Mode
                </Label>
              </div>
            </>
          )}

          {/* Hedge Mode specific fields */}
          {taskType === 'hedge' && (
            <>
              <div className="grid gap-3">
                <Label htmlFor="size">Size (per iteration)</Label>
                <Input
                  id="size"
                  type="number"
                  step="0.001"
                  value={formData.size}
                  onChange={(e) => setFormData({ ...formData, size: e.target.value })}
                  placeholder="0.03"
                />
              </div>

              <div className="grid gap-3">
                <Label htmlFor="iterations">Iterations</Label>
                <Input
                  id="iterations"
                  type="number"
                  value={formData.iterations}
                  onChange={(e) => setFormData({ ...formData, iterations: e.target.value })}
                  placeholder="100"
                />
              </div>

              <div className="grid gap-3">
                <Label htmlFor="sleep">Sleep (seconds)</Label>
                <Input
                  id="sleep"
                  type="number"
                  value={formData.sleep}
                  onChange={(e) => setFormData({ ...formData, sleep: e.target.value })}
                  placeholder="3"
                />
              </div>

              <div className="grid gap-3">
                <Label htmlFor="fillTimeout">Fill Timeout (seconds)</Label>
                <Input
                  id="fillTimeout"
                  type="number"
                  value={formData.fillTimeout}
                  onChange={(e) => setFormData({ ...formData, fillTimeout: e.target.value })}
                  placeholder="5"
                />
              </div>
            </>
          )}

          {/* Momentum Bot specific fields */}
          {taskType === 'momentum' && (
            <>
              <div className="grid gap-3">
                <Label htmlFor="direction">Direction</Label>
                <Select
                  value={formData.direction}
                  onValueChange={(value) => setFormData({ ...formData, direction: value })}
                >
                  <SelectTrigger id="direction">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="buy">Buy (Long)</SelectItem>
                    <SelectItem value="sell">Sell (Short)</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="grid gap-3">
                <Label htmlFor="quantity">Quantity (数量)</Label>
                <Input
                  id="quantity"
                  type="number"
                  step="0.001"
                  value={formData.quantity}
                  onChange={(e) => setFormData({ ...formData, quantity: e.target.value })}
                  placeholder="0.1"
                />
              </div>

              <div className="grid gap-3">
                <Label htmlFor="tickOffset">Tick Offset (价格偏移，以 tick 为单位)</Label>
                <Input
                  id="tickOffset"
                  type="number"
                  step="1"
                  value={formData.tickOffset}
                  onChange={(e) => setFormData({ ...formData, tickOffset: e.target.value })}
                  placeholder="1"
                />
                <p className="text-xs text-muted-foreground">
                  从当前最佳价格偏移几个 tick（例如 1 表示偏移 1 个 tick size）
                </p>
              </div>

              <div className="grid gap-3">
                <Label htmlFor="takeProfitPct">Take Profit % (止盈)</Label>
                <Input
                  id="takeProfitPct"
                  type="number"
                  step="0.01"
                  value={formData.takeProfitPct}
                  onChange={(e) => setFormData({ ...formData, takeProfitPct: e.target.value })}
                  placeholder="0.02"
                />
              </div>

              <div className="grid gap-3">
                <Label htmlFor="maxPositions">Max Positions (最大持仓数)</Label>
                <Input
                  id="maxPositions"
                  type="number"
                  value={formData.maxPositions}
                  onChange={(e) => setFormData({ ...formData, maxPositions: e.target.value })}
                  placeholder="1"
                />
              </div>

              <div className="grid gap-3">
                <Label htmlFor="waitTime">Wait Time (等待时间，秒)</Label>
                <Input
                  id="waitTime"
                  type="number"
                  value={formData.waitTime}
                  onChange={(e) => setFormData({ ...formData, waitTime: e.target.value })}
                  placeholder="5"
                />
              </div>
            </>
          )}
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={loading}
          >
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            disabled={loading}
          >
            {loading ? 'Creating...' : 'Create Task'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}