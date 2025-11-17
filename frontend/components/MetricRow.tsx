interface MetricRowProps {
  label: string;
  value: string | number;
  valueClassName?: string;
}

export function MetricRow({ label, value, valueClassName }: MetricRowProps) {
  return (
    <div className="flex justify-between items-center">
      <span className="text-xs text-gray-400">{label}</span>
      <span className={`text-base font-semibold ${valueClassName || 'text-white'}`}>
        {value}
      </span>
    </div>
  );
}
