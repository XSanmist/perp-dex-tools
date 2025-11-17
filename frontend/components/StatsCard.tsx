import { Card, CardContent } from "@/components/ui/card";
import { ReactNode } from "react";

interface StatsCardProps {
  title: string;
  value: string | ReactNode;
  subtitle?: string;
  className?: string;
}

export function StatsCard({ title, value, subtitle, className }: StatsCardProps) {
  return (
    <Card className={className || "bg-card border-gray-800"}>
      <CardContent className="p-4">
        <div className="text-sm text-gray-400 mb-2">{title}</div>
        <div className="text-3xl font-bold text-white">{value}</div>
        {subtitle && <div className="text-xs text-gray-500 mt-1.5">{subtitle}</div>}
      </CardContent>
    </Card>
  );
}
