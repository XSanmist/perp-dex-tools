"use client";

import { useState } from "react";
import { Info } from "lucide-react";

export default function Portfolio() {
  const [timeRange, setTimeRange] = useState("1W");
  const [selectedTab, setSelectedTab] = useState("Positions");

  const tabs = [
    "Positions",
    "Open Orders",
    "Order History",
    "Trade History",
    "Funding History",
    "Liquidations",
    "Public Pools",
    "Deposits",
    "Withdrawals",
    "Transfers"
  ];

  const timeRanges = ["1W", "24H"];

  return (
    <div className="min-h-screen bg-[#0a0a0a] text-white">
      {/* Header */}
      <div className="border-b border-gray-800">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <h1 className="text-2xl font-semibold">Portfolio</h1>
            <div className="flex gap-4">
              <button className="px-4 py-2 border border-gray-700 rounded-lg hover:bg-gray-900 transition">
                Invite
              </button>
              <button className="px-4 py-2 bg-blue-600 rounded-lg hover:bg-blue-700 transition">
                Deposit
              </button>
              <select className="px-4 py-2 bg-gray-900 border border-gray-700 rounded-lg">
                <option>Account Type</option>
              </select>
            </div>
          </div>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="container mx-auto px-4 py-8">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <StatCard
            title="Total Equity"
            value="$201.51"
            info="Total value of your account"
          />
          <StatCard
            title="Trading Equity"
            value="$201.51"
            info="Available for trading"
          />
          <StatCard
            title="Available Balance"
            value="$201.51"
            info="Available for withdrawal"
          />
          <StatCard
            title="Public Pools Equity"
            value="-"
            info="Value in public pools"
          />
        </div>

        {/* Stats and Chart Section */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
          {/* Left Stats */}
          <div className="space-y-4">
            <StatItem label="PnL" value="$1.51" />
            <StatItem label="Volume" value="$97,140.56" />
            <StatItem label="Return Percentage" value="0.75%" />
            <StatItem label="Average Hourly PnL" value="$0.04" />
            <StatItem label="PnL Volatility" value="$0.28" />
            <StatItem label="Sharpe Ratio" value="2.87" />
            <StatItem label="Maximum Drawdown" value="$0.56" />
          </div>

          {/* Chart */}
          <div className="lg:col-span-2 bg-gray-950 border border-gray-800 rounded-lg p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-4">
                <label className="text-sm text-gray-400">Total Equity</label>
                <select className="bg-transparent border-0 text-white">
                  <option>1W</option>
                  <option>24H</option>
                </select>
              </div>
              <div className="flex gap-2">
                <button className="px-3 py-1 text-sm border-b-2 border-blue-500 text-blue-500">
                  Cumulative PnL
                </button>
                <button className="px-3 py-1 text-sm text-gray-400 hover:text-white">
                  PnL
                </button>
                <button className="px-3 py-1 text-sm text-gray-400 hover:text-white">
                  Return Percentage
                </button>
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm text-gray-400">Total Equity</label>
                <select className="bg-transparent border-0 text-white">
                  <option>24H</option>
                </select>
              </div>
            </div>

            {/* Placeholder Chart */}
            <div className="h-64 flex items-center justify-center border border-gray-800 rounded">
              <svg className="w-full h-full p-4" viewBox="0 0 400 200">
                <polyline
                  fill="none"
                  stroke="#10b981"
                  strokeWidth="2"
                  points="0,150 50,150 100,50 150,100 200,30 250,100 300,150 350,150 400,120"
                />
              </svg>
            </div>

            {/* Chart Time Labels */}
            <div className="flex justify-between mt-2 text-xs text-gray-500">
              <span>00</span>
              <span>14:00</span>
              <span>16:00</span>
              <span>18:00</span>
              <span>20:00</span>
              <span>22:00</span>
              <span>00:00</span>
              <span>02:00</span>
              <span>04:00</span>
              <span>06:00</span>
              <span>08:00</span>
              <span>10:00</span>
            </div>
          </div>
        </div>

        {/* Tabs Section */}
        <div className="border-t border-gray-800 pt-6">
          <div className="flex gap-6 mb-6 overflow-x-auto">
            {tabs.map((tab) => (
              <button
                key={tab}
                onClick={() => setSelectedTab(tab)}
                className={`whitespace-nowrap pb-2 transition ${
                  selectedTab === tab
                    ? "text-blue-500 border-b-2 border-blue-500"
                    : "text-gray-400 hover:text-white"
                }`}
              >
                {tab}
              </button>
            ))}
          </div>

          {/* Tab Content */}
          <div className="bg-gray-950 border border-gray-800 rounded-lg p-8">
            <div className="text-center text-gray-500">
              {selectedTab === "Positions" && "No Open Positions"}
              {selectedTab === "Open Orders" && "No Open Orders"}
              {selectedTab === "Order History" && "No Order History"}
              {selectedTab === "Trade History" && "No Trade History"}
              {selectedTab === "Funding History" && "No Funding History"}
              {selectedTab === "Liquidations" && "No Liquidations"}
              {selectedTab === "Public Pools" && "No Public Pools"}
              {selectedTab === "Deposits" && "No Deposits"}
              {selectedTab === "Withdrawals" && "No Withdrawals"}
              {selectedTab === "Transfers" && "No Transfers"}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function StatCard({
  title,
  value,
  info
}: {
  title: string;
  value: string;
  info: string;
}) {
  return (
    <div className="bg-gray-950 border border-gray-800 rounded-lg p-4">
      <div className="flex items-center gap-2 mb-2">
        <h3 className="text-sm text-gray-400">{title}</h3>
        <div className="group relative">
          <Info className="w-3 h-3 text-gray-500 cursor-help" />
          <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity">
            {info}
          </div>
        </div>
      </div>
      <p className="text-2xl font-semibold">{value}</p>
    </div>
  );
}

function StatItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between py-2 border-b border-gray-800">
      <div className="flex items-center gap-2">
        <span className="text-sm text-gray-400">{label}</span>
        <div className="group relative">
          <Info className="w-3 h-3 text-gray-500 cursor-help" />
          <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-2 py-1 bg-gray-900 border border-gray-700 rounded text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity z-10">
            {label} information
          </div>
        </div>
      </div>
      <span className="font-medium">{value}</span>
    </div>
  );
}