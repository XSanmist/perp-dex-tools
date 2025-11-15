import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Perp DEX Tools",
  description: "Perpetual DEX Trading Tools Dashboard",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased bg-[#0a0a0a] text-white">
        {children}
      </body>
    </html>
  );
}
