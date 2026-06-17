import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Data Score Dashboard",
  description: "Queued evaluation runs, score summaries, and issue review workspace"
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
