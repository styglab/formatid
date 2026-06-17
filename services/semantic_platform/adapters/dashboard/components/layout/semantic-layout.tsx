"use client";

import { useState } from "react";
import { SemanticSidebar } from "@/components/layout/semantic-sidebar";
import { SemanticTopbar } from "@/components/layout/semantic-topbar";

export function SemanticLayout({ children }: { children: React.ReactNode }) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      <SemanticSidebar collapsed={sidebarCollapsed} onToggle={() => setSidebarCollapsed((current) => !current)} />
      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <SemanticTopbar />
        <main className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden p-5 md:p-6">{children}</main>
      </div>
    </div>
  );
}
