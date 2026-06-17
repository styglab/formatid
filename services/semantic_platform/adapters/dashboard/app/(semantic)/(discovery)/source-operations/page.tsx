"use client";

import Link from "next/link";
import { ArrowRight } from "lucide-react";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { EmptyPanel } from "@/components/semantic/common/state-panel";
import { Button } from "@/components/ui/button";

export default function OperationsPage() {
  return (
    <SectionPlaceholder
      title="Operations / Access Paths"
      description="Operations are no longer the top-level onboarding axis. Open a source or run first, then inspect operations in that source context."
      body={
        <div className="space-y-4">
          <EmptyPanel message="Choose a source to inspect its assets, structures, and optional operations under one authoring context." />
          <div className="flex flex-wrap gap-2">
            <Button type="button" asChild>
              <Link href="/sources">
                <ArrowRight className="h-4 w-4" />
                Go to Sources
              </Link>
            </Button>
            <Button type="button" variant="outline" asChild>
              <Link href="/onboarding-runs">
                <ArrowRight className="h-4 w-4" />
                Go to Runs
              </Link>
            </Button>
          </div>
        </div>
      }
    />
  );
}
