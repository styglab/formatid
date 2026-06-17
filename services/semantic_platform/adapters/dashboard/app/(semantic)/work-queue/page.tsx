"use client";

import Link from "next/link";
import { ArrowRight } from "lucide-react";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { EmptyPanel } from "@/components/semantic/common/state-panel";
import { Button } from "@/components/ui/button";

export default function WorkQueuePage() {
  return (
    <SectionPlaceholder
      title="Review Tasks"
      description="Work queue is now a run-scoped review view. Open the latest onboarding run for a source to inspect and complete tasks."
      body={
        <div className="space-y-4">
          <EmptyPanel message="Review tasks are most useful inside Run Detail, where evidence, structures, operations, and proposals stay in the same context." />
          <div className="flex flex-wrap gap-2">
            <Button type="button" asChild>
              <Link href="/onboarding-runs">
                <ArrowRight className="h-4 w-4" />
                Go to Runs
              </Link>
            </Button>
            <Button type="button" variant="outline" asChild>
              <Link href="/sources">
                <ArrowRight className="h-4 w-4" />
                Go to Sources
              </Link>
            </Button>
          </div>
        </div>
      }
    />
  );
}
