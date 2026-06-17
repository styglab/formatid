"use client";

import Link from "next/link";
import { ArrowRight } from "lucide-react";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { EmptyPanel } from "@/components/semantic/common/state-panel";
import { Button } from "@/components/ui/button";

export default function SchemasPage() {
  return (
    <SectionPlaceholder
      title="Structures"
      description="Extracted structures and fields belong under a specific source or onboarding run. Open the source context first."
      body={
        <div className="space-y-4">
          <EmptyPanel message="Use Source Detail or Run Detail to review assets, structures, fields, and control candidates together." />
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
