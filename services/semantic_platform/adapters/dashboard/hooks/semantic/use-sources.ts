"use client";

import { useEffect, useState } from "react";
import { listExecutionSourcesPage } from "@/api/semantic-admin";
import type { ExecutionSource, PaginatedResult } from "@/types/semantic";

export function useSources(params: { query?: string; status?: string; page?: number; pageSize?: number } = {}) {
  const [data, setData] = useState<PaginatedResult<ExecutionSource>>({ items: [], total: 0, page: 1, page_size: params.pageSize || 20 });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      setData(await listExecutionSourcesPage(params));
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load sources.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [params.page, params.pageSize, params.query, params.status]);

  return { data, loading, error, reload };
}
