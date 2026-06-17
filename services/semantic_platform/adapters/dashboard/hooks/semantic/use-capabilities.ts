"use client";

import { useEffect, useState } from "react";
import { listCapabilitiesPage } from "@/api/semantic-admin";
import type { Capability, PaginatedResult } from "@/types/semantic";

export function useCapabilities(params: { query?: string; status?: string; page?: number; pageSize?: number } = {}) {
  const [data, setData] = useState<PaginatedResult<Capability>>({ items: [], total: 0, page: 1, page_size: params.pageSize || 20 });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      setData(await listCapabilitiesPage(params));
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load capabilities.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [params.page, params.pageSize, params.query, params.status]);

  return { data, loading, error, reload };
}
