"use client";

import { useEffect, useState } from "react";
import { listMappings, listMappingsPage, mappingExists } from "@/api/semantic-admin";
import type { FieldMapping, PaginatedResult } from "@/types/semantic";

export function useMappings(params: { query?: string; status?: string; operationId?: string; page?: number; pageSize?: number } = {}) {
  const [data, setData] = useState<PaginatedResult<FieldMapping>>({ items: [], total: 0, page: 1, page_size: params.pageSize || 20 });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      setData(await listMappingsPage(params));
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load mappings.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [params.operationId, params.page, params.pageSize, params.query, params.status]);

  return { data, loading, error, reload };
}

export function useMappingExists(params: { operationId?: string; fieldPath?: string; excludeMappingId?: string } = {}) {
  const [exists, setExists] = useState(false);
  const [mappingId, setMappingId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    async function run() {
      const operationId = (params.operationId || "").trim();
      const fieldPath = (params.fieldPath || "").trim();
      if (!operationId || !fieldPath) {
        setExists(false);
        setMappingId(null);
        setError("");
        setLoading(false);
        return;
      }
      setLoading(true);
      setError("");
      try {
        const result = await mappingExists({
          operationId,
          fieldPath,
          excludeMappingId: params.excludeMappingId,
        });
        setExists(result.exists);
        setMappingId(result.mapping_id);
      } catch (requestError) {
        setError(requestError instanceof Error ? requestError.message : "Failed to validate mapping uniqueness.");
        setExists(false);
        setMappingId(null);
      } finally {
        setLoading(false);
      }
    }

    void run();
  }, [params.excludeMappingId, params.fieldPath, params.operationId]);

  return { exists, mappingId, loading, error };
}

export function useAllMappings(params: { query?: string; status?: string; operationId?: string } = {}) {
  const [data, setData] = useState<FieldMapping[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const items = await listMappings();
      setData(
        items.filter((item) => {
          const display = item.draft_snapshot || item;
          const query = (params.query || "").trim().toLowerCase();
          const status = (params.status || "").trim().toLowerCase();
          const operationId = (params.operationId || "").trim();
          if (operationId && display.operation_id !== operationId) return false;
          if (status && status !== "all" && String(display.status || "").toLowerCase() !== status) return false;
          if (!query) return true;
          return [
            display.field_path,
            display.source_id,
            display.operation_id,
            display.semantic_type_id,
            display.notes,
          ]
            .filter(Boolean)
            .some((value) => String(value).toLowerCase().includes(query));
        })
      );
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load mappings.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [params.operationId, params.query, params.status]);

  return { data, loading, error, reload };
}
