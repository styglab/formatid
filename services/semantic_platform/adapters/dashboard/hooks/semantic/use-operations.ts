"use client";

import { useEffect, useState } from "react";
import { listExecutionOperationsPage, listOperationFields, listOperationVariantsPage } from "@/api/semantic-admin";
import type { ExecutionOperation, OperationField, OperationVariant, PaginatedResult } from "@/types/semantic";

export function useOperations(params: { query?: string; status?: string; sourceId?: string; assetId?: string; page?: number; pageSize?: number } = {}) {
  const [data, setData] = useState<PaginatedResult<ExecutionOperation>>({ items: [], total: 0, page: 1, page_size: params.pageSize || 20 });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      setData(await listExecutionOperationsPage(params));
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load operations.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [params.assetId, params.page, params.pageSize, params.query, params.sourceId, params.status]);

  return { data, loading, error, reload };
}

export function useOperationFields() {
  const [data, setData] = useState<OperationField[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      setData(await listOperationFields());
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load operation fields.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  return { data, loading, error, reload };
}

export function useOperationVariants(params: { query?: string; status?: string; operationId?: string; page?: number; pageSize?: number } = {}) {
  const [data, setData] = useState<PaginatedResult<OperationVariant>>({ items: [], total: 0, page: 1, page_size: params.pageSize || 20 });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      setData(await listOperationVariantsPage(params));
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load operation variants.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [params.operationId, params.page, params.pageSize, params.query, params.status]);

  return { data, loading, error, reload };
}
