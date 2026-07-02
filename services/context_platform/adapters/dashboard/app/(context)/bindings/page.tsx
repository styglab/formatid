"use client";

import { useEffect, useMemo, useState } from "react";
import { GitBranch, RefreshCw, Search } from "lucide-react";
import {
  listBindings,
  listCanonicalRepresentations,
  listRepresentationSchemas,
  listSourceFields,
  listSourceParameters,
} from "@/api/context-admin";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { CanonicalRepresentation, ContextBinding, ContextSourceField, ContextSourceParameter, RepresentationSchema } from "@/types/context";

export default function BindingsPage() {
  const [bindings, setBindings] = useState<ContextBinding[]>([]);
  const [parameters, setParameters] = useState<ContextSourceParameter[]>([]);
  const [fields, setFields] = useState<ContextSourceField[]>([]);
  const [representations, setRepresentations] = useState<CanonicalRepresentation[]>([]);
  const [schemas, setSchemas] = useState<RepresentationSchema[]>([]);
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const [loadedBindings, loadedParameters, loadedFields, loadedRepresentations, loadedSchemas] = await Promise.all([
        listBindings(),
        listSourceParameters(),
        listSourceFields(),
        listCanonicalRepresentations(),
        listRepresentationSchemas(),
      ]);
      setBindings(loadedBindings);
      setParameters(loadedParameters);
      setFields(loadedFields);
      setRepresentations(loadedRepresentations);
      setSchemas(loadedSchemas);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load bindings.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  const parameterById = useMemo(() => new Map(parameters.map((item) => [item.id, item])), [parameters]);
  const fieldById = useMemo(() => new Map(fields.map((item) => [item.id, item])), [fields]);
  const representationById = useMemo(() => new Map(representations.map((item) => [item.id, item])), [representations]);
  const schemaById = useMemo(() => new Map(schemas.map((item) => [item.id, item])), [schemas]);
  const normalizedQuery = query.trim().toLowerCase();
  const filteredBindings = bindings.filter((binding) => {
    const source = binding.source_parameter_id ? parameterById.get(binding.source_parameter_id) : binding.source_field_id ? fieldById.get(binding.source_field_id) : null;
    const target = binding.representation_id ? representationById.get(binding.representation_id) : null;
    const schema = binding.representation_schema_id ? schemaById.get(binding.representation_schema_id) : null;
    return `${binding.id} ${binding.direction} ${binding.binding_type || ""} ${sourceLabel(source)} ${target?.stable_key || ""} ${schema?.stable_key || ""} ${binding.required_concept_id || ""} ${binding.context_key || ""} ${binding.status || ""}`.toLowerCase().includes(normalizedQuery);
  });
  const approvedCount = bindings.filter((item) => item.status === "approved" || item.status === "published").length;

  return (
    <div className="mx-auto flex max-w-[1280px] flex-col gap-5">
      <section className="flex flex-col gap-4 border-b border-border pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">
            <GitBranch className="h-3.5 w-3.5" />
            Catalog
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Resolution</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Source fields and parameters resolved to canonical representations, context slots, and required concepts.
          </p>
        </div>
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </section>

      {error ? <Notice message={error} /> : null}

      <section className="grid gap-3 md:grid-cols-3">
        <Metric label="Bindings" value={bindings.length} />
        <Metric label="Approved" value={approvedCount} />
        <Metric label="Representations" value={representations.length} />
      </section>

      <Card>
        <CardHeader>
          <div>
            <CardTitle>Resolution Graph</CardTitle>
            <CardDescription>Resolution stays source-scoped; identical raw names are not global meanings.</CardDescription>
          </div>
          <GitBranch className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent className="space-y-3">
          <label className="flex h-9 max-w-md items-center gap-2 rounded-lg border border-border bg-background px-3">
            <Search className="h-4 w-4 text-muted-foreground" />
            <input className="min-w-0 flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search bindings" />
          </label>
          <div className="overflow-hidden rounded-lg border border-border">
            <div className="grid grid-cols-[minmax(180px,1fr)_110px_minmax(180px,1fr)_130px_110px] border-b border-border bg-muted/35 px-3 py-2 text-xs font-medium text-muted-foreground">
              <div>Source</div>
              <div>Direction</div>
              <div>Representation</div>
              <div>Type</div>
              <div>Status</div>
            </div>
            <div className="max-h-[42rem] overflow-auto">
              {filteredBindings.length ? (
                filteredBindings.map((binding) => {
                  const source = binding.source_parameter_id ? parameterById.get(binding.source_parameter_id) : binding.source_field_id ? fieldById.get(binding.source_field_id) : null;
                  const target = binding.representation_id ? representationById.get(binding.representation_id) : null;
                  const schema = binding.representation_schema_id ? schemaById.get(binding.representation_schema_id) : null;
                  return (
                    <div key={binding.id} className="grid grid-cols-[minmax(180px,1fr)_110px_minmax(180px,1fr)_130px_110px] gap-3 border-b border-border px-3 py-2.5 text-sm last:border-b-0">
                      <div className="min-w-0">
                        <div className="truncate font-medium text-foreground">{sourceLabel(source) || binding.id}</div>
                        <div className="mt-0.5 truncate text-xs text-muted-foreground">{binding.id}</div>
                      </div>
                      <div className="text-muted-foreground">{binding.direction}</div>
                      <div className="min-w-0">
                        <div className="truncate">{target?.stable_key || binding.representation_id || binding.required_concept_id || ""}</div>
                        <div className="mt-0.5 truncate text-xs text-muted-foreground">{schema?.stable_key || binding.context_key || ""}</div>
                      </div>
                      <div className="text-muted-foreground">{binding.binding_type || "field"}</div>
                      <StatusBadge status={binding.status} />
                    </div>
                  );
                })
              ) : (
                <EmptyState message={loading ? "Loading bindings..." : "No bindings found."} />
              )}
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return <div className="rounded-lg border border-border bg-card px-4 py-3"><div className="text-xs text-muted-foreground">{label}</div><div className="mt-2 text-2xl font-semibold text-foreground">{value}</div></div>;
}

function sourceLabel(source?: ContextSourceParameter | ContextSourceField | null) {
  if (!source) return "";
  if ("field_path" in source) return source.raw_name || source.field_path;
  return source.raw_name || source.name;
}

function StatusBadge({ status }: { status?: string | null }) {
  const value = status || "draft";
  const variant = value === "approved" || value === "published" ? "success" : value === "failed" ? "danger" : value === "proposed" ? "info" : "warning";
  return <Badge variant={variant}>{value}</Badge>;
}

function EmptyState({ message }: { message: string }) {
  return <div className="px-4 py-8 text-center text-sm text-muted-foreground">{message}</div>;
}

function Notice({ message }: { message: string }) {
  return <div className="rounded-lg border border-red-500/25 bg-red-500/10 px-4 py-3 text-sm text-red-700 dark:text-red-300">{message}</div>;
}
