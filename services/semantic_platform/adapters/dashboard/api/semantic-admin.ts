import type {
  Capability,
  CapabilityBinding,
  CanonicalAttribute,
  CanonicalEntity,
  CanonicalRelation,
  ExecutionAsset,
  ExecutionOperation,
  ExecutionSource,
  FieldMapping,
  OperationField,
  OperationVariant,
  OnboardingRun,
  OnboardingRunDetail,
  PaginatedResult,
  ProposalBundle,
  SemanticRelationship,
  SemanticType,
  WorkQueueTask
} from "@/types/semantic";
import type { Overview, Proposal } from "@/types/governance";

const API_BASE = process.env.NEXT_PUBLIC_SEMANTIC_PLATFORM_API_URL || "/semantic-platform";

export async function semanticAdminRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const isFormData = typeof FormData !== "undefined" && init?.body instanceof FormData;
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      ...(isFormData ? {} : { "Content-Type": "application/json" }),
      ...(init?.headers || {}),
    }
  });

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new Error(detail || `Request failed: ${response.status}`);
  }

  return (await response.json()) as T;
}

export function listOverview() {
  return semanticAdminRequest<Overview>("/api/overview");
}

export function listSemanticTypes() {
  return semanticAdminRequest<SemanticType[]>("/api/semantic-types");
}

export function createSemanticType(payload: Record<string, unknown>) {
  return semanticAdminRequest<{ semantic_type: SemanticType; proposal: { id?: string } }>("/api/semantic-types", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function updateSemanticType(semanticTypeId: string, payload: Record<string, unknown>) {
  return semanticAdminRequest(`/api/semantic-types/${semanticTypeId}`, {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

export function deleteSemanticType(semanticTypeId: string) {
  return semanticAdminRequest(`/api/semantic-types/${semanticTypeId}`, {
    method: "DELETE"
  });
}

export function listExecutionSources() {
  return semanticAdminRequest<ExecutionSource[]>("/api/execution-sources");
}

export function getExecutionSource(sourceId: string) {
  return semanticAdminRequest<ExecutionSource>(`/api/execution-sources/${sourceId}`);
}

export function startWorkspace(sourceId: string, reviewer = "dashboard") {
  return semanticAdminRequest<{ run: OnboardingRun; trigger: Record<string, unknown> }>(`/api/execution-sources/${sourceId}/start-workspace`, {
    method: "POST",
    body: JSON.stringify({ reviewer })
  });
}

export function listExecutionSourcesPage(params: { query?: string; status?: string; page?: number; pageSize?: number }) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  search.set("page", String(params.page || 1));
  search.set("page_size", String(params.pageSize || 20));
  return semanticAdminRequest<PaginatedResult<ExecutionSource>>(`/api/execution-sources/page?${search.toString()}`);
}

export function createExecutionSource(payload: Record<string, unknown>) {
  return semanticAdminRequest("/api/execution-sources", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function updateExecutionSource(sourceId: string, payload: Record<string, unknown>) {
  return semanticAdminRequest(`/api/execution-sources/${sourceId}`, {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

export function deleteExecutionSource(sourceId: string) {
  return semanticAdminRequest(`/api/execution-sources/${sourceId}`, {
    method: "DELETE"
  });
}

export function uploadExecutionSource(formData: FormData) {
  return semanticAdminRequest("/api/execution-sources/upload", {
    method: "POST",
    body: formData
  });
}

export function listExecutionAssets(params: { query?: string; status?: string; sourceId?: string } = {}) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  if (params.sourceId) search.set("source_id", params.sourceId);
  const suffix = search.size ? `?${search.toString()}` : "";
  return semanticAdminRequest<ExecutionAsset[]>(`/api/execution-assets${suffix}`);
}

export function listExecutionOperations() {
  return semanticAdminRequest<ExecutionOperation[]>("/api/execution-operations");
}

export function listExecutionOperationsPage(params: { query?: string; status?: string; sourceId?: string; assetId?: string; page?: number; pageSize?: number }) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  if (params.sourceId) search.set("source_id", params.sourceId);
  if (params.assetId) search.set("asset_id", params.assetId);
  search.set("page", String(params.page || 1));
  search.set("page_size", String(params.pageSize || 20));
  return semanticAdminRequest<PaginatedResult<ExecutionOperation>>(`/api/execution-operations/page?${search.toString()}`);
}

export function listOperationFields(params: { operationId?: string; variantId?: string } = {}) {
  const search = new URLSearchParams();
  if (params.operationId) search.set("operation_id", params.operationId);
  if (params.variantId) search.set("variant_id", params.variantId);
  const suffix = search.size ? `?${search.toString()}` : "";
  return semanticAdminRequest<OperationField[]>(`/api/operation-fields${suffix}`);
}

export function listOnboardingRuns() {
  return semanticAdminRequest<OnboardingRun[]>("/api/onboarding-runs");
}

export function getOnboardingRun(runId: string) {
  return semanticAdminRequest<OnboardingRunDetail>(`/api/onboarding-runs/${runId}`);
}

export function resumeOnboardingRun(runId: string, reviewer = "dashboard") {
  return semanticAdminRequest<{ run: OnboardingRun; trigger: Record<string, unknown> }>(`/api/onboarding-runs/${runId}/resume`, {
    method: "POST",
    body: JSON.stringify({ reviewer })
  });
}

export function pauseOnboardingRun(runId: string, reviewer = "dashboard") {
  return semanticAdminRequest<{ run: OnboardingRun }>(`/api/onboarding-runs/${runId}/pause`, {
    method: "POST",
    body: JSON.stringify({ reviewer })
  });
}

export function cancelOnboardingRun(runId: string, reviewer = "dashboard") {
  return semanticAdminRequest<{ run: OnboardingRun }>(`/api/onboarding-runs/${runId}/cancel`, {
    method: "POST",
    body: JSON.stringify({ reviewer })
  });
}

export function generateOnboardingTaskDraft(taskId: string, reviewer = "dashboard") {
  return semanticAdminRequest<{ task: WorkQueueTask }>(`/api/onboarding-tasks/${taskId}/generate-draft`, {
    method: "POST",
    body: JSON.stringify({ reviewer })
  });
}

export function completeOnboardingTask(taskId: string, reviewer = "dashboard") {
  return semanticAdminRequest<{ task: WorkQueueTask; run: OnboardingRun | null }>(`/api/onboarding-tasks/${taskId}/complete`, {
    method: "POST",
    body: JSON.stringify({ reviewer })
  });
}

export function listProposalBundles() {
  return semanticAdminRequest<ProposalBundle[]>("/api/proposal-bundles");
}

export function listProposals(params: { status?: string; entityType?: string } = {}) {
  const search = new URLSearchParams();
  if (params.status) search.set("status", params.status);
  if (params.entityType) search.set("entity_type", params.entityType);
  const suffix = search.size ? `?${search.toString()}` : "";
  return semanticAdminRequest<Proposal[]>(`/api/proposals${suffix}`);
}

export function listCapabilityBindings() {
  return semanticAdminRequest<CapabilityBinding[]>("/api/capability-bindings");
}

export function listOperationVariants() {
  return semanticAdminRequest<OperationVariant[]>("/api/operation-variants");
}

export function listOperationVariantsPage(params: { query?: string; status?: string; operationId?: string; page?: number; pageSize?: number }) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  if (params.operationId) search.set("operation_id", params.operationId);
  search.set("page", String(params.page || 1));
  search.set("page_size", String(params.pageSize || 20));
  return semanticAdminRequest<PaginatedResult<OperationVariant>>(`/api/operation-variants/page?${search.toString()}`);
}

export function createOperationVariant(payload: Record<string, unknown>) {
  return semanticAdminRequest("/api/operation-variants", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function updateOperationVariant(variantId: string, payload: Record<string, unknown>) {
  return semanticAdminRequest(`/api/operation-variants/${variantId}`, {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

export function deleteOperationVariant(variantId: string) {
  return semanticAdminRequest(`/api/operation-variants/${variantId}`, {
    method: "DELETE"
  });
}

export function listCapabilities() {
  return semanticAdminRequest<Capability[]>("/api/capabilities");
}

export function listCapabilitiesPage(params: { query?: string; status?: string; page?: number; pageSize?: number }) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  search.set("page", String(params.page || 1));
  search.set("page_size", String(params.pageSize || 20));
  return semanticAdminRequest<PaginatedResult<Capability>>(`/api/capabilities/page?${search.toString()}`);
}

export function createCapability(payload: Record<string, unknown>) {
  return semanticAdminRequest("/api/capabilities", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function updateCapability(capabilityId: string, payload: Record<string, unknown>) {
  return semanticAdminRequest(`/api/capabilities/${capabilityId}`, {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

export function deleteCapability(capabilityId: string) {
  return semanticAdminRequest(`/api/capabilities/${capabilityId}`, {
    method: "DELETE"
  });
}

export function listMappings() {
  return semanticAdminRequest<FieldMapping[]>("/api/mappings");
}

export function listMappingsPage(params: { query?: string; status?: string; operationId?: string; page?: number; pageSize?: number }) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  if (params.operationId) search.set("operation_id", params.operationId);
  search.set("page", String(params.page || 1));
  search.set("page_size", String(params.pageSize || 20));
  return semanticAdminRequest<PaginatedResult<FieldMapping>>(`/api/mappings/page?${search.toString()}`);
}

export function mappingExists(params: { operationId: string; fieldPath: string; excludeMappingId?: string }) {
  const search = new URLSearchParams();
  search.set("operation_id", params.operationId);
  search.set("field_path", params.fieldPath);
  if (params.excludeMappingId) search.set("exclude_mapping_id", params.excludeMappingId);
  return semanticAdminRequest<{ exists: boolean; mapping_id: string | null }>(`/api/mappings/exists?${search.toString()}`);
}

export function createMapping(payload: Record<string, unknown>) {
  return semanticAdminRequest<{ field_mapping: FieldMapping; proposal: Proposal }>("/api/mappings", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function updateMapping(mappingId: string, payload: Record<string, unknown>) {
  return semanticAdminRequest(`/api/mappings/${mappingId}`, {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

export type TransformSuggestion = {
  mode: string;
  transform_spec: Record<string, unknown>;
  mapping_type: string;
  mapping_kind: string;
  enum_mapping?: Record<string, unknown>;
  confidence: number;
  rationale: string;
  samples: string[];
  preview: Array<{ input: string; output: unknown; ok: boolean; error?: string }>;
};

export function suggestMappingTransform(mappingId: string) {
  return semanticAdminRequest<TransformSuggestion>(`/api/mappings/${mappingId}/transform-suggestion`, {
    method: "POST",
    body: JSON.stringify({})
  });
}

export type SemanticTypeSuggestion = {
  semantic_type_id: string;
  name: string;
  datatype: string;
  description: string;
  confidence: number;
  rationale: string;
};

export type MappingSuggestion = {
  mode: string;
  field_id: string;
  semantic_type_suggestions: SemanticTypeSuggestion[];
  transform_suggestion: TransformSuggestion | null;
};

export function suggestOperationFieldMapping(fieldId: string, payload: { semantic_type_id?: string | null } = {}) {
  return semanticAdminRequest<MappingSuggestion>(`/api/operation-fields/${fieldId}/mapping-suggestion`, {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function deleteMapping(mappingId: string) {
  return semanticAdminRequest(`/api/mappings/${mappingId}`, {
    method: "DELETE"
  });
}

export function listSemanticRelationships() {
  return semanticAdminRequest<SemanticRelationship[]>("/api/semantic-relationships");
}

export function listCanonicalEntities() {
  return semanticAdminRequest<CanonicalEntity[]>("/api/canonical-entities");
}

export function createCanonicalEntity(payload: Record<string, unknown>) {
  return semanticAdminRequest<{ canonical_entity: CanonicalEntity; proposal: { id?: string } }>("/api/canonical-entities", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function updateCanonicalEntity(entityId: string, payload: Record<string, unknown>) {
  return semanticAdminRequest(`/api/canonical-entities/${entityId}`, {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

export function deleteCanonicalEntity(entityId: string) {
  return semanticAdminRequest(`/api/canonical-entities/${entityId}`, {
    method: "DELETE"
  });
}

export function listCanonicalAttributes() {
  return semanticAdminRequest<CanonicalAttribute[]>("/api/canonical-attributes");
}

export function createCanonicalAttribute(payload: Record<string, unknown>) {
  return semanticAdminRequest<{ canonical_attribute: CanonicalAttribute; proposal: { id?: string } }>("/api/canonical-attributes", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function updateCanonicalAttribute(attributeId: string, payload: Record<string, unknown>) {
  return semanticAdminRequest(`/api/canonical-attributes/${attributeId}`, {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

export function deleteCanonicalAttribute(attributeId: string) {
  return semanticAdminRequest(`/api/canonical-attributes/${attributeId}`, {
    method: "DELETE"
  });
}

export function listCanonicalRelations() {
  return semanticAdminRequest<CanonicalRelation[]>("/api/canonical-relations");
}

export function createCanonicalRelation(payload: Record<string, unknown>) {
  return semanticAdminRequest("/api/canonical-relations", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function updateCanonicalRelation(relationId: string, payload: Record<string, unknown>) {
  return semanticAdminRequest(`/api/canonical-relations/${relationId}`, {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

export function deleteCanonicalRelation(relationId: string) {
  return semanticAdminRequest(`/api/canonical-relations/${relationId}`, {
    method: "DELETE"
  });
}

export function listPendingProposals() {
  return semanticAdminRequest<Proposal[]>("/api/proposals?status=pending_review");
}

export function listPendingProposalsPage(params: { query?: string; entityType?: string; ids?: string[]; page?: number; pageSize?: number }) {
  const search = new URLSearchParams();
  search.set("status", "pending_review");
  if (params.query) search.set("query", params.query);
  if (params.entityType) search.set("entity_type", params.entityType);
  if (params.ids?.length) search.set("ids", params.ids.join(","));
  search.set("page", String(params.page || 1));
  search.set("page_size", String(params.pageSize || 20));
  return semanticAdminRequest<PaginatedResult<Proposal>>(`/api/proposals/page?${search.toString()}`);
}

export function reviewProposal(proposalId: string, decision: "approve" | "reject", reviewer = "dashboard") {
  return semanticAdminRequest<Proposal>(`/api/proposals/${proposalId}/${decision}`, {
    method: "POST",
    body: JSON.stringify({ reviewer })
  });
}

async function readErrorDetail(response: Response) {
  try {
    const payload = (await response.json()) as { detail?: unknown };
    const detail = payload.detail;
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) {
      return detail
        .map((item) => {
          if (typeof item === "string") return item;
          if (item && typeof item === "object") {
            const record = item as Record<string, unknown>;
            const path = Array.isArray(record.loc) ? record.loc.join(".") : "";
            const message = typeof record.msg === "string" ? record.msg : JSON.stringify(record);
            return path ? `${path}: ${message}` : message;
          }
          return String(item);
        })
        .join("\n");
    }
    if (detail && typeof detail === "object") {
      return JSON.stringify(detail, null, 2);
    }
    return detail == null ? "" : String(detail);
  } catch {
    return "";
  }
}
