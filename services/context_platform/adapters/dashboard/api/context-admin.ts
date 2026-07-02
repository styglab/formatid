import type {
  Capability,
  CanonicalClassSlotUsage,
  CanonicalClassSlot,
  CanonicalClass,
  CanonicalEnum,
  CanonicalRelation,
  CanonicalSlot,
  CanonicalType,
  ContextBinding,
  CanonicalRepresentation,
  Concept,
  ConceptScheme,
  ContextCapabilityOperation,
  ContextCapabilityStep,
  ContextExecution,
  LinkType,
  MeaningScope,
  ObjectType,
  PropertyType,
  RepresentationSchema,
  ContextOnboardingRun,
  ContextOverview,
  ContextPlan,
  ContextProposalBundle,
  ContextSource,
  ContextSourceDocument,
  ContextSourceField,
  ContextSourceOperation,
  ContextSourceParameter,
  ValueDomain,
  ValueDomainValue,
  PaginatedResult,
  Proposal,
  WorkbenchActionResult,
  WorkbenchWorkflow,
} from "@/types/context";

export type {
  CanonicalRepresentation,
  Concept,
  ConceptScheme,
  ContextBinding,
  ContextCapabilityStep,
  ContextCapabilityOperation,
  ContextExecution,
  ContextOnboardingRun,
  ContextOverview,
  ContextPlan,
  ContextProposalBundle,
  ContextSource,
  ContextSourceDocument,
  ContextSourceField,
  ContextSourceOperation,
  ContextSourceParameter,
  LinkType,
  MeaningScope,
  ObjectType,
  PropertyType,
  RepresentationSchema,
  ValueDomain,
  ValueDomainValue,
} from "@/types/context";

const API_BASE = process.env.NEXT_PUBLIC_CONTEXT_PLATFORM_API_URL || "/context-platform-api";

export async function contextAdminRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const isFormData = typeof FormData !== "undefined" && init?.body instanceof FormData;
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      ...(isFormData ? {} : { "Content-Type": "application/json" }),
      ...(init?.headers || {}),
    },
  });

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new Error(detail || `Request failed: ${response.status}`);
  }

  return (await response.json()) as T;
}

export function listOverview() {
  return contextAdminRequest<ContextOverview>("/api/overview");
}

export function getWorkbenchWorkflow(params: { sourceDocumentId?: string; runId?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceDocumentId) search.set("source_document_id", params.sourceDocumentId);
  if (params.runId) search.set("run_id", params.runId);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<WorkbenchWorkflow>(`/api/workbench/workflow${suffix}`);
}

export function runWorkbenchAction(
  action: "validate" | "submit-proposal" | "approve-bundle" | "reject-bundle",
  payload: { source_document_id?: string; run_id?: string; proposal_bundle_id?: string; reviewer?: string; rationale?: string; agent_response?: Record<string, unknown>; manual_llm_response?: Record<string, unknown> } = {}
) {
  return contextAdminRequest<WorkbenchActionResult>(`/api/workbench/${action}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function approveProposalBundle(bundleId: string, payload: { reviewer?: string } = {}) {
  return contextAdminRequest<WorkbenchActionResult>(`/api/proposal-bundles/${bundleId}/approve`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function rejectProposalBundle(bundleId: string, payload: { reviewer?: string; rationale?: string } = {}) {
  return contextAdminRequest<WorkbenchActionResult>(`/api/proposal-bundles/${bundleId}/reject`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function listContextSourcesPage(params: { query?: string; status?: string; page?: number; pageSize?: number }) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  search.set("page", String(params.page || 1));
  search.set("page_size", String(params.pageSize || 20));
  return contextAdminRequest<PaginatedResult<ContextSource>>(`/api/sources/page?${search.toString()}`);
}

export function uploadSourceDocument(formData: FormData) {
  return contextAdminRequest<{
    source: ContextSource;
    source_document: Record<string, unknown>;
    onboarding_run: { id: string; status: string; stage: string };
    submission: { status?: string; deployment?: string; flow_run_id?: string; reason?: string; run_id?: string };
    source_operations: ContextSourceOperation[];
    proposals: Proposal[];
  }>("/api/sources/upload", {
    method: "POST",
    body: formData,
  });
}

export function listSourceDocuments(params: { sourceId?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceId) search.set("source_id", params.sourceId);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextSourceDocument[]>(`/api/source-documents${suffix}`);
}

export function listSourceOperations(params: { sourceId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceId) search.set("source_id", params.sourceId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextSourceOperation[]>(`/api/source-operations${suffix}`);
}

export function listSourceParameters(params: { sourceOperationId?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceOperationId) search.set("source_operation_id", params.sourceOperationId);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextSourceParameter[]>(`/api/source-parameters${suffix}`);
}

export function listSourceFields(params: { sourceOperationId?: string; sourceDocumentId?: string; direction?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceOperationId) search.set("source_operation_id", params.sourceOperationId);
  if (params.sourceDocumentId) search.set("source_document_id", params.sourceDocumentId);
  if (params.direction) search.set("direction", params.direction);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextSourceField[]>(`/api/source-fields${suffix}`);
}

export function listMeaningScopes(params: { status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<MeaningScope[]>(`/api/meaning-scopes${suffix}`);
}

export function listConceptSchemes(params: { status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ConceptScheme[]>(`/api/concept-schemes${suffix}`);
}

export function listConcepts(params: { query?: string; kind?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.kind) search.set("kind", params.kind);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<Concept[]>(`/api/concepts${suffix}`);
}

export function listValueDomains(params: { status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ValueDomain[]>(`/api/value-domains${suffix}`);
}

export function listValueDomainValues(params: { valueDomainId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.valueDomainId) search.set("value_domain_id", params.valueDomainId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ValueDomainValue[]>(`/api/value-domain-values${suffix}`);
}

export function listObjectTypes(params: { query?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ObjectType[]>(`/api/object-types${suffix}`);
}

export function listPropertyTypes(params: { query?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<PropertyType[]>(`/api/property-types${suffix}`);
}

export function listLinkTypes(params: { status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<LinkType[]>(`/api/link-types${suffix}`);
}

export function listCanonicalRepresentations(params: { conceptId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.conceptId) search.set("concept_id", params.conceptId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<CanonicalRepresentation[]>(`/api/canonical-representations${suffix}`);
}

export function listRepresentationSchemas(params: { representationId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.representationId) search.set("representation_id", params.representationId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<RepresentationSchema[]>(`/api/representation-schemas${suffix}`);
}

export function listCanonicalTypes(params: { query?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<CanonicalType[]>(`/api/canonical-types${suffix}`);
}

export function listCanonicalEnums(params: { query?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<CanonicalEnum[]>(`/api/canonical-enums${suffix}`);
}

export function listCanonicalSlots(params: { query?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<CanonicalSlot[]>(`/api/canonical-slots${suffix}`);
}

export function listCanonicalClasses(params: { query?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<CanonicalClass[]>(`/api/canonical-classes${suffix}`);
}

export function listCanonicalClassSlotUsages(params: { classId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.classId) search.set("class_id", params.classId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<CanonicalClassSlotUsage[]>(`/api/canonical-class-slot-usages${suffix}`);
}

export function listCanonicalClassSlots(params: { classId?: string; slotId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.classId) search.set("class_id", params.classId);
  if (params.slotId) search.set("slot_id", params.slotId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<CanonicalClassSlot[]>(`/api/canonical-class-slots${suffix}`);
}

export function listCanonicalRelations(params: { status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<CanonicalRelation[]>(`/api/canonical-relations${suffix}`);
}

export function getCanonicalModelLinkml(params: { namespace?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.namespace) search.set("namespace", params.namespace);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<Record<string, unknown>>(`/api/canonical-model/linkml${suffix}`);
}

export function listBindings(params: { sourceOperationId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceOperationId) search.set("source_operation_id", params.sourceOperationId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextBinding[]>(`/api/bindings${suffix}`);
}

export function listFieldBindings(params: { sourceOperationId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceOperationId) search.set("source_operation_id", params.sourceOperationId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextBinding[]>(`/api/field-bindings${suffix}`);
}

export function listContextBindings(params: { sourceOperationId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceOperationId) search.set("source_operation_id", params.sourceOperationId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextBinding[]>(`/api/context-bindings${suffix}`);
}

export function listParameterBindings(params: { sourceOperationId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceOperationId) search.set("source_operation_id", params.sourceOperationId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextBinding[]>(`/api/parameter-bindings${suffix}`);
}

export function listCapabilities(params: { query?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.query) search.set("query", params.query);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<Capability[]>(`/api/capabilities${suffix}`);
}

export function listCapabilityOperations(params: { capabilityId?: string } = {}) {
  const search = new URLSearchParams();
  if (params.capabilityId) search.set("capability_id", params.capabilityId);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextCapabilityOperation[]>(`/api/capability-operations${suffix}`);
}

export function listCapabilitySteps(params: { capabilityId?: string } = {}) {
  const search = new URLSearchParams();
  if (params.capabilityId) search.set("capability_id", params.capabilityId);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextCapabilityStep[]>(`/api/capability-steps${suffix}`);
}

export function listPlans(params: { status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextPlan[]>(`/api/plans${suffix}`);
}

export function listExecutions(params: { status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextExecution[]>(`/api/executions${suffix}`);
}

export function listOnboardingRuns(params: { sourceDocumentId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceDocumentId) search.set("source_document_id", params.sourceDocumentId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextOnboardingRun[]>(`/api/onboarding-runs${suffix}`);
}

export function listProposalBundles(params: { sourceId?: string; status?: string } = {}) {
  const search = new URLSearchParams();
  if (params.sourceId) search.set("source_id", params.sourceId);
  if (params.status && params.status !== "all") search.set("status", params.status);
  const suffix = search.size ? `?${search.toString()}` : "";
  return contextAdminRequest<ContextProposalBundle[]>(`/api/proposal-bundles${suffix}`);
}

export function listProposalBundleItems(bundleId: string) {
  return contextAdminRequest<Proposal[]>(`/api/proposal-bundles/${bundleId}/items`);
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
