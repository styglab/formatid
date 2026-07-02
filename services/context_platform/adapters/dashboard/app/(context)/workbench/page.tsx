"use client";

import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import {
  ArrowRight,
  CheckCircle2,
  ClipboardCheck,
  FileSearch,
  GitBranch,
  Layers3,
  Lock,
  PackageCheck,
  RefreshCw,
  Route,
  Send,
  Upload,
  XCircle,
} from "lucide-react";
import {
  getWorkbenchWorkflow,
  listBindings,
  listCapabilities,
  listCapabilityOperations,
  listCanonicalClassSlotUsages,
  listCanonicalClasses,
  listCanonicalEnums,
  listCanonicalSlots,
  listCanonicalTypes,
  listOnboardingRuns,
  listOverview,
  listProposalBundles,
  listSourceDocuments,
  listSourceFields,
  listSourceOperations,
  listSourceParameters,
  runWorkbenchAction,
  uploadSourceDocument,
  type ContextBinding,
  type ContextCapabilityOperation,
  type ContextOnboardingRun,
  type ContextOverview,
  type ContextProposalBundle,
  type ContextSourceDocument,
  type ContextSourceField,
  type ContextSourceOperation,
  type ContextSourceParameter,
} from "@/api/context-admin";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type {
  Capability,
  CanonicalClassSlotUsage,
  CanonicalClass,
  CanonicalEnum,
  CanonicalSlot,
  CanonicalType,
  WorkbenchWorkflow,
  WorkbenchWorkflowStep,
} from "@/types/context";

type Snapshot = {
  overview: ContextOverview | null;
  workflow: WorkbenchWorkflow | null;
  documents: ContextSourceDocument[];
  runs: ContextOnboardingRun[];
  operations: ContextSourceOperation[];
  parameters: ContextSourceParameter[];
  fields: ContextSourceField[];
  types: CanonicalType[];
  enums: CanonicalEnum[];
  slots: CanonicalSlot[];
  classes: CanonicalClass[];
  classSlotUsages: CanonicalClassSlotUsage[];
  bindings: ContextBinding[];
  capabilities: Capability[];
  capabilityOperations: ContextCapabilityOperation[];
  bundles: ContextProposalBundle[];
};

type StepState = "blocked" | "ready" | "running" | "complete" | "warning";

const stepIcons = {
  upload_source: Upload,
  agent_ingestion: FileSearch,
  review_bundle: PackageCheck,
  extract_assets: FileSearch,
  model_canonical: Layers3,
  bind_fields: GitBranch,
  define_capability: Route,
  validate_bundle: ClipboardCheck,
  submit_review: PackageCheck,
} as const;

const documentTypes = [
  { value: "auto", label: "Auto Detect" },
  { value: "api_document", label: "API Spec" },
  { value: "data_dictionary", label: "Data Dictionary" },
  { value: "schema_document", label: "Schema Document" },
  { value: "sample_payload", label: "Sample Payload" },
  { value: "spreadsheet", label: "Spreadsheet" },
  { value: "manual_field_list", label: "Manual Field List" },
];

export default function ContextPlatformWorkbench() {
  const [snapshot, setSnapshot] = useState<Snapshot>({
    overview: null,
    workflow: null,
    documents: [],
    runs: [],
    operations: [],
    parameters: [],
    fields: [],
    types: [],
    enums: [],
    slots: [],
    classes: [],
    classSlotUsages: [],
    bindings: [],
    capabilities: [],
    capabilityOperations: [],
    bundles: [],
  });
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [sourceName, setSourceName] = useState("");
  const [provider, setProvider] = useState("");
  const [documentType, setDocumentType] = useState("auto");
  const [documentFile, setDocumentFile] = useState<File | null>(null);
  const [activeStepKey, setActiveStepKey] = useState("upload_source");
  const [acting, setActing] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const [
        overview,
        workflow,
        documents,
        runs,
        operations,
        parameters,
        fields,
        types,
        enums,
        slots,
        classes,
        classSlotUsages,
        bindings,
        capabilities,
        capabilityOperations,
        bundles,
      ] = await Promise.all([
        listOverview(),
        getWorkbenchWorkflow(),
        listSourceDocuments(),
        listOnboardingRuns(),
        listSourceOperations(),
        listSourceParameters(),
        listSourceFields(),
        listCanonicalTypes(),
        listCanonicalEnums(),
        listCanonicalSlots(),
        listCanonicalClasses(),
        listCanonicalClassSlotUsages(),
        listBindings(),
        listCapabilities(),
        listCapabilityOperations(),
        listProposalBundles(),
      ]);
      setSnapshot({
        overview,
        workflow,
        documents,
        runs,
        operations,
        parameters,
        fields,
        types,
        enums,
        slots,
        classes,
        classSlotUsages,
        bindings,
        capabilities,
        capabilityOperations,
        bundles,
      });
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load Context Platform data.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  async function handleUpload(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!documentFile || !sourceName.trim()) {
      setError("Source name and source document file are required.");
      return;
    }
    setUploading(true);
    setError("");
    setMessage("");
    try {
      const formData = new FormData();
      formData.set("name", sourceName.trim());
      formData.set("provider", provider.trim());
      formData.set("document_type", documentType);
      formData.set("source_type", documentType === "api_document" ? "api" : "file");
      formData.set("file", documentFile);
      const result = await uploadSourceDocument(formData);
      setMessage(
        `Queued source document for agent ingestion. Run ${result.onboarding_run.id} is waiting for an operator agent response.`
      );
      setSourceName("");
      setProvider("");
      setDocumentType("auto");
      setDocumentFile(null);
      event.currentTarget.reset();
      await reload();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Upload failed.");
    } finally {
      setUploading(false);
    }
  }

  async function handleStepAction() {
    const action = stepAction(activeStep.key);
    if (!action) return;
    setActing(action);
    setError("");
    setMessage("");
    try {
      const result = await runWorkbenchAction(action, {
        source_document_id: activeDocument?.id,
        run_id: activeRun?.id,
      });
      setMessage(actionMessage(result.status, result.reason));
      if (result.workflow) {
        setSnapshot((current) => ({ ...current, workflow: result.workflow || current.workflow }));
      }
      await reload();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Workbench action failed.");
    } finally {
      setActing("");
    }
  }

  async function handleApproveBundle() {
    if (!activeBundle?.id) return;
    setActing("approve-bundle");
    setError("");
    setMessage("");
    try {
      const result = await runWorkbenchAction("approve-bundle", {
        source_document_id: activeDocument?.id,
        run_id: activeRun?.id,
        proposal_bundle_id: activeBundle.id,
        reviewer: "dashboard",
      });
      setMessage(actionMessage(result.status, result.reason, result.applied_count, result.skipped_count));
      if (result.workflow) {
        setSnapshot((current) => ({ ...current, workflow: result.workflow || current.workflow }));
      }
      await reload();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Bundle approval failed.");
    } finally {
      setActing("");
    }
  }

  async function handleRejectBundle() {
    if (!activeBundle?.id) return;
    setActing("reject-bundle");
    setError("");
    setMessage("");
    try {
      const result = await runWorkbenchAction("reject-bundle", {
        source_document_id: activeDocument?.id,
        run_id: activeRun?.id,
        proposal_bundle_id: activeBundle.id,
        reviewer: "dashboard",
        rationale: "Rejected from Context Platform workbench. Regenerate after fixing ingestion output.",
      });
      setMessage(actionMessage(result.status, result.reason, result.applied_count, result.skipped_count, result.rejected_count));
      if (result.workflow) {
        setSnapshot((current) => ({ ...current, workflow: result.workflow || current.workflow }));
      }
      await reload();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Bundle rejection failed.");
    } finally {
      setActing("");
    }
  }

  const activeDocument = snapshot.workflow?.active_document || snapshot.documents[0] || null;
  const activeRun = useMemo(() => {
    if (snapshot.workflow?.active_run) return snapshot.workflow.active_run;
    if (!activeDocument) return snapshot.runs[0] || null;
    return snapshot.runs.find((run) => run.source_document_id === activeDocument.id) || snapshot.runs[0] || null;
  }, [activeDocument, snapshot.runs, snapshot.workflow?.active_run]);
  const activeBundle = useMemo(() => {
    if (snapshot.workflow?.active_bundle) return snapshot.workflow.active_bundle;
    if (!activeRun) return snapshot.bundles[0] || null;
    return snapshot.bundles.find((bundle) => bundle.run_id === activeRun.id) || snapshot.bundles[0] || null;
  }, [activeRun, snapshot.bundles, snapshot.workflow?.active_bundle]);
  const executableOperations = useMemo(() => {
    if (!activeDocument) return snapshot.operations;
    return snapshot.operations.filter(
      (operation) =>
        operation.source_document_id === activeDocument.id ||
        (!operation.source_document_id && operation.source_id === activeDocument.source_id)
    );
  }, [activeDocument, snapshot.operations]);
  const executableOperationIds = useMemo(() => new Set(executableOperations.map((operation) => operation.id)), [executableOperations]);
  const documentFields = useMemo(() => {
    if (!activeDocument) return snapshot.fields;
    return snapshot.fields.filter(
      (field) =>
        field.source_document_id === activeDocument.id ||
        (field.source_operation_id ? executableOperationIds.has(field.source_operation_id) : !field.source_document_id)
    );
  }, [activeDocument, executableOperationIds, snapshot.fields]);
  const latestRunRunning = activeRun?.status === "running" || activeRun?.status === "submitted";
  const hasDocument = Boolean(activeDocument);
  const hasExtractedAssets = executableOperations.length > 0 || documentFields.length > 0;
  const workflowCounts = snapshot.workflow?.counts || {};
  const bundleSummary = activeBundle?.summary || {};
  const hasCanonicalDrafts =
    Boolean(activeBundle) &&
    (Number(bundleSummary.meaning_term_count || bundleSummary.canonical_term_count || 0) > 0 ||
      proposalCount(snapshot, ["meaning_resolution_decision", "canonical_class_slot"]) > 0);
  const hasBindingDrafts =
    Boolean(activeBundle) &&
    (Number(bundleSummary.resolution_term_count || bundleSummary.binding_term_count || 0) > 0 ||
      proposalCount(snapshot, ["resolution_binding", "binding"]) > 0);
  const hasCapabilityDrafts =
    Boolean(activeBundle) &&
    (Number(bundleSummary.capability_contract_count || bundleSummary.capability_operation_count || 0) > 0 ||
      proposalCount(snapshot, ["capability", "capability_step", "capability_operation"]) > 0);
  const hasBundle = Boolean(activeBundle);
  const hasExecutableCapability = Boolean(snapshot.workflow?.execution_ready) || (snapshot.capabilityOperations.length > 0 && executableOperations.length > 0);
  const fallbackSteps: WorkbenchWorkflowStep[] = [
    {
      key: "upload_source",
      number: 1,
      title: "Upload Source Document",
      state: uploading ? "running" : hasDocument ? "complete" : "ready",
      detail: hasDocument ? activeDocument?.name || "Document uploaded" : "Start with any source document, API or non-API.",
    },
  ];
  const workflowSteps: WorkbenchWorkflowStep[] = snapshot.workflow?.steps?.length
    ? snapshot.workflow.steps
    : fallbackSteps;
  const steps = workflowSteps.map((step) => ({
    ...step,
    state: step.key === "upload_source" && uploading ? "running" : step.state,
    icon: stepIcons[step.key as keyof typeof stepIcons] || Layers3,
  })) satisfies Array<{ number: number; title: string; state: StepState; detail: string; icon: typeof Upload }>;
  const activeStep = steps.find((step) => step.key === activeStepKey) || steps[0];
  const currentAction = stepAction(activeStep.key);
  const actionDisabled =
    loading ||
    uploading ||
    Boolean(acting) ||
    !currentAction ||
    (currentAction === "submit-proposal" && !hasBundle);

  useEffect(() => {
    if (!snapshot.workflow?.steps?.length) return;
    const keys = new Set(snapshot.workflow.steps.map((step) => step.key));
    if (!keys.has(activeStepKey)) {
      setActiveStepKey(snapshot.workflow.steps[0]?.key || "upload_source");
    }
  }, [activeStepKey, snapshot.workflow?.steps]);

  return (
    <div className="mx-auto flex max-w-[1480px] flex-col gap-5">
      <section className="flex flex-col gap-4 border-b border-border pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">
            <Layers3 className="h-3.5 w-3.5" />
            Context Platform Review Workbench
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Source intake, agent ingestion, governed review</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Upload source documents for operator-agent ingestion, then review the generated meaning graph proposal bundle
            before approval and publication.
          </p>
        </div>
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading || uploading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </section>

      {error ? <Notice tone="danger" message={error} /> : null}
      {message ? <Notice tone="success" message={message} /> : null}

      <section className="grid gap-3 xl:grid-cols-5">
        {steps.map((step, index) => (
          <StepCard
            key={step.number}
            step={step}
            selected={step.key === activeStep.key}
            showArrow={index < steps.length - 1}
            onSelect={() => setActiveStepKey(step.key)}
          />
        ))}
      </section>

      <section className="rounded-lg border border-border bg-card px-4 py-3">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="min-w-0">
            <div className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">Current Step</div>
            <div className="mt-1 text-lg font-semibold text-foreground">
              {activeStep.number}. {activeStep.title}
            </div>
            <div className="mt-1 text-sm text-muted-foreground">{activeStep.detail}</div>
          </div>
          <div className="flex shrink-0 flex-wrap items-center gap-2">
            <StatusBadge status={activeStep.state} />
            {currentAction ? (
              <Button type="button" variant="outline" onClick={() => void handleStepAction()} disabled={actionDisabled}>
                <ClipboardCheck className={`h-4 w-4 ${acting === currentAction ? "animate-pulse" : ""}`} />
                {actionLabel(currentAction)}
              </Button>
            ) : null}
          </div>
        </div>
      </section>

      {activeStep.key === "upload_source" ? (
      <section className="grid gap-5 xl:grid-cols-[420px_minmax(0,1fr)]">
        <Card>
          <CardHeader>
            <div>
              <CardTitle>1. Upload Source Document</CardTitle>
              <CardDescription>Uploads create source intake records. Semantic drafting is handled by an operator agent.</CardDescription>
            </div>
            <Upload className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <form className="space-y-3" onSubmit={handleUpload}>
              <Field label="Source Name">
                <input
                  className="h-9 w-full rounded-lg border border-border bg-background px-3 text-sm outline-none focus:ring-2 focus:ring-primary/25"
                  value={sourceName}
                  onChange={(event) => setSourceName(event.target.value)}
                  placeholder="company-reference-source"
                />
              </Field>
              <Field label="Provider">
                <input
                  className="h-9 w-full rounded-lg border border-border bg-background px-3 text-sm outline-none focus:ring-2 focus:ring-primary/25"
                  value={provider}
                  onChange={(event) => setProvider(event.target.value)}
                  placeholder="provider or owner"
                />
              </Field>
              <Field label="Document Type">
                <select
                  className="h-9 w-full rounded-lg border border-border bg-background px-3 text-sm outline-none focus:ring-2 focus:ring-primary/25"
                  value={documentType}
                  onChange={(event) => setDocumentType(event.target.value)}
                >
                  {documentTypes.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </Field>
              <Field label="Source Document">
                <input
                  className="block w-full rounded-lg border border-border bg-background px-3 py-2 text-sm file:mr-3 file:rounded-md file:border-0 file:bg-muted file:px-2.5 file:py-1.5 file:text-xs file:font-medium"
                  type="file"
                  accept=".pdf,.json,.yaml,.yml,.txt,.md,.csv,.tsv,application/pdf,application/json,text/*"
                  onChange={(event: ChangeEvent<HTMLInputElement>) => setDocumentFile(event.target.files?.[0] ?? null)}
                />
              </Field>
              <Button className="w-full" type="submit" disabled={uploading || loading}>
                <Send className="h-4 w-4" />
                {uploading ? "Uploading" : "Queue Agent Ingestion"}
              </Button>
            </form>
          </CardContent>
        </Card>

        <StageGuide
          title="Source intake"
          rows={[
            ["Accepted inputs", "API specs, data dictionaries, schema files, samples, spreadsheets, field lists"],
            ["Storage", "Uploaded files are stored through MinIO"],
            ["Owner", "Operator agent creates the ingestion response artifact"],
            ["Next step", "Review starts after a proposal bundle is generated"],
          ]}
        />
      </section>
      ) : null}

      {activeStep.key === "agent_ingestion" ? (
      <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
        <Card>
          <CardHeader>
            <div>
              <CardTitle>2. Agent Ingestion Queue</CardTitle>
              <CardDescription>Dashboard tracks intake and reviews agent output; it does not generate source meaning.</CardDescription>
            </div>
            <FileSearch className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent className="grid gap-4 xl:grid-cols-2">
            <AssetPanel
              title="Source Operations"
              empty="No operations have been generated yet."
              items={executableOperations.slice(0, 6).map((operation) => ({
                title: `${operation.method} ${operation.path}`,
                detail: operation.name,
                status: operation.status || "draft",
              }))}
            />
            <AssetPanel
              title="Source Fields"
              empty="No source fields have been generated yet."
              items={documentFields.slice(0, 8).map((field) => ({
                title: field.field_path,
                detail: field.source_operation_id ? "operation field" : "document scoped field",
                status: field.status || "draft",
              }))}
            />
          </CardContent>
        </Card>
        <StageGuide
          title="Agent-owned work"
          rows={[
            ["Run status", activeRun?.status || "not created"],
            ["Run stage", activeRun?.stage || "none"],
            ["Operations", executableOperations.length],
            ["Fields", documentFields.length],
            ["Proposal bundle", activeBundle ? activeBundle.status : "not available"],
          ]}
        />
      </section>
      ) : null}

      {activeStep.key === "review_bundle" ? (
      <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_420px]">
        <DraftPanel
          icon={<PackageCheck className="h-4 w-4 text-muted-foreground" />}
          title="3. Review Proposal Bundle"
          description="Agent output is reviewed as one bundle across concepts, representations, schemas, resolution bindings, and capabilities."
          rows={[
            ["Object types", snapshot.classes.length],
            ["Property types", snapshot.slots.length],
            ["Representation schemas", snapshot.types.length],
            ["Resolution edges", snapshot.bindings.length],
            ["Capabilities", snapshot.capabilities.length],
            ["Bundle status", activeBundle?.status || "not available"],
          ]}
          blocked={!hasBundle}
          blockedReason="An operator agent must generate a proposal bundle first."
        />
        <StageGuide
          title="Review boundary"
          rows={[
            ["Agent output", "Must pass through the proposal bundle boundary"],
            ["Dashboard role", "Review, validate, approve, and publish"],
            ["Runtime rule", "Only approved graph data is planner-visible"],
          ]}
        />
      </section>
      ) : null}

      {activeStep.key === "extract_assets" ? (
      <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
        <Card>
          <CardHeader>
            <div>
              <CardTitle>2. Inspect Extracted Assets</CardTitle>
              <CardDescription>Operations are executable only when a real API endpoint exists. Fields may be document-only.</CardDescription>
            </div>
            <FileSearch className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent className="grid gap-4 xl:grid-cols-2">
            <AssetPanel
              title="Executable Operations"
              empty="No executable operations found."
              items={executableOperations.slice(0, 6).map((operation) => ({
                title: `${operation.method} ${operation.path}`,
                detail: operation.name,
                status: operation.status || "draft",
              }))}
            />
            <AssetPanel
              title="Extracted Fields"
              empty="No source fields extracted yet."
              items={documentFields.slice(0, 8).map((field) => ({
                title: field.field_path,
                detail: field.source_operation_id ? "operation field" : "document scoped field",
                status: field.status || "draft",
              }))}
            />
          </CardContent>
        </Card>
        <StageGuide
          title="Discovery result"
          rows={[
            ["Executable operations", executableOperations.length],
            ["Input parameters", snapshot.parameters.length],
            ["Extracted fields", documentFields.length],
            ["Source mode", executableOperations.length ? "executable" : "knowledge-only"],
          ]}
        />
      </section>
      ) : null}

      {activeStep.key === "model_canonical" ? (
      <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
        <DraftPanel
          icon={<Layers3 className="h-4 w-4 text-muted-foreground" />}
          title="3. Model Meaning And Representation"
          description="Concepts, object types, property types, representation templates, and schemas are drafted from extracted evidence."
          rows={[
            ["Object types", snapshot.classes.length],
            ["Property types", snapshot.slots.length],
            ["Representation schemas", snapshot.types.length],
            ["Value domains", snapshot.enums.length],
            ["Representation proposals", proposalCount(snapshot, ["meaning_resolution_decision", "canonical_class_slot"])],
          ]}
          blocked={!hasExtractedAssets}
          blockedReason="Extract fields or operations first."
        />
        <StageGuide
          title="Representation model"
          rows={[
            ["Concept", "Meaning atom"],
            ["Object / property", "Structural carrier and value slot"],
            ["Representation schema", "Datatype, regex, enum, and validation rules"],
            ["Runtime storage", "PostgreSQL meaning and representation tables"],
          ]}
        />
      </section>
      ) : null}

      {activeStep.key === "bind_fields" ? (
      <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
        <DraftPanel
          icon={<GitBranch className="h-4 w-4 text-muted-foreground" />}
          title="4. Resolve Source To Representation"
          description="Resolution edges connect source parameters or fields to representations, context keys, and required concepts."
          rows={[
            ["Resolution edges", snapshot.bindings.length],
            ["Resolution proposals", proposalCount(snapshot, ["resolution_binding", "binding"])],
            ["Input params", snapshot.parameters.length],
            ["Output fields", documentFields.length],
          ]}
          blocked={!hasCanonicalDrafts}
          blockedReason="Shape meaning and representation draft first."
        />
        <StageGuide
          title="Resolution graph"
          rows={[
            ["Parameter binding", "Source parameter to required concept"],
            ["Field binding", "Source field to representation value"],
            ["Context binding", "Source field to representation context"],
            ["Transform", "Normalization and enum mapping are recorded when needed"],
          ]}
        />
      </section>
      ) : null}

      {activeStep.key === "define_capability" ? (
      <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
        <DraftPanel
          icon={<Route className="h-4 w-4 text-muted-foreground" />}
          title="5. Define Capability Draft"
          description="Capabilities depend on required concepts, provided concepts, representations, and executable source operation steps."
          rows={[
            ["Capabilities", snapshot.capabilities.length],
            ["Capability steps", snapshot.capabilityOperations.length],
            ["Executable operations", executableOperations.length],
            ["Mode", snapshot.workflow?.mode === "executable" ? "executable candidate" : "knowledge-only"],
          ]}
          blocked={!hasBindingDrafts}
          blockedReason="Map fields first."
        />
        <StageGuide
          title="Capability catalog"
          rows={[
            ["Capability", "Planner-facing business intent"],
            ["Operation link", "Added only when source_operations exist"],
            ["Knowledge-only", "Non-API documents can still contribute reviewed catalog knowledge"],
            ["Planner execution", hasExecutableCapability ? "ready" : "not ready"],
          ]}
        />
      </section>
      ) : null}

      {activeStep.key === "validate_bundle" ? (
      <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_420px]">
        <Card id="validate-final-bundle">
          <CardHeader>
            <div>
              <CardTitle>4. Validate Review Bundle</CardTitle>
              <CardDescription>Validation checks graph dependencies before approval.</CardDescription>
            </div>
            <ClipboardCheck className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-2">
            <CheckRow ok={hasDocument} label="Source document uploaded" />
            <CheckRow ok={!latestRunRunning && hasExtractedAssets} label="Agent extraction generated source assets" />
            <CheckRow ok={hasCanonicalDrafts} label="Meaning and representation draft available" />
            <CheckRow ok={hasBindingDrafts} label="Resolution draft available" />
            <CheckRow ok={hasCapabilityDrafts} label="Capability draft evaluated" />
            <CheckRow
              ok={(snapshot.workflow?.counts.endpoint_checks ?? 0) > 0}
              warning={executableOperations.length > 0 && (snapshot.workflow?.counts.endpoint_checks ?? 0) === 0}
              label={`Endpoint checks recorded: ${snapshot.workflow?.counts.endpoint_checks ?? 0}`}
            />
            <CheckRow ok={hasBundle} label="Final proposal bundle created" />
            <CheckRow
              ok={executableOperations.length > 0}
              warning={!executableOperations.length && hasExtractedAssets}
              label={executableOperations.length ? "Executable operation available" : "Knowledge-only document"}
            />
            <CheckRow ok={hasExecutableCapability} warning={hasBundle && !hasExecutableCapability} label="Planner execution readiness" />
          </CardContent>
        </Card>
        <StageGuide
          title="Validation scope"
          rows={[
            ["Dependency", "Review depends on the generated proposal bundle"],
            ["Endpoint checks", `${snapshot.workflow?.counts.endpoint_checks_verified ?? 0} verified, ${snapshot.workflow?.counts.endpoint_checks_needs_input ?? 0} need input, ${snapshot.workflow?.counts.endpoint_checks_skipped ?? 0} skipped`],
            ["Final artifact", "A single proposal bundle is reviewed"],
            ["Execution", "Only validated executable capabilities are planner-ready"],
            ["Knowledge-only", "Valid for catalog review, not raw operation execution"],
          ]}
        />
      </section>
      ) : null}

      {activeStep.key === "submit_review" ? (
      <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_420px]">
        <StageGuide
          title="Final proposal policy"
          rows={[
            ["Review unit", "One source creates one final dependency bundle"],
            ["Lifecycle", "Proposed, reviewed, approved, published"],
            ["Auto approval", "Generated artifacts stay reviewable until explicitly approved"],
          ]}
        />
        <Card id="submit-final-proposal">
          <CardHeader>
            <div>
              <CardTitle>5. Approve Or Publish</CardTitle>
              <CardDescription>Approved bundle changes become the governed graph visible to planner runtime.</CardDescription>
            </div>
            <PackageCheck className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent className="space-y-3">
            {activeBundle ? (
              <div className="rounded-lg border border-border p-3">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-medium text-foreground">{activeBundle.title}</div>
                    <div className="mt-1 text-xs text-muted-foreground">
                      {activeBundle.proposal_count || 0} items · {bundleMode(activeBundle)}
                    </div>
                  </div>
                  <StatusBadge status={activeBundle.status} />
                </div>
                <div className="mt-3 rounded-md bg-muted/35 px-3 py-2 text-xs text-muted-foreground">
                  {bundleNote(activeBundle)}
                </div>
              </div>
            ) : (
              <EmptyState message={loading ? "Loading bundle..." : "No final proposal bundle yet."} />
            )}
            <div className="grid gap-2 sm:grid-cols-2">
              <Button
                type="button"
                variant="outline"
                disabled={!activeBundle || Boolean(acting) || activeBundle.status === "approved" || activeBundle.status === "rejected"}
                onClick={() => void handleRejectBundle()}
              >
                <XCircle className="h-4 w-4" />
                {activeBundle?.status === "rejected" ? "Bundle Rejected" : acting === "reject-bundle" ? "Rejecting Bundle" : "Reject Bundle"}
              </Button>
              <Button
                type="button"
                disabled={!activeBundle || Boolean(acting) || activeBundle.status === "approved" || activeBundle.status === "rejected"}
                onClick={() => void handleApproveBundle()}
              >
                {activeBundle?.status === "approved" ? <CheckCircle2 className="h-4 w-4" /> : <PackageCheck className="h-4 w-4" />}
                {activeBundle?.status === "approved" ? "Bundle Applied" : acting === "approve-bundle" ? "Applying Bundle" : "Approve & Apply Bundle"}
              </Button>
            </div>
          </CardContent>
        </Card>
      </section>
      ) : null}
    </div>
  );
}

function proposalCount(snapshot: Snapshot, entityType: string | string[]) {
  const entityTypes = new Set(Array.isArray(entityType) ? entityType : [entityType]);
  return (snapshot.overview?.recent_proposals || []).filter((proposal) => entityTypes.has(proposal.entity_type)).length;
}

type WorkbenchActionName = "validate" | "submit-proposal" | "approve-bundle" | "reject-bundle";

function stepAction(stepKey: string): WorkbenchActionName | null {
  if (stepKey === "validate_bundle") return "validate";
  if (stepKey === "submit_review") return "submit-proposal";
  return null;
}

function actionLabel(action: WorkbenchActionName) {
  if (action === "validate") return "Validate Bundle";
  if (action === "approve-bundle") return "Approve Bundle";
  if (action === "reject-bundle") return "Reject Bundle";
  return "Submit Proposal";
}

function actionMessage(status: string, reason?: string, appliedCount?: number, skippedCount?: number, rejectedCount?: number) {
  if (status === "validated") return "Validated current workspace dependencies.";
  if (status === "ready_for_review") return "Final proposal bundle is ready for review.";
  if (status === "approved") return `Approved and applied bundle: ${appliedCount || 0} catalog changes, ${skippedCount || 0} skipped.`;
  if (status === "rejected") return `Rejected proposal bundle: ${rejectedCount || 0} proposal items.`;
  if (status === "submitted") return "Submitted the workspace action.";
  return `Workbench action returned ${status}.`;
}

function bundleMode(bundle: ContextProposalBundle) {
  const executable = bundle.summary?.executable;
  return executable ? "executable candidate" : "knowledge-only";
}

function bundleNote(bundle: ContextProposalBundle) {
  const note = bundle.summary?.execution_note;
  return typeof note === "string" ? note : "Review the full dependency bundle before publication.";
}

function StepCard({
  step,
  selected,
  showArrow,
  onSelect,
}: {
  step: { key: string; number: number; title: string; state: StepState; detail: string; icon: typeof Upload };
  selected: boolean;
  showArrow: boolean;
  onSelect: () => void;
}) {
  const Icon = step.icon;
  return (
    <div className="relative">
      <button
        type="button"
        onClick={onSelect}
        className={`h-full w-full rounded-lg border p-3 text-left transition ${
          selected ? "border-primary/35 bg-primary/[0.08]" : "border-border bg-card hover:border-primary/20 hover:bg-muted/20"
        }`}
      >
        <div className="flex items-start justify-between gap-2">
          <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-border bg-muted/30">
            <Icon className="h-4 w-4" />
          </div>
          <StatusBadge status={step.state} />
        </div>
        <div className="mt-3 text-xs font-medium text-muted-foreground">Step {step.number}</div>
        <div className="mt-1 text-sm font-semibold text-foreground">{step.title}</div>
        <div className="mt-2 text-xs leading-5 text-muted-foreground">{step.detail}</div>
      </button>
      {showArrow ? <ArrowRight className="absolute -right-3 top-1/2 z-10 hidden h-4 w-4 -translate-y-1/2 text-muted-foreground xl:block" /> : null}
    </div>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="block">
      <span className="mb-1.5 block text-xs font-medium text-muted-foreground">{label}</span>
      {children}
    </label>
  );
}

function Notice({ tone, message }: { tone: "success" | "danger"; message: string }) {
  return (
    <div
      className={`rounded-lg border px-4 py-3 text-sm ${
        tone === "success"
          ? "border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
          : "border-red-500/25 bg-red-500/10 text-red-700 dark:text-red-300"
      }`}
    >
      {message}
    </div>
  );
}

function StatusBadge({ status }: { status?: string }) {
  const value = status || "draft";
  const variant =
    value === "complete" || value === "approved" || value === "published"
      ? "success"
      : value === "blocked" || value === "rejected"
        ? "danger"
        : value === "running" || value === "ready" || value === "proposed"
          ? "info"
          : "warning";
  return <Badge variant={variant}>{value}</Badge>;
}

function AssetPanel({
  title,
  items,
  empty,
}: {
  title: string;
  items: Array<{ title: string; detail: string; status: string }>;
  empty: string;
}) {
  return (
    <div className="rounded-lg border border-border p-3">
      <div className="mb-3 text-sm font-semibold text-foreground">{title}</div>
      <div className="space-y-2">
        {items.length ? (
          items.map((item) => (
            <div key={`${item.title}-${item.detail}`} className="rounded-md border border-border/70 px-3 py-2">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="truncate text-sm font-medium">{item.title}</div>
                  <div className="mt-1 truncate text-xs text-muted-foreground">{item.detail}</div>
                </div>
                <StatusBadge status={item.status} />
              </div>
            </div>
          ))
        ) : (
          <EmptyState message={empty} />
        )}
      </div>
    </div>
  );
}

function DraftPanel({
  icon,
  title,
  description,
  rows,
  blocked,
  blockedReason,
}: {
  icon: ReactNode;
  title: string;
  description: string;
  rows: Array<[string, string | number]>;
  blocked: boolean;
  blockedReason: string;
}) {
  return (
    <Card>
      <CardHeader>
        <div>
          <CardTitle>{title}</CardTitle>
          <CardDescription>{description}</CardDescription>
        </div>
        {icon}
      </CardHeader>
      <CardContent className="space-y-3">
        {blocked ? <Notice tone="danger" message={blockedReason} /> : null}
        <div className="grid gap-2">
          {rows.map(([label, value]) => (
            <div key={label} className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
              <span className="text-sm text-muted-foreground">{label}</span>
              <span className="text-sm font-medium text-foreground">{value}</span>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function StageGuide({ title, rows }: { title: string; rows: Array<[string, string | number]> }) {
  return (
    <Card>
      <CardHeader>
        <div>
          <CardTitle>{title}</CardTitle>
          <CardDescription>What this step controls in the source workspace.</CardDescription>
        </div>
        <ClipboardCheck className="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent className="grid gap-2">
        {rows.map(([label, value]) => (
          <div key={label} className="rounded-lg border border-border px-3 py-2">
            <div className="text-xs font-medium text-muted-foreground">{label}</div>
            <div className="mt-1 text-sm text-foreground">{value}</div>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function CheckRow({ ok, warning, label }: { ok: boolean; warning?: boolean; label: string }) {
  const tone = ok ? "success" : warning ? "warning" : "danger";
  return (
    <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
      <div className="flex min-w-0 items-center gap-2">
        {ok ? <CheckCircle2 className="h-4 w-4 text-emerald-600" /> : <Lock className="h-4 w-4 text-muted-foreground" />}
        <span className="truncate text-sm text-foreground">{label}</span>
      </div>
      <Badge variant={tone}>{ok ? "ok" : warning ? "warning" : "blocked"}</Badge>
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-lg border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
      {message}
    </div>
  );
}
