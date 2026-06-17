"use client";

import type { FormEvent } from "react";
import { Input } from "@/components/ui/input";
import { FormField, FormGrid, FormShell, FormTextarea } from "@/components/semantic/forms/form-shell";

export type CapabilityFormState = {
  capabilityKey: string;
  namespace: string;
  name: string;
  description: string;
  version: string;
  lifecycle: string;
  status: string;
  inputSemanticTypes: string;
  outputSemanticTypes: string;
  intentSpec: string;
};

export const capabilityFormDefaults: CapabilityFormState = {
  capabilityKey: "",
  namespace: "public",
  name: "",
  description: "",
  version: "1.0.0",
  lifecycle: "draft",
  status: "draft",
  inputSemanticTypes: "",
  outputSemanticTypes: "",
  intentSpec: ""
};

type CapabilityFormProps = {
  title: string;
  description: string;
  form: CapabilityFormState;
  onChange: (next: CapabilityFormState) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onCancel: () => void;
  submitLabel: string;
  submitting?: boolean;
};

export function CapabilityForm(props: CapabilityFormProps) {
  const { form, onChange } = props;
  return (
    <FormShell {...props}>
      <FormGrid>
        <FormField label="Capability Key">
          <Input value={form.capabilityKey} onChange={(event) => onChange({ ...form, capabilityKey: event.target.value })} required />
        </FormField>
        <FormField label="Name">
          <Input value={form.name} onChange={(event) => onChange({ ...form, name: event.target.value })} required />
        </FormField>
        <FormField label="Namespace">
          <Input value={form.namespace} onChange={(event) => onChange({ ...form, namespace: event.target.value })} />
        </FormField>
        <FormField label="Status">
          <Input value={form.status} onChange={(event) => onChange({ ...form, status: event.target.value })} />
        </FormField>
        <FormField label="Version">
          <Input value={form.version} onChange={(event) => onChange({ ...form, version: event.target.value })} />
        </FormField>
        <FormField label="Lifecycle">
          <Input value={form.lifecycle} onChange={(event) => onChange({ ...form, lifecycle: event.target.value })} />
        </FormField>
      </FormGrid>
      <FormField label="Description">
        <FormTextarea value={form.description} onChange={(event) => onChange({ ...form, description: event.target.value })} />
      </FormField>
      <FormGrid>
        <FormField label="Input Semantic Types">
          <Input value={form.inputSemanticTypes} onChange={(event) => onChange({ ...form, inputSemanticTypes: event.target.value })} placeholder="comma,separated ids" />
        </FormField>
        <FormField label="Output Semantic Types">
          <Input value={form.outputSemanticTypes} onChange={(event) => onChange({ ...form, outputSemanticTypes: event.target.value })} placeholder="comma,separated ids" />
        </FormField>
      </FormGrid>
      <FormField label="Intent Spec">
        <FormTextarea value={form.intentSpec} onChange={(event) => onChange({ ...form, intentSpec: event.target.value })} placeholder='{"examples":["..."]}' />
      </FormField>
    </FormShell>
  );
}
