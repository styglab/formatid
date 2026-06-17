"use client";

import type { FormEvent } from "react";
import { Input } from "@/components/ui/input";
import { FormField, FormGrid, FormSelect, FormShell, FormTextarea } from "@/components/semantic/forms/form-shell";

export type SourceFormState = {
  name: string;
  provider: string;
  sourceType: string;
  description: string;
  status: string;
  referenceUri: string;
  manualNotes: string;
};

export const sourceFormDefaults: SourceFormState = {
  name: "",
  provider: "",
  sourceType: "api",
  description: "",
  status: "draft",
  referenceUri: "",
  manualNotes: ""
};

type SourceFormProps = {
  title: string;
  description: string;
  form: SourceFormState;
  file?: File | null;
  onChange: (next: SourceFormState) => void;
  onFileChange?: (file: File | null) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onCancel: () => void;
  submitLabel: string;
  submitting?: boolean;
  showUpload?: boolean;
};

export function SourceForm(props: SourceFormProps) {
  const { form, onChange } = props;
  return (
    <FormShell {...props}>
      <FormGrid>
        <FormField label="Name">
          <Input value={form.name} onChange={(event) => onChange({ ...form, name: event.target.value })} required />
        </FormField>
        <FormField label="Provider">
          <Input value={form.provider} onChange={(event) => onChange({ ...form, provider: event.target.value })} />
        </FormField>
        <FormField label="Source Type">
          <FormSelect value={form.sourceType} onChange={(event) => onChange({ ...form, sourceType: event.target.value })}>
            <option value="api">api</option>
            <option value="file">file</option>
            <option value="database">database</option>
            <option value="csv">csv</option>
          </FormSelect>
        </FormField>
        <FormField label="Status">
          <FormSelect value={form.status} onChange={(event) => onChange({ ...form, status: event.target.value })}>
            <option value="draft">draft</option>
            <option value="review">review</option>
            <option value="approved">approved</option>
            <option value="published">published</option>
          </FormSelect>
        </FormField>
      </FormGrid>
      <FormField label="Description">
        <FormTextarea value={form.description} onChange={(event) => onChange({ ...form, description: event.target.value })} />
      </FormField>
      <FormGrid>
        <FormField label="Reference URI">
          <Input value={form.referenceUri} onChange={(event) => onChange({ ...form, referenceUri: event.target.value })} />
        </FormField>
        {props.showUpload ? (
          <FormField label="Upload File">
            <Input type="file" onChange={(event) => props.onFileChange?.(event.target.files?.[0] || null)} />
          </FormField>
        ) : null}
      </FormGrid>
      <FormField label="Manual Notes">
        <FormTextarea value={form.manualNotes} onChange={(event) => onChange({ ...form, manualNotes: event.target.value })} />
      </FormField>
    </FormShell>
  );
}
