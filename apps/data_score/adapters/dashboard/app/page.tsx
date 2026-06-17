"use client";

import { useEffect, useState } from "react";

type RunListItem = {
  run_id: string;
  dataset_name: string;
  status: string;
  llm_mode: string;
  created_at: string | null;
  finished_at: string | null;
  duration_ms: number | null;
  summary: {
    overall_score?: number | null;
  };
};

type RunDetail = {
  run_id: string;
  dataset_name: string;
  llm_mode: string;
  business_context?: string | null;
  status: string;
  summary: {
    overall_score?: number | null;
  };
  error?: { type?: string; message?: string } | null;
  created_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  updated_at: string | null;
  duration_ms: number | null;
};

type RunSummary = {
  run_id: string;
  status: string;
  dataset_name: string;
  summary: {
    overall_score?: number | null;
  };
  scores: {
    traditional_score?: number | null;
    semantic_score?: number | null;
  };
  issues: Array<{
    severity?: string;
    dimension?: string;
    message?: string;
  }>;
  suggestions: string[];
};

type RunReport = Record<string, unknown>;

type CreateEvaluationResponse = {
  run_id: string;
  status: string;
  dataset_name: string;
  created_at: string | null;
  message: string;
  status_url: string;
  report_url: string;
};

const API_BASE = "/data-score";
const DEFAULT_CSV = `company_name,description,category
Samsung Electronics,Global semiconductor and consumer electronics manufacturer,technology
LG Energy Solution,Battery manufacturer for electric vehicles,energy`;

export default function DataScoreDashboardPage() {
  const [datasetName, setDatasetName] = useState("dataset.company_profiles");
  const [businessContext, setBusinessContext] = useState("vendor discovery");
  const [llmMode, setLlmMode] = useState("disabled");
  const [csvText, setCsvText] = useState(DEFAULT_CSV);
  const [message, setMessage] = useState("");
  const [runs, setRuns] = useState<RunListItem[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [summary, setSummary] = useState<RunSummary | null>(null);
  const [report, setReport] = useState<RunReport | null>(null);

  useEffect(() => {
    void refreshRuns(true);
    const timer = window.setInterval(() => {
      void refreshRuns(false);
    }, 3000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!selectedRunId) {
      setDetail(null);
      setSummary(null);
      setReport(null);
      return;
    }
    void loadRunDetail(selectedRunId);
  }, [selectedRunId]);

  async function fetchJson<T>(path: string, options?: RequestInit): Promise<T> {
    const response = await fetch(`${API_BASE}${path}`, options);
    const data = (await response.json()) as T & { detail?: string; message?: string };
    if (!response.ok) {
      throw new Error(data.detail || data.message || `Request failed: ${response.status}`);
    }
    return data;
  }

  async function refreshRuns(selectNewest: boolean) {
    const data = await fetchJson<{ runs: RunListItem[] }>("/evaluations");
    const nextRuns = Array.isArray(data.runs) ? data.runs : [];
    setRuns(nextRuns);
    if (selectNewest && nextRuns.length > 0) {
      setSelectedRunId(nextRuns[0].run_id);
      return;
    }
    if (selectedRunId && nextRuns.some((run) => run.run_id === selectedRunId)) {
      return;
    }
    if (!selectedRunId && nextRuns.length > 0) {
      setSelectedRunId(nextRuns[0].run_id);
    }
  }

  async function loadRunDetail(runId: string) {
    const nextDetail = await fetchJson<RunDetail>(`/evaluations/${encodeURIComponent(runId)}`);
    const nextSummary = await fetchJson<RunSummary>(`/evaluations/${encodeURIComponent(runId)}/summary`);
    let nextReport: RunReport | null = null;
    try {
      nextReport = await fetchJson<RunReport>(`/evaluations/${encodeURIComponent(runId)}/report`);
    } catch {
      nextReport = null;
    }
    setDetail(nextDetail);
    setSummary(nextSummary);
    setReport(nextReport);
  }

  async function submitEvaluation(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage("Queueing evaluation...");
    try {
      const response = await fetchJson<CreateEvaluationResponse>("/evaluations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset_name: datasetName,
          business_context: businessContext || null,
          llm_mode: llmMode,
          csv_text: csvText
        })
      });
      setMessage(`Queued ${response.run_id}. Worker will process it asynchronously.`);
      setSelectedRunId(response.run_id);
      await refreshRuns(true);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unknown error");
    }
  }

  const completedCount = runs.filter((run) => run.status === "completed").length;
  const pendingCount = runs.filter((run) => run.status === "pending" || run.status === "running").length;

  return (
    <main className="page-shell">
      <section className="hero-panel">
        <div className="hero-copy">
          <p className="eyebrow">AI-Native Data Quality Evaluation</p>
          <h1>Data Score</h1>
          <p className="lede">
            Queue evaluation runs, inspect worker progress, and review structured score summaries
            without leaving the dashboard.
          </p>
        </div>
        <div className="hero-stats">
          <StatCard label="Runs" value={String(runs.length)} />
          <StatCard label="Completed" value={String(completedCount)} />
          <StatCard label="Pending" value={String(pendingCount)} />
        </div>
      </section>

      <section className="workspace">
        <section className="workspace-panel">
          <div className="section-head">
            <h2>Submit Evaluation</h2>
            <button type="button" className="secondary-button" onClick={() => void refreshRuns(false)}>
              Refresh Runs
            </button>
          </div>

          <form className="form-grid" onSubmit={submitEvaluation}>
            <label>
              Dataset Name
              <input value={datasetName} onChange={(event) => setDatasetName(event.target.value)} required />
            </label>
            <label>
              Business Context
              <input value={businessContext} onChange={(event) => setBusinessContext(event.target.value)} />
            </label>
            <label>
              LLM Mode
              <select value={llmMode} onChange={(event) => setLlmMode(event.target.value)}>
                <option value="disabled">disabled</option>
                <option value="codex_manual">codex_manual</option>
                <option value="openai">openai</option>
              </select>
            </label>
            <label>
              CSV Text
              <textarea value={csvText} onChange={(event) => setCsvText(event.target.value)} required />
            </label>
            <div className="actions">
              <button type="submit" className="primary-button">
                Queue Evaluation
              </button>
            </div>
            <p className="message-line">{message || "Use queued runs for worker-driven processing and report review."}</p>
          </form>

          <div className="section-head section-head-spaced">
            <h2>Recent Runs</h2>
          </div>
          <div className="run-list">
            {runs.length === 0 ? (
              <p className="empty-state">No evaluation runs have been queued yet.</p>
            ) : (
              runs.map((run) => (
                <button
                  key={run.run_id}
                  type="button"
                  className={`run-card ${selectedRunId === run.run_id ? "run-card-active" : ""}`}
                  onClick={() => setSelectedRunId(run.run_id)}
                >
                  <div className="run-card-head">
                    <strong>{run.dataset_name}</strong>
                    <span className={`status-badge status-${run.status}`}>{run.status}</span>
                  </div>
                  <p className="run-meta">{run.run_id}</p>
                  <p className="run-meta">
                    Overall {formatValue(run.summary?.overall_score)} · {run.created_at || "-"}
                  </p>
                </button>
              ))
            )}
          </div>
        </section>

        <section className="workspace-panel">
          <div className="section-head">
            <h2>Run Detail</h2>
          </div>
          {detail === null || summary === null ? (
            <p className="empty-state">Select a run to inspect status and quality report.</p>
          ) : (
            <>
              <div className="detail-grid">
                <DetailCard label="Run ID" value={detail.run_id} />
                <DetailCard label="Status" value={detail.status} />
                <DetailCard label="Overall Score" value={formatValue(detail.summary?.overall_score)} />
              </div>
              <div className="detail-grid">
                <DetailCard label="Dataset" value={detail.dataset_name} />
                <DetailCard label="LLM Mode" value={detail.llm_mode} />
                <DetailCard
                  label="Duration"
                  value={detail.duration_ms == null ? "-" : `${detail.duration_ms} ms`}
                />
              </div>
              <div className="detail-grid">
                <DetailCard label="Traditional Score" value={formatValue(summary.scores?.traditional_score)} />
                <DetailCard label="Semantic Score" value={formatValue(summary.scores?.semantic_score)} />
                <DetailCard label="Issue Count" value={String(summary.issues.length)} />
              </div>

              <div className="section-head section-head-spaced">
                <h2>Top Issues</h2>
              </div>
              <div className="stack-list">
                {summary.issues.length === 0 ? (
                  <p className="empty-state">No issues recorded for this run.</p>
                ) : (
                  summary.issues.slice(0, 5).map((issue, index) => (
                    <article className="stack-item" key={`${issue.dimension || "issue"}-${index}`}>
                      <strong>
                        {issue.dimension || "issue"} · {issue.severity || "unknown"}
                      </strong>
                      <p>{issue.message || "-"}</p>
                    </article>
                  ))
                )}
              </div>

              <div className="section-head section-head-spaced">
                <h2>Suggestions</h2>
              </div>
              <div className="stack-list">
                {summary.suggestions.length === 0 ? (
                  <p className="empty-state">No suggestions available yet.</p>
                ) : (
                  summary.suggestions.slice(0, 5).map((item, index) => (
                    <article className="stack-item" key={`suggestion-${index}`}>
                      <strong>Suggestion {index + 1}</strong>
                      <p>{item}</p>
                    </article>
                  ))
                )}
              </div>

              <div className="section-head section-head-spaced">
                <h2>Raw Detail</h2>
              </div>
              <pre className="json-block">{JSON.stringify({ detail, summary, report }, null, 2)}</pre>
            </>
          )}
        </section>
      </section>
    </main>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="stat-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function DetailCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="detail-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function formatValue(value: number | null | undefined) {
  return value == null ? "-" : String(value);
}
