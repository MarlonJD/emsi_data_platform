CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.data_quality_runs (
  data_quality_run_id TEXT PRIMARY KEY,
  pipeline_run_id TEXT,
  tool TEXT NOT NULL,
  contract_name TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  window_start TIMESTAMPTZ,
  window_end TIMESTAMPTZ,
  status TEXT NOT NULL CHECK (status IN ('passed', 'warning', 'failed', 'error')),
  critical_count INTEGER NOT NULL DEFAULT 0 CHECK (critical_count >= 0),
  warning_count INTEGER NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ NOT NULL,
  raw_result_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS data_quality_runs_contract_finished_idx
  ON ops.data_quality_runs (contract_name, finished_at DESC);

CREATE INDEX IF NOT EXISTS data_quality_runs_status_finished_idx
  ON ops.data_quality_runs (status, finished_at DESC);

CREATE TABLE IF NOT EXISTS ops.data_quality_findings (
  data_quality_finding_id TEXT PRIMARY KEY,
  data_quality_run_id TEXT NOT NULL REFERENCES ops.data_quality_runs (data_quality_run_id) ON DELETE CASCADE,
  severity TEXT NOT NULL CHECK (severity IN ('critical', 'warning', 'info')),
  dimension TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  check_name TEXT NOT NULL,
  message TEXT NOT NULL,
  sample_count BIGINT,
  owner TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('open', 'acknowledged', 'resolved', 'wont_fix')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS data_quality_findings_run_idx
  ON ops.data_quality_findings (data_quality_run_id);

CREATE INDEX IF NOT EXISTS data_quality_findings_open_idx
  ON ops.data_quality_findings (status, severity, created_at DESC)
  WHERE status IN ('open', 'acknowledged');
