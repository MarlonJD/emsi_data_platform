CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.analytics_platform_smoke (
  id INTEGER PRIMARY KEY,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL
);

INSERT INTO analytics.analytics_platform_smoke (id, status)
VALUES (1, 'local-dev')
ON CONFLICT (id) DO UPDATE SET
  loaded_at = EXCLUDED.loaded_at,
  status = EXCLUDED.status;
