-- Least-privilege read-only role for the Product Reporting PL mart.
--
-- The Go API Product Reporting dashboard reader connects as this role so the
-- API can only read published `analytics_mart` views and never the raw landing,
-- stage, raw_vault, business_vault, or ops schemas. The mart models are
-- definer's-rights views owned by `analytics`, so SELECT on the views is
-- sufficient and the role needs no privilege on the underlying tables.
--
-- This is a LOCAL-STAGING-EQUIVALENT default password and must never be reused
-- outside local dev. Real environments must override the password (and keep the
-- DSN out of the repository). This file is idempotent so it is safe to re-run
-- against an already-initialised volume.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reporting_reader') THEN
    CREATE ROLE reporting_reader LOGIN PASSWORD 'reporting_reader_local_password';
  END IF;
END
$$;

-- Start from no database-level privileges, then grant only CONNECT.
REVOKE ALL ON DATABASE analytics FROM reporting_reader;
GRANT CONNECT ON DATABASE analytics TO reporting_reader;

-- Ensure the published mart schema exists on a fresh init (dbt also manages it).
CREATE SCHEMA IF NOT EXISTS analytics_mart AUTHORIZATION analytics;

-- Read-only access to the published mart schema only.
GRANT USAGE ON SCHEMA analytics_mart TO reporting_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_mart TO reporting_reader;

-- dbt drops and recreates the mart views on every run; default privileges keep
-- the reader's SELECT grant attached to views recreated by the `analytics` owner.
ALTER DEFAULT PRIVILEGES FOR ROLE analytics IN SCHEMA analytics_mart
  GRANT SELECT ON TABLES TO reporting_reader;

-- Defense in depth: never let the reader reach raw landing or intermediate
-- vault/ops schemas even if a future grant is added carelessly.
REVOKE ALL ON SCHEMA analytics FROM reporting_reader;
REVOKE ALL ON ALL TABLES IN SCHEMA analytics FROM reporting_reader;
