# EMSI Data Platform

Private EMSI analytics and data-platform workspace.

This repository is intended to hold data-platform code and governance assets:

- Dagster orchestration assets and jobs.
- dbt or SQL transformation models.
- Data quality checks and production-readiness report generators.
- Manifest schemas, run metadata, and evidence-id conventions.
- Deployment and local-development tooling for the analytics platform.

Do not commit raw production data, personal data, large exports, model artifacts,
or generated dashboard/evidence dumps. Keep source-of-truth data in Postgres,
object storage, model registries, or other approved runtime systems; keep only
code, manifests, schemas, and non-sensitive metadata in git.
