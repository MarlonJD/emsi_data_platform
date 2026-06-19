from dagster import Definitions, asset


@asset(group_name="platform_baseline")
def platform_baseline_decisions() -> dict[str, str]:
    return {
        "postgres": "baseline: postgres:18.4-alpine3.24",
        "python": "baseline: python:3.12.13-slim-bookworm",
        "dbt": "baseline: dbt-core==1.11.11 and dbt-postgres==1.10.1",
        "data_vault": "baseline: ScalefreeCOM/datavault4dbt==1.18.3",
        "soda": "local-dev: soda-core==4.14.0 and soda-postgres==4.14.0",
    }


defs = Definitions(assets=[platform_baseline_decisions])
