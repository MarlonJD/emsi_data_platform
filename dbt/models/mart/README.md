# Product Layer Mart Models

Product Layer marts are API/dashboard-facing views over reviewed Business Vault
semantics. They must hide suppressed rows and must not expose Raw Vault lineage,
hashed actor keys, raw payload JSON, raw content/text, contact values, tokens,
screenshots, request/response bodies, or exact GPS.

## Product Reporting Phase 3

The Phase 3 product-reporting mart slice publishes only reportable aggregate
rows for the first vertical slice:

- `mart_product_reporting_occupation_cohort_daily`
- `mart_product_reporting_content_performance_daily`
- `mart_product_reporting_emoji_reaction_daily`
- `mart_product_reporting_reaction_valence_daily`
- `mart_product_reporting_feed_interest_proxy_daily`
- `mart_product_reporting_contract_coverage`

API callers should preserve `metric_contract_id`, `source_completeness_label`,
`wording_status`, and `reporting_timezone` in responses so UI copy does not
turn proxy or explicit-signal metrics into mood or satisfaction claims.
