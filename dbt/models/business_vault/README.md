# Business Vault Models

Business Vault models encode reviewed metric semantics over Raw Vault lineage.
They are not Product Layer marts and are not direct dashboard/API outputs.

## Product Reporting Phase 2

The Phase 2 product-reporting views add the first semantic layer for
occupation cohorts, content performance, emoji usage, reaction valence, and feed
interest proxy reporting:

- `pit_reporting_content_daily`
- `br_content_reaction_daily`
- `br_feed_interest_proxy`
- `s_occupation_cohort_daily`
- `s_content_performance_daily`
- `s_emoji_usage_daily`
- `s_reaction_valence_daily`
- `product_reporting_bdv_contract_coverage`

The views use only bounded ids, cohort keys, emoji keys, reviewed valence
labels, event counts, hashed actor keys, and source-completeness labels. They do
not expose raw content/text, raw payload JSON, DM content, voice transcripts,
raw notes, contact/reveal values, request or response bodies, tokens,
screenshots, or exact GPS.
