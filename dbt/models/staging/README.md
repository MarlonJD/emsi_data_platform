# Staging Models

Stage models normalize source records for Data Vault loading. Stage tables are
reproducible load layers, not reporting sources and not durable production
truth.

## Product Reporting Phase 1

The product-reporting Phase 1 staging views are bounded projections over
`analytics.raw_event_landing`:

- `stg_product_reporting_content_events`
- `stg_product_reporting_reactions`
- `stg_product_reporting_feed_events`

They extract only stable ids, timestamps, enums, bounded occupation cohort keys,
bounded emoji keys, reaction/interest valence labels, bounded feed metadata,
hashes, privacy class, consent scope, and source lineage needed by the Phase 0
product reporting contract. They do not expose raw `payload` JSON, raw
content/text, DM content, voice transcripts, raw notes, contact/reveal values,
request or response bodies, tokens, screenshots, or exact GPS.
