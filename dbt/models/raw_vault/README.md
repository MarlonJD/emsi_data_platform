# Raw Vault Smoke Models

These local-dev views prove the first EMSI analytics event Data Vault shape over
`analytics.raw_event_landing`.

- `hub_analytics_event` uses `event_id` as the business key and `event_hk` as
  the hashed hub key.
- `sat_analytics_event_payload` carries hashes and bounded event metadata plus
  `event_hashdiff` as the satellite hashdiff. It does not project raw `subject`
  or `payload` JSON.

They are smoke evidence only. Production Raw Vault acceptance still requires
source-bound data windows, owner-approved event taxonomy, retention decisions,
and privacy/legal review.

## Product Reporting Phase 1

The Phase 1 product-reporting Raw Vault views extend the event-envelope vault
with content, reaction, and feed-serving shaped hubs, links, and satellites:

- `hub_reporting_content`
- `hub_reporting_reaction`
- `hub_reporting_feed_item`
- `link_reporting_reaction_content`
- `link_reporting_feed_item_content`
- `sat_reporting_content_event`
- `sat_reporting_reaction_event`
- `sat_reporting_feed_serving_event`
- `product_reporting_stage_reconciliation`

These views are source-faithful projections over accepted analytics landing
rows. They prepare occupation-cohort, content-performance, emoji, reaction
valence, and feed-interest source-completeness lineage, but they do not claim
app operational source completeness until explicit app tables are connected in a
later source slice.
