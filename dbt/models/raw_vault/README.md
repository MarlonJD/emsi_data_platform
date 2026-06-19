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
