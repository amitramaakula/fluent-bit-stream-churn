# Choosing destination identity

`pod-named.conf`, `reused-streams.conf`, and `partition.lua` come from the real Deployment-rollout follow-up. They expect the runner's tail path, Kubernetes enrichment, Lua mount and an existing group. Set `AWS_REGION` and `CW_LOG_GROUP`; use one desired output. The demo Lua filter drops records without Kubernetes metadata so missing enrichment fails the record audit. Define a production fallback instead of copying that policy blindly.

The identity experiment additionally compares collector-plus-file names. Its cloud control consumes synthetic records with precomputed `high_stream` and `reused_stream` fields. `isolated-changing.conf` and `isolated-reused.conf` show the exact deployed configuration shape, with the group replaced by `${CW_LOG_GROUP}`. They are HTTP-input benchmark configurations, not production tail-input recommendations.

Trace filename → input tag → enriched fields → output template. A field can change without affecting the destination if the template does not retain it. Collector-plus-file naming is a deliberate comparison scheme, not an assertion about AWS defaults. A bound of 32 must have a defined scope; 32 per cluster across 100 clusters can still mean 3,200 names.
