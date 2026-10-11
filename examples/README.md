# Stream-naming examples

The configurations for the seven-cluster experiment are in [fleet](fleet/README.md). The other files in this directory were used in the [earlier experiments](../archive/2026-09/README.md).

`pod-named.conf`, `reused-streams.conf` and `partition.lua` were used in the Deployment-rollout experiment. The runner's tail path, Kubernetes metadata filter, Lua mount and an existing log group are required. `AWS_REGION` and `CW_LOG_GROUP` must be set, and one output configuration must be selected. Records without Kubernetes metadata are dropped by the test's Lua filter so that missing metadata is detected in the record audit. A fallback for these records would be needed in production.

Collector-plus-file names were also tested. For that CloudWatch comparison, synthetic records were supplied over HTTP with precomputed `high_stream` and `reused_stream` fields. The deployed configurations are provided in `isolated-changing.conf` and `isolated-reused.conf`, with the log group replaced by `${CW_LOG_GROUP}`. These files were used for the HTTP-input benchmark; node log files are not read by them.

Stream names are determined by the output template. A filename or metadata field can change without creating a new destination if that field is omitted from the template. The collector-plus-file template was chosen for comparison and was not an AWS default. Stream counts must also be scoped: 32 distinct names per cluster across 100 clusters can produce 3,200 names.
