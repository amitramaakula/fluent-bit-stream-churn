# Fleet collector configurations

The three files were extracted from the effective submitted ConfigMaps for the seven-cluster experiment:

- [Pod/container names with default retries](high-default.conf)
- [Pod/container names with unlimited retries](high-unlimited.conf)
- [Reused streams with default retries](reused-default.conf)

Only the private log-group prefix and trial-specific probe Pod name were replaced with `${CW_LOG_GROUP_PREFIX}` and `${PROBE_POD}`. The prefix must be distinct for each cluster and trial. Its node/namespace and fallback groups must be created before collection. The configurations were used with Fluent Bit 5.1.3, mounted node log files, Kubernetes API read access, CloudWatch Logs credentials and a writable `/buffers` directory. They do not create those dependencies.

The output snippets in the main README describe differences between modes. For a complete configuration, one of these files is required. The reused-stream mode replaces the Pod/container stream template; both stream settings must not be combined.

`Health_Check On` and the HTTP metrics server were enabled explicitly. Filesystem output buffering was not enabled. The tail offset database was stored in a fresh test Pod's `emptyDir`; it was not a durability test. CPU and memory requests and limits were absent from the effective experiment Pod specifications.

The naming syntax and retry behavior are described in the upstream [CloudWatch output](https://docs.fluentbit.io/manual/data-pipeline/outputs/cloudwatch) and [scheduling and retries](https://docs.fluentbit.io/manual/administration/scheduling-and-retries) documentation. Pod/container names are an example configuration, not the plugin's automatic naming default.
