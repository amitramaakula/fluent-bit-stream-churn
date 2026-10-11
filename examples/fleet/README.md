# Fleet collector configurations

The three files were extracted from the effective submitted ConfigMaps for the seven-cluster experiment:

- [Pod/container names with default retries](high-default.conf)
- [Pod/container names with unlimited retries](high-unlimited.conf)
- [Reused streams with default retries](reused-default.conf)

The private log-group prefix and trial-specific probe Pod name were replaced with `${CW_LOG_GROUP_PREFIX}` and `${PROBE_POD}`. No other settings were changed. A distinct prefix is required for each cluster and trial, and its node/namespace and fallback groups must be created before collection. Fluent Bit 5.1.3, mounted node log files, Kubernetes API read access, CloudWatch Logs credentials and a writable `/buffers` directory were supplied separately.

The differences between modes are shown as output snippets in the main README. Complete settings are provided in these files. In the reused-stream mode, the Pod/container stream template is replaced by `log_stream_name`; both settings must not be combined.

`Health_Check On` and the HTTP metrics server were enabled explicitly. Filesystem output buffering was not enabled. The tail offset database was stored in a fresh test Pod's `emptyDir`; recovery after a Pod or node failure was not tested. No CPU or memory requests or limits were set on the experiment containers in the submitted Pod specifications.

The naming syntax and retry behavior are described in the upstream [CloudWatch output](https://docs.fluentbit.io/manual/data-pipeline/outputs/cloudwatch) and [scheduling and retries](https://docs.fluentbit.io/manual/administration/scheduling-and-retries) documentation. Pod/container names are an example configuration, not the plugin's automatic naming default.
