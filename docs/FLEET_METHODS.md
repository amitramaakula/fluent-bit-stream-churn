# Fleet evidence and offline reproduction

Run `kubecon-fleet-20261011-0201` contains six seven-cluster comparisons and two single-cluster baselines, completed on 11 October 2026. The source containers emitted synthetic records. Production logs and account inventory are not included.

## Regenerate the checks and graphs

Python 3.11 or later and the listed dependencies are required. No AWS access or cluster is used by these commands, which are run from the repository root:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/verify-package.py
python -m unittest discover -s scripts -p 'test_*.py'
python scripts/analyze-fleet.py
python scripts/analyze-fleet-api.py
```

Audit reports and five PNG/SVG figure pairs are written under `results/kubecon-fleet-20261011-0201`: three for Fluent Bit counters and two for AWS API metrics. The compressed public evidence is read directly. The unpack script is only required by the archived September analyses.

The analysis fails if a required metric is missing, a counter falls, records are duplicated or altered, a stream name is incorrect, or the input/output/drop accounting disagrees with source and backend IDs. All 44 per-cluster comparisons are checked. The six synchronized fleet timestamps and reversed trial order are also checked.

## Counter interpretation

The Prometheus text returned by `/api/v1/metrics/prometheus` is retained in each compressed trial file. Selected input series have `name="tail.0"`; selected output series have `name="cloud"`. The value column is read before any optional sample timestamp. Failed HTTP observations are retained but are not converted to zero samples.

Fresh collector processes, tail databases, memory buffers and output caches were used for every comparison. Raw observations are checked for decreasing counters. Terminal values are then summed across the seven collectors. They are cumulative counts for the finite backlog, not averages, maxima over time or requests per second. Counter snapshots end before the recovery probes, so those records are excluded. Collectors could finish at different times; a common-duration throughput comparison is not implied.

| Counter | Unit and meaning |
|---|---|
| `fluentbit_input_records_total` | Records accepted by the Tail input |
| `fluentbit_output_proc_records_total` | Records successfully processed by the selected output |
| `fluentbit_output_dropped_records_total` | Records discarded by that output route |
| `fluentbit_output_retries_total` | Output chunk retries scheduled |
| `fluentbit_output_retries_failed_total` | Chunks abandoned after the retry limit was reached |
| `fluentbit_output_errors_total` | Output error events; retry exhaustion was reported through the separate counters in this run |

The retry counters cannot be used as CloudWatch API-call counts. Multiple records can be held in a chunk, and API activity can occur within an output attempt. Metric definitions are also available in Fluent Bit's [monitoring documentation](https://docs.fluentbit.io/manual/administration/monitoring).

## AWS API metric interpretation

`aws-api-metrics.json.gz` contains a later read of the original experiment window, from 03:00 to 03:50 UTC on 11 October 2026. It was retrieved at 21:17:41 UTC that day. `GetMetricStatistics` was used with the `AWS/Logs` namespace, `Sum`, a 60-second period, and these dimensions:

| Dimension | Value |
|---|---|
| `Service` | `Logs` |
| `Class` | `None` |
| `Type` | `API` |
| `Resource` | `CreateLogStream` or `PutLogEvents` |

`CallCount` is also available in `AWS/Usage`; overlapping counts from that namespace were not added. See the [AWS metric definitions](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CloudWatch-Logs-Monitoring-CloudWatch-Metrics.html).

Each bar covers the minute beginning at its returned timestamp. An absent minute remains missing, and an empty series is not converted to zeros. No `PutLogEvents` throttling datapoints were returned. The validator rejects duplicate minutes, unexpected dimensions or namespaces, unaligned timestamps, and missing series definitions. Trial timing is reconstructed from the same 44 public exports used by the Fluent Bit analysis.

API metrics are account/region totals. Recovery probes and any concurrent account activity are included. Trial bands end at the last collector's pre-probe terminal sample. A one-minute bin can overlap a trial boundary, and exact per-trial API totals cannot be established from these bins. The plotted sums are sums of returned samples, not estimates for missing minutes. Per-second quota utilization cannot be inferred from these one-minute totals.

The saved collector logs contain 5,653 `CreateLogStream` throttling-response lines across the six fleet trials and two solo baselines. The AWS metric samples sum to 5,654. The source of that one-count difference was not established. Neither count was changed to force agreement. No `PutLogEvents` throttling-response lines were found in these trial logs.

The read-only fetch helper is retained in `scripts/fetch-fleet-api-metrics.py`. It requests the fixed experiment window and needs AWS credentials with metric-read access and `boto3`. The offline plots use the saved response and do not require credentials. Source hashes, retrieval time and selected response fields are recorded in `aws-api-provenance.json.gz`; account IDs and response request IDs are excluded.

## Record and health checks

The backend records are supplied separately from the metric samples. Their IDs and contents are compared with the records captured from the source files. The comparison is repeated against the export taken after eight new recovery records per collector were sent. This check establishes the difference between a missing record and a record that arrived later within the observation period. The probe is a delivery check, not an alerting integration.

Activation-to-ingestion times in the README are derived from backend ingestion timestamps. They are not native Fluent Bit latency metrics, and they are not plotted in the README. HTTP health responses are observations across collectors, not measurements of time spent unhealthy. The successful root response is separate from the explicitly enabled health endpoint.

## Scope and export

The capacity ramp, the 800-record reused-stream pilot and the earlier aborted fleet were excluded from these six comparisons. No production failure rate, live rollout rate, long-running byte-throughput limit or crash-recovery guarantee is established by this finite backlog. The two rounds are repeated observations, not confidence intervals.

Public trial files contain raw metric responses, synthetic source fields, selected backend fields and throttling-response log lines. Infrastructure addresses, service event IDs, billing, credential material and account inventory are excluded. Source-file hashes and the export description are supplied in `evidence/kubecon-fleet-20261011-0201/provenance.json.gz`. The original evidence archive was not changed by export. Image versions and effective collector configurations are recorded in the README and examples.

The public analysis reproduces the measurement checks described above. The completed infrastructure teardown was separately audited through AWS: all 77 checks passed. The live cluster provisioning harness and account-specific ownership state are not installed by this package. The example configurations require an existing disposable Kubernetes environment and suitable AWS access; running them can create billable log traffic.

The earlier executable experiments are documented in the [September archive](../archive/2026-09/README.md). Their setup and measurements differ from this fleet run and must not be pooled with it.
