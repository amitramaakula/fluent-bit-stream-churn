# September 2026 experiment archive

These results precede the seven-cluster comparison. The original experiment description follows. The earlier evidence and scripts remain at their original paths.

[Current experiment](../../README.md)

# When restarts rename your logs

Reproducible Fluent Bit experiments connecting Kubernetes lifecycle events to log-stream creation demand and delivery delay. This repository contains synthetic workload code, measured evidence, graphs, and a configuration-review checklist.

The strongest result is configuration-dependent: restarting a container can change its filename while preserving its Pod name. Replacing a collector can rename all destinations combining its identity with source filenames. Three containers per Pod produced three times as many such name changes at the same record count.

Across two isolated real CloudWatch runs, 100 new streams/second produced 20,586–20,955 stream-creation throttling responses versus zero with 32 reused names. P95 first-ingestion delay was 81.798–84.763 versus 0.626–0.752 seconds, 99.1–99.3% lower with reuse in paired comparisons. Counts include each one-minute send window and drain. All 10,500 records per design per run arrived without duplicates. The repeat reversed collector startup and per-record endpoint order, preserving concurrent designs and the demand stages. These are observed results, not a production SLA or measured hundred-cluster deployment.

## Start with the evidence: no AWS account needed

Run these commands from the repository root using Linux or Ubuntu through WSL, Python 3.11 or later, and a virtual environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/verify-package.py
python scripts/unpack-evidence.py
python -m unittest discover -s scripts -p 'test_*.py'
python scripts/analyze-identity.py lifecycle identity-20260915T020624Z
python scripts/analyze-identity.py cloud identity-cloud-20260915T022435Z
python scripts/analyze-identity.py cloud identity-cloud-20260916T011221Z
python scripts/compare-identity.py identity-cloud-20260916T011221Z
```

The first command checks hashes before compressed evidence is expanded. The analyses regenerate the included PNG and SVG graphs and audit record IDs, payloads, destinations, lifecycle transitions, and delivery times. Gzip contains ordinary text files, not serialized executable objects. Archived evidence is separate from runnable scripts; `runner-source.py` in an evidence folder preserves the version used for that measurement.

- [Measured results and limitations](RESULTS.md)
- [Configuration and event matrix](../../figures/identity-matrix.png)
- [Creation throttling and delivery delay](../../figures/demand-comparison.png)
- [Original versus reversed-order comparison](../../figures/repeat-comparison.png)
- [Diagnostic checklist](../../docs/DIAGNOSTICS.md)
- [Configuration examples](../../examples/README.md)
- [Evidence provenance](../../PROVENANCE.md)

## Run the real Kubernetes identity experiment

Docker, Kind, and kubectl must already be installed. The experiment expects an otherwise disposable cluster named `cwlogs-identity`; it does not use your current Kubernetes context. Infrastructure setup is separate from installing or retrying the workload:

```bash
kind create cluster --name cwlogs-identity --image kindest/node:v1.36.1 --wait 120s
docker exec cwlogs-identity-control-plane crictl pull cr.fluentbit.io/fluent/fluent-bit:5.1.2
docker exec cwlogs-identity-control-plane crictl pull python:3.13.2-slim-bookworm
python scripts/identity-lifecycle.py
```

The runner prints its timestamped run directory under `results/`. Analyze that ID with `python scripts/analyze-identity.py lifecycle RUN_ID`. It tails actual CRI files, enriches records from Kubernetes, and compares five naming schemes across controlled application/collector events. It computes output names without calling AWS. It deliberately stops application and collector containers in its own test namespace. No log rotation or repeated crash-loop sequence is tested. Tail offsets persist on the disposable node, and every phase drains before the next.

## Run the bounded cloud control

Use a disposable AWS account with no competing production logging workload: the test deliberately exceeds its shared creation quota. Install a current AWS CLI and authenticate with short-lived credentials using your usual login or SSO workflow. The SDK must be able to refresh that profile. Pass `AWS_PROFILE` when needed. Do not put credentials in this repository.

The caller needs STS identity lookup, Service Quotas list access, CloudWatch Logs group creation/tagging/retention/deletion, stream creation, writes, event reads, and group/tag listing. Independent API metrics additionally require `cloudwatch:GetMetricStatistics`. No IAM policy is installed automatically. The cloud runner verifies the expected account and an applied CreateLogStream quota of 50/s, uses `us-west-2`, and refuses an existing `identity-cloud` namespace.

```bash
python scripts/identity-cloud.py --account YOUR_EXPECTED_ACCOUNT_ID --identity-run YOUR_LOCAL_IDENTITY_RUN_ID --isolated
# Repeat only after the previous run and its cleanup have completed:
python scripts/identity-cloud.py --account YOUR_EXPECTED_ACCOUNT_ID --identity-run YOUR_LOCAL_IDENTITY_RUN_ID --isolated --reverse-order
```

Each command sends 10,500 records to each of two independent Fluent Bit collectors, with 30/60/100 new changing names per second for 30/60/60 seconds. The source filenames come from the local mapping. Synthetic collector generations emulate destination changes; collectors remain running. Both designs run concurrently. The reverse option reverses collector startup and per-record HTTP endpoint order, preserving the stage sequence and warm-up conditions. It is not a sequential A/B test or a reversal of the demand stages.

Each stage allows up to 300 seconds to drain. The output uses eight workers, a 128 MiB input memory-buffer limit, and unlimited output retries within the runner's bounded lifetime. This tests naming/API behavior, not durable recovery after a crash. A single precreated log group isolates stream creation from group creation; it receives one-day retention and ownership tags. No EKS, EC2, NAT gateways, IAM roles, or alarms are created. Ingestion, storage, reads and any applicable account charges remain billable; no exact cost cap is promised.

After the runner exits, inspect `cleanup.json` and `failure.txt` if present. Analyze successful runs with `python scripts/analyze-identity.py cloud RUN_ID`. Delete the owned local infrastructure when finished:

```bash
kind delete cluster --name cwlogs-identity
kind get clusters
python scripts/identity-cloud-metrics.py YOUR_CLOUD_RUN_ID
```

The final read-only script checks group absence and retrieves account/region API metrics. Those metrics may arrive later; missing datapoints are not proof of zero. Do not repeatedly rerun the load solely to wait for metrics.

## Interrupted runs and cleanup

The runner exports evidence and separately attempts log-group and namespace deletion in `finally`. It requires matching ownership tags before deleting the exact group. A process or host killed abruptly cannot run cleanup. Use the saved `ownership.json` to identify the exact group and cluster, inspect its `Experiment` tag, and remove only those resources. `python scripts/cleanup-owned.py RUN_ID --account YOUR_EXPECTED_ACCOUNT_ID` verifies these markers before attempting cleanup; cluster deletion remains an explicit separate command. One-day log retention expires events, not the group itself.

## Retry demonstration

With Docker and OpenSSL available, run `python scripts/retry-demo.py --no-throttle` and then `python scripts/retry-demo.py`. A loopback TLS stub and dummy credentials exercise real Fluent Bit without AWS. One injected late creation failure can replay earlier accepted records in a mixed-destination batch. This conditional mechanism does not imply duplicates in the isolated cloud control, which observed none.

## License

Copyright 2026 fluent-bit-stream-churn contributors.

The project's code, configurations, documentation, graphs and synthetic experiment data are licensed under the [Apache License 2.0](../../LICENSE), unless otherwise noted. External tools and dependencies retain their own licenses and are not redistributed as part of this repository.
