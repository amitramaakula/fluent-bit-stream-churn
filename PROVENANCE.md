# Evidence and release provenance

## October 2026 fleet experiment

The main README describes Fluent Bit 5.1.3 on seven real EKS 1.36 clusters. Compressed evidence is stored under `evidence/kubecon-fleet-20261011-0201`. Each of the 44 trial exports contains raw Prometheus-format responses, the pre-probe terminal response, synthetic source records, selected backend record fields, health responses and stream-creation/write throttling log lines. The 800-record pilot and capacity-ramp samples are excluded from this set.

`provenance.json.gz` in that folder records original source-file hashes. Exported backend fields are restricted to the synthetic record, stream name and ingestion timestamp. Account inventory, infrastructure addresses, service event IDs, billing and credentials are excluded. The configurations under `examples/fleet` were extracted from effective submitted ConfigMaps; the private group prefix and probe Pod name were replaced with environment variables. Source records and counter values were not changed. The sealed internal archive was not modified.

`scripts/analyze-fleet.py` reconstructs all six fleet totals and two c01 baselines from the public data. Its checks include counter monotonicity, the optional Prometheus timestamp column, source/backend content and ID matches, destination selection, recovery-probe separation and trial order. The three current figures use native Fluent Bit counters only. The production of those figures does not require AWS access.

## September 2026 archive

The code and data come from a controlled research project using synthetic records, local Kind, Fluent Bit 5.1.2, and real CloudWatch Logs. Captured application/collector names, Pod UIDs, container IDs, record timestamps and delivery IDs are retained because they establish event transitions and exact delivery. They describe disposable synthetic workloads.

`evidence/provenance.json` records the SHA-256 of each internal source file and exported text. Account identifiers are replaced by `000000000000`, machine-specific home paths are generalized, and caller-identity/QuotaArn fields are omitted from exported cloud preflight records. Values used in statistical comparisons are preserved. Gzip compression is deterministic. The internal originals remain unchanged.

Raw credentials, credential Secrets, TLS private keys, billing reports, employer material, downloaded conference material, CFP notes, and the author-facing article are outside the export file list. The local stub uses dummy credentials; these are not real AWS credentials. No live credential values are archived by the cloud runner.

`MANIFEST.json` lists release files, byte sizes, and SHA-256 checksums. Check it before unpacking. It provides integrity within this package, not an external signature or proof of independent authorship. The scripts re-audit records and regenerate figures from the exported evidence. Graphs use matplotlib; included SVG and PNG versions derive from the same observations.

The runner source saved inside the primary evidence predates the reverse-order option and cleanup hardening in `scripts/identity-cloud.py`. The archived sources record what generated each measurement. The completed repeat `identity-cloud-20260916T011221Z` used the reverse-order option with the same image digests; its evidence and source snapshot are included. One earlier kubectl-PATH preflight failure produced no workload or cloud resources and is excluded.

Fluent Bit, Kubernetes, Python and dependencies are external projects under their own licenses; container binaries are not redistributed here. This project's code, configurations, documentation, graphs and synthetic experiment data are provided under the [Apache License 2.0](LICENSE), unless otherwise noted.
