# Evidence and release provenance

The code and data come from a controlled research project using synthetic records, local Kind, Fluent Bit 5.1.2, and real CloudWatch Logs. Captured application/collector names, Pod UIDs, container IDs, record timestamps and delivery IDs are retained because they establish event transitions and exact delivery. They describe disposable synthetic workloads.

`evidence/provenance.json` records the SHA-256 of each internal source file and exported text. Account identifiers are replaced by `000000000000`, machine-specific home paths are generalized, and caller-identity/QuotaArn fields are omitted from exported cloud preflight records. Values used in statistical comparisons are preserved. Gzip compression is deterministic. The internal originals remain unchanged.

Raw credentials, credential Secrets, TLS private keys, billing reports, employer material, downloaded conference material, CFP notes, and the author-facing article are outside the export file list. The local stub uses dummy credentials; these are not real AWS credentials. No live credential values are archived by the cloud runner.

`MANIFEST.json` lists release files, byte sizes, and SHA-256 checksums. Check it before unpacking. It provides integrity within this package, not an external signature or proof of independent authorship. The scripts re-audit records and regenerate figures from the exported evidence. Graphs use matplotlib; included SVG and PNG versions derive from the same observations.

The runner source saved inside the primary evidence predates the reverse-order option and cleanup hardening in `scripts/identity-cloud.py`. The archived sources record what generated each measurement. The completed repeat `identity-cloud-20260916T011221Z` used the reverse-order option with the same image digests; its evidence and source snapshot are included. One earlier kubectl-PATH preflight failure produced no workload or cloud resources and is excluded.

Fluent Bit, Kubernetes, Python and dependencies are external projects under their own licenses; container binaries are not redistributed here. This project's code, configurations, documentation, graphs and synthetic experiment data are provided under the [Apache License 2.0](LICENSE), unless otherwise noted.
