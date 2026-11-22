# Contributing to Lakehouse CDC Compaction Benchmark

Contributions and optimizations to the benchmark harness are welcome.

## Areas for Contribution
- **Compaction Algorithms**: Alternative Merge-on-Read heuristics, micro-batch sizing, and partition pruning.
- **Engines**: Extending telemetry benchmarks to Apache Iceberg or Delta Lake standalone readers.
- **Telemetry**: Additional OpenTelemetry metrics or visualization dashboards.

## Submitting Pull Requests
1. Ensure all scripts run cleanly under Docker (`./run_lab.sh` or `.\run_lab.ps1`).
2. Verify that benchmark output metrics remain consistent and reproducible.
3. Open a PR with clear details on performance observations or hardware specifications.
