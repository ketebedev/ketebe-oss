# Benchmark & Correctness Harness

Ketebe ships a reproducible single-node benchmark harness in the `ketebe-bench` workspace crate.

## CI correctness profile

```bash
cargo run -p ketebe-bench -- --profile ci --json /tmp/ketebe-bench.json
```

This profile is intentionally small enough for normal CI. It uses a fixed seed and checks correctness invariants including ANN recall against exact search. It records latency and throughput, but **does not fail CI on absolute timing values** because GitHub-hosted runner performance is not a stable benchmark environment.

## Local benchmark profile

```bash
cargo run --release -p ketebe-bench -- --profile local --json ketebe-bench.json
```

The local profile uses a larger deterministic workload and is intended for developer workstations or dedicated benchmark hosts. For meaningful before/after comparisons, use the same machine, build mode, CPU policy, profile, seed and Ketebe revision.

## Measurements

The report includes:

- exact-search p50/p95/p99 latency
- HNSW p50/p95/p99 latency
- filtered exact-search latency
- filtered HNSW latency
- mean and minimum recall@k, computed by `RecordId` overlap against exact results
- single durable upsert throughput
- batch durable upsert throughput
- seal/checkpoint duration
- restart recovery duration
- post-recovery result/stat equivalence

## Reproducibility metadata

Every JSON report records:

- Ketebe version
- profile
- deterministic dataset seed
- vector dimension
- record count
- query count
- `top_k`
- distance metric
- HNSW `m`, `ef_construction`, and `ef_search`
- batch size

## Correctness policy

Exact search is the ANN ground-truth oracle. The CI profile enforces a deliberately non-brittle minimum recall threshold, while latency and throughput remain observational metrics until Ketebe has dedicated, controlled benchmark hosts.

The harness is not intended for external competitor comparisons, distributed benchmarking, soak testing, or machine-independent latency SLO enforcement.
