#![forbid(unsafe_code)]

use ketebe_core::{
    CollectionId, DistanceMetric, FieldPath, Metadata, MetadataValue, Predicate, Record, RecordId,
    SequenceNumber, Vector,
};
use ketebe_server::{AppState, CollectionService, PendingRecord, RuntimeCatalog, WriteService};
use ketebe_storage::{
    Checkpoint, DEFAULT_RRF_K, ExecutionPreference, HnswConfig, HnswHit, HnswIndex, HnswIndexStore,
    HnswLoadResult, HybridHit, HybridOptions, LexicalIndex, LexicalQuery, QueryRequest, SearchHit,
    Segment, SegmentId, SegmentStore, WalMutation, exact_search_filtered_segments,
    exact_search_segments, execute_hybrid_query_with_index_and_options, hnsw_search_filtered,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
struct Profile {
    name: &'static str,
    seed: u64,
    dimension: usize,
    records: usize,
    queries: usize,
    top_k: usize,
    batch_size: usize,
    minimum_recall: f64,
}

impl Profile {
    fn ci() -> Self {
        Self {
            name: "ci",
            seed: 0x4b45_5445_4245,
            dimension: 16,
            records: 256,
            queries: 12,
            top_k: 10,
            batch_size: 32,
            minimum_recall: 0.80,
        }
    }

    fn local() -> Self {
        Self {
            name: "local",
            seed: 0x4b45_5445_4245,
            dimension: 64,
            records: 10_000,
            queries: 200,
            top_k: 10,
            batch_size: 256,
            minimum_recall: 0.80,
        }
    }
}

#[derive(Serialize)]
struct Report {
    metadata: RunMetadata,
    search: SearchMetrics,
    hybrid: HybridMetrics,
    writes: WriteMetrics,
    lifecycle: LifecycleMetrics,
    hnsw_restart: HnswRestartMetrics,
}

#[derive(Serialize)]
struct RunMetadata {
    ketebe_version: String,
    profile: String,
    seed: u64,
    dimension: usize,
    record_count: usize,
    query_count: usize,
    top_k: usize,
    metric: String,
    batch_size: usize,
    hnsw: HnswConfigReport,
}

#[derive(Serialize)]
struct HnswConfigReport {
    m: usize,
    ef_construction: usize,
    ef_search: usize,
}

#[derive(Serialize)]
struct SearchMetrics {
    mean_recall_at_k: f64,
    minimum_recall_at_k: f64,
    exact: LatencyReport,
    hnsw: LatencyReport,
    filtered_exact: LatencyReport,
    filtered_hnsw: LatencyReport,
}

#[derive(Serialize)]
struct HybridMetrics {
    baseline: HybridVariantMetrics,
    expanded: HybridVariantMetrics,
    result_change_rate: f64,
}

#[derive(Serialize)]
struct HybridVariantMetrics {
    dense_k: usize,
    lexical_k: usize,
    rrf_k: u32,
    mean_selected_precision_at_k: f64,
    latency: LatencyReport,
}

#[derive(Serialize)]
struct LatencyReport {
    operations: usize,
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    mean_us: f64,
    ops_per_second: f64,
}

#[derive(Serialize)]
struct WriteMetrics {
    single: ThroughputReport,
    batch: ThroughputReport,
}

#[derive(Serialize)]
struct ThroughputReport {
    operations: usize,
    elapsed_ms: f64,
    operations_per_second: f64,
}

#[derive(Serialize)]
struct HnswRestartMetrics {
    rebuild_ms: f64,
    native_restore_ms: f64,
    speedup: f64,
    result_equivalent: bool,
}

#[derive(Serialize)]
struct LifecycleMetrics {
    seal_ms: f64,
    recovery_ms: f64,
    recovery_result_equivalent: bool,
    recovered_live_records: usize,
}

struct Dataset {
    segment: Segment,
    queries: Vec<Vec<f32>>,
    predicate: Predicate,
}

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn next_f32(&mut self) -> f32 {
        let value = (self.next_u64() >> 40) as u32;
        (value as f32 / ((1_u32 << 24) - 1) as f32) * 2.0 - 1.0
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("ketebe-bench failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (profile, json_path) = parse_args()?;
    let metric = DistanceMetric::L2;
    let config = HnswConfig::default();
    let collection = CollectionId::new("benchmark")?;
    let dataset = generate_dataset(profile, &collection)?;
    let segments = vec![dataset.segment];
    let hnsw = HnswIndex::build(&segments, &collection, metric, config)?;
    let search = measure_search(
        profile,
        &collection,
        &segments,
        &hnsw,
        metric,
        &dataset.queries,
        &dataset.predicate,
    )?;
    if search.minimum_recall_at_k < profile.minimum_recall {
        return Err(format!(
            "recall regression: minimum recall@{} {:.3} < {:.3}",
            profile.top_k, search.minimum_recall_at_k, profile.minimum_recall
        )
        .into());
    }
    let hybrid = measure_hybrid(profile, &collection, &segments, metric, &dataset.queries)?;
    let hnsw_restart = measure_hnsw_restart(
        profile,
        &collection,
        &segments,
        metric,
        config,
        &dataset.queries[0],
    )?;
    if !hnsw_restart.result_equivalent || hnsw_restart.native_restore_ms >= hnsw_restart.rebuild_ms
    {
        return Err(format!("native HNSW restore regression: rebuild_ms={:.3} native_restore_ms={:.3} equivalent={}", hnsw_restart.rebuild_ms, hnsw_restart.native_restore_ms, hnsw_restart.result_equivalent).into());
    }

    let data_dir = temp_dir();
    let (writes, lifecycle) = measure_writes_and_lifecycle(profile, &data_dir, metric).await?;
    let report = Report {
        metadata: RunMetadata {
            ketebe_version: ketebe_core::build_info().version.to_string(),
            profile: profile.name.to_string(),
            seed: profile.seed,
            dimension: profile.dimension,
            record_count: profile.records,
            query_count: profile.queries,
            top_k: profile.top_k,
            metric: "l2".to_string(),
            batch_size: profile.batch_size,
            hnsw: HnswConfigReport {
                m: config.m,
                ef_construction: config.ef_construction,
                ef_search: config.ef_search,
            },
        },
        search,
        hybrid,
        writes,
        lifecycle,
        hnsw_restart,
    };
    print_summary(&report);
    let json = serde_json::to_string_pretty(&report)?;
    if let Some(path) = json_path {
        fs::write(&path, format!("{json}\n"))?;
        println!("json_report={}", path.display());
    } else {
        println!("{json}");
    }
    let _ = fs::remove_dir_all(data_dir);
    Ok(())
}

fn parse_args() -> Result<(Profile, Option<PathBuf>), Box<dyn std::error::Error>> {
    let mut profile = Profile::ci();
    let mut json_path = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                profile = match args
                    .next()
                    .ok_or("--profile requires ci or local")?
                    .as_str()
                {
                    "ci" => Profile::ci(),
                    "local" => Profile::local(),
                    value => return Err(format!("unknown profile: {value}").into()),
                };
            }
            "--json" => {
                json_path = Some(PathBuf::from(args.next().ok_or("--json requires a path")?));
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run -p ketebe-bench -- --profile <ci|local> [--json <path>]"
                );
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    Ok((profile, json_path))
}

fn generate_dataset(
    profile: Profile,
    collection: &CollectionId,
) -> Result<Dataset, Box<dyn std::error::Error>> {
    let mut rng = Lcg::new(profile.seed);
    let mut mutations = Vec::with_capacity(profile.records);
    for index in 0..profile.records {
        let mut metadata = Metadata::new();
        metadata.insert(
            "bucket".to_string(),
            MetadataValue::String(if index % 4 == 0 { "selected" } else { "other" }.to_string()),
        );
        mutations.push(WalMutation::Upsert {
            collection_id: collection.clone(),
            record: Record::new(
                RecordId::unsigned(index as u64 + 1),
                Vector::new(random_vector(&mut rng, profile.dimension))?,
                metadata,
                SequenceNumber::new(index as u64 + 1),
            ),
        });
    }
    let segment = Segment::from_mutations(SegmentId::new(1), &mutations)?;
    let queries = (0..profile.queries)
        .map(|_| random_vector(&mut rng, profile.dimension))
        .collect();
    let predicate = Predicate::Eq(
        FieldPath::new(["bucket"])?,
        MetadataValue::String("selected".to_string()),
    );
    Ok(Dataset {
        segment,
        queries,
        predicate,
    })
}

fn random_vector(rng: &mut Lcg, dimension: usize) -> Vec<f32> {
    (0..dimension).map(|_| rng.next_f32()).collect()
}

fn measure_hnsw_restart(
    profile: Profile,
    collection: &CollectionId,
    segments: &[Segment],
    metric: DistanceMetric,
    config: HnswConfig,
    query: &[f32],
) -> Result<HnswRestartMetrics, Box<dyn std::error::Error>> {
    let dir = temp_dir();
    fs::create_dir_all(&dir)?;
    let checkpoint = Checkpoint::new(
        collection.clone(),
        segments.iter().map(Segment::id).collect(),
        SequenceNumber::new(profile.records as u64),
    );
    let store = HnswIndexStore::open(&dir)?;
    let start = Instant::now();
    let rebuilt = store.rebuild_and_publish(&checkpoint, metric, config, segments)?;
    let rebuild_ms = start.elapsed().as_secs_f64() * 1000.0;
    let start = Instant::now();
    let restored = match store.load(&checkpoint, metric, config)? {
        HnswLoadResult::Loaded(index) => index,
        other => return Err(format!("expected native HNSW restore, got {other:?}").into()),
    };
    let native_restore_ms = start.elapsed().as_secs_f64() * 1000.0;
    let result_equivalent =
        rebuilt.search(query, profile.top_k)? == restored.search(query, profile.top_k)?;
    let speedup = if native_restore_ms > 0.0 {
        rebuild_ms / native_restore_ms
    } else {
        f64::INFINITY
    };
    let _ = fs::remove_dir_all(dir);
    Ok(HnswRestartMetrics {
        rebuild_ms,
        native_restore_ms,
        speedup,
        result_equivalent,
    })
}

fn measure_search(
    profile: Profile,
    collection: &CollectionId,
    segments: &[Segment],
    hnsw: &HnswIndex,
    metric: DistanceMetric,
    queries: &[Vec<f32>],
    predicate: &Predicate,
) -> Result<SearchMetrics, Box<dyn std::error::Error>> {
    let mut exact_times = Vec::with_capacity(queries.len());
    let mut hnsw_times = Vec::with_capacity(queries.len());
    let mut filtered_exact_times = Vec::with_capacity(queries.len());
    let mut filtered_hnsw_times = Vec::with_capacity(queries.len());
    let mut recalls = Vec::with_capacity(queries.len());

    for query in queries {
        let start = Instant::now();
        let exact = exact_search_segments(segments, collection, query, metric, profile.top_k)?;
        exact_times.push(start.elapsed());

        let start = Instant::now();
        let approximate = hnsw.search(query, profile.top_k)?;
        hnsw_times.push(start.elapsed());
        recalls.push(recall_by_id(&exact, &approximate));

        let start = Instant::now();
        let exact_filtered = exact_search_filtered_segments(
            segments,
            collection,
            query,
            metric,
            profile.top_k,
            predicate,
        )?;
        filtered_exact_times.push(start.elapsed());

        let start = Instant::now();
        let hnsw_filtered = hnsw_search_filtered(hnsw, query, profile.top_k, predicate)?;
        filtered_hnsw_times.push(start.elapsed());
        if exact_filtered.len() > profile.top_k || hnsw_filtered.len() > profile.top_k {
            return Err("filtered search returned more than top_k results".into());
        }
    }

    Ok(SearchMetrics {
        mean_recall_at_k: recalls.iter().sum::<f64>() / recalls.len() as f64,
        minimum_recall_at_k: recalls.iter().copied().fold(1.0_f64, f64::min),
        exact: latency_report(&exact_times),
        hnsw: latency_report(&hnsw_times),
        filtered_exact: latency_report(&filtered_exact_times),
        filtered_hnsw: latency_report(&filtered_hnsw_times),
    })
}

fn measure_hybrid(
    profile: Profile,
    collection: &CollectionId,
    segments: &[Segment],
    metric: DistanceMetric,
    queries: &[Vec<f32>],
) -> Result<HybridMetrics, Box<dyn std::error::Error>> {
    let field = FieldPath::new(["bucket"])?;
    let lexical_query = LexicalQuery::new("selected", vec![field.clone()])?;
    let lexical_index = LexicalIndex::build(
        segments,
        collection,
        vec![field],
        lexical_query.analyzer(),
        0,
    )?;
    let expanded_k = profile
        .top_k
        .saturating_mul(4)
        .min(profile.records)
        .max(profile.top_k);
    let baseline_options =
        HybridOptions::new(profile.top_k, profile.top_k, profile.top_k, DEFAULT_RRF_K)?;
    let expanded_options =
        HybridOptions::new(profile.top_k, expanded_k, expanded_k, DEFAULT_RRF_K)?;
    let mut baseline_times = Vec::with_capacity(queries.len());
    let mut expanded_times = Vec::with_capacity(queries.len());
    let mut baseline_precision = Vec::with_capacity(queries.len());
    let mut expanded_precision = Vec::with_capacity(queries.len());
    let mut changed = 0_usize;

    for query in queries {
        let dense = QueryRequest::new(collection.clone(), query.clone(), metric, profile.top_k)
            .with_preference(ExecutionPreference::Exact);
        let start = Instant::now();
        let baseline = execute_hybrid_query_with_index_and_options(
            &dense,
            &lexical_query,
            &lexical_index,
            segments,
            None,
            baseline_options,
        )?;
        baseline_times.push(start.elapsed());
        let start = Instant::now();
        let expanded = execute_hybrid_query_with_index_and_options(
            &dense,
            &lexical_query,
            &lexical_index,
            segments,
            None,
            expanded_options,
        )?;
        expanded_times.push(start.elapsed());
        baseline_precision.push(selected_precision(baseline.hits()));
        expanded_precision.push(selected_precision(expanded.hits()));
        let baseline_ids = baseline
            .hits()
            .iter()
            .map(|hit| hit.record().id())
            .collect::<Vec<_>>();
        let expanded_ids = expanded
            .hits()
            .iter()
            .map(|hit| hit.record().id())
            .collect::<Vec<_>>();
        if baseline_ids != expanded_ids {
            changed += 1;
        }
    }

    Ok(HybridMetrics {
        baseline: HybridVariantMetrics {
            dense_k: baseline_options.dense_k,
            lexical_k: baseline_options.lexical_k,
            rrf_k: baseline_options.rrf_k,
            mean_selected_precision_at_k: mean(&baseline_precision),
            latency: latency_report(&baseline_times),
        },
        expanded: HybridVariantMetrics {
            dense_k: expanded_options.dense_k,
            lexical_k: expanded_options.lexical_k,
            rrf_k: expanded_options.rrf_k,
            mean_selected_precision_at_k: mean(&expanded_precision),
            latency: latency_report(&expanded_times),
        },
        result_change_rate: changed as f64 / queries.len() as f64,
    })
}

fn selected_precision(hits: &[HybridHit]) -> f64 {
    if hits.is_empty() {
        return 1.0;
    }
    let selected = hits
        .iter()
        .filter(|hit| {
            matches!(
                hit.record().metadata().get("bucket"),
                Some(MetadataValue::String(value)) if value == "selected"
            )
        })
        .count();
    selected as f64 / hits.len() as f64
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn recall_by_id(exact: &[SearchHit], approximate: &[HnswHit]) -> f64 {
    if exact.is_empty() {
        return 1.0;
    }
    let expected: BTreeSet<RecordId> = exact.iter().map(|hit| hit.record().id().clone()).collect();
    let matched = approximate
        .iter()
        .filter(|hit| expected.contains(hit.record().id()))
        .count();
    matched as f64 / expected.len() as f64
}

fn latency_report(values: &[Duration]) -> LatencyReport {
    let mut micros: Vec<u128> = values.iter().map(Duration::as_micros).collect();
    micros.sort_unstable();
    let total_seconds = values.iter().map(Duration::as_secs_f64).sum::<f64>();
    LatencyReport {
        operations: micros.len(),
        p50_us: percentile(&micros, 50),
        p95_us: percentile(&micros, 95),
        p99_us: percentile(&micros, 99),
        mean_us: micros.iter().sum::<u128>() as f64 / micros.len() as f64,
        ops_per_second: if total_seconds > 0.0 {
            micros.len() as f64 / total_seconds
        } else {
            0.0
        },
    }
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    values[((values.len() - 1) * percentile).div_ceil(100)]
}

async fn measure_writes_and_lifecycle(
    profile: Profile,
    data_dir: &Path,
    metric: DistanceMetric,
) -> Result<(WriteMetrics, LifecycleMetrics), Box<dyn std::error::Error>> {
    let threshold = profile.records.saturating_mul(4).max(1);
    let state = AppState::with_data_dir_and_threshold(
        RuntimeCatalog::empty_ready(),
        data_dir.to_path_buf(),
        threshold,
    );
    let writes = WriteService::new(state.clone());
    let single_id = CollectionId::new("bench-single")?;
    writes
        .create_collection(single_id.clone(), profile.dimension, metric, Vec::new())
        .await?;

    let single_count = profile
        .records
        .min(if profile.name == "ci" { 64 } else { 1_000 });
    let start = Instant::now();
    for index in 0..single_count {
        writes
            .upsert(&single_id, pending(index as u64 + 1, profile.dimension))
            .await?;
    }
    let single = throughput_report(single_count, start.elapsed());

    let batch_id = CollectionId::new("bench-batch")?;
    writes
        .create_collection(batch_id.clone(), profile.dimension, metric, Vec::new())
        .await?;
    let start = Instant::now();
    let mut next_id = 1_u64;
    while next_id <= profile.records as u64 {
        let count = (profile.records as u64 - next_id + 1).min(profile.batch_size as u64);
        let batch = (0..count)
            .map(|offset| pending(next_id + offset, profile.dimension))
            .collect();
        if writes.upsert_batch(&batch_id, batch).await?.len() != count as usize {
            return Err("batch sequence count mismatch".into());
        }
        next_id += count;
    }
    let batch = throughput_report(profile.records, start.elapsed());

    let seal_start = Instant::now();
    writes.seal_collection(&batch_id).await?;
    let seal_elapsed = seal_start.elapsed();

    let segment_path = data_dir.join("collections/bench-batch/segments");
    let probe = deterministic_vector(7, profile.dimension);
    let before_segments = SegmentStore::open(&segment_path)?.discover()?;
    let before = exact_search_segments(&before_segments, &batch_id, &probe, metric, profile.top_k)?;
    let before_ids: Vec<RecordId> = before.iter().map(|hit| hit.record().id().clone()).collect();
    let before_info = CollectionService::new(state.clone()).get(&batch_id).await?;
    drop(writes);
    drop(state);

    let recovery_start = Instant::now();
    let recovered = AppState::recover_with_threshold(data_dir, threshold)?;
    let recovery_elapsed = recovery_start.elapsed();
    let after_info = CollectionService::new(recovered).get(&batch_id).await?;
    let after_segments = SegmentStore::open(&segment_path)?.discover()?;
    let after = exact_search_segments(&after_segments, &batch_id, &probe, metric, profile.top_k)?;
    let after_ids: Vec<RecordId> = after.iter().map(|hit| hit.record().id().clone()).collect();
    let equivalent = before_ids == after_ids
        && before_info.live_records == after_info.live_records
        && after_info.live_records == profile.records;
    if !equivalent {
        return Err("recovery correctness check failed".into());
    }

    Ok((
        WriteMetrics { single, batch },
        LifecycleMetrics {
            seal_ms: seal_elapsed.as_secs_f64() * 1_000.0,
            recovery_ms: recovery_elapsed.as_secs_f64() * 1_000.0,
            recovery_result_equivalent: true,
            recovered_live_records: after_info.live_records,
        },
    ))
}

fn pending(id: u64, dimension: usize) -> PendingRecord {
    PendingRecord {
        id: RecordId::unsigned(id),
        vector: deterministic_vector(id, dimension),
        metadata: Metadata::new(),
    }
}

fn deterministic_vector(id: u64, dimension: usize) -> Vec<f32> {
    let mut rng = Lcg::new(0x9e37_79b9_7f4a_7c15 ^ id);
    random_vector(&mut rng, dimension)
}

fn throughput_report(operations: usize, elapsed: Duration) -> ThroughputReport {
    let seconds = elapsed.as_secs_f64();
    ThroughputReport {
        operations,
        elapsed_ms: seconds * 1_000.0,
        operations_per_second: if seconds > 0.0 {
            operations as f64 / seconds
        } else {
            0.0
        },
    }
}

fn temp_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch")
        .as_nanos();
    env::temp_dir().join(format!("ketebe-bench-{nonce}"))
}

fn print_summary(report: &Report) {
    println!("Ketebe benchmark profile: {}", report.metadata.profile);
    println!(
        "dataset: records={} dimension={} queries={} top_k={} seed={}",
        report.metadata.record_count,
        report.metadata.dimension,
        report.metadata.query_count,
        report.metadata.top_k,
        report.metadata.seed
    );
    println!(
        "recall@k: mean={:.3} min={:.3}",
        report.search.mean_recall_at_k, report.search.minimum_recall_at_k
    );
    print_latency("exact", &report.search.exact);
    print_latency("hnsw", &report.search.hnsw);
    print_latency("filtered_exact", &report.search.filtered_exact);
    print_latency("filtered_hnsw", &report.search.filtered_hnsw);
    println!(
        "hybrid baseline: dense_k={} lexical_k={} selected_precision@k={:.3}",
        report.hybrid.baseline.dense_k,
        report.hybrid.baseline.lexical_k,
        report.hybrid.baseline.mean_selected_precision_at_k
    );
    print_latency("hybrid_baseline", &report.hybrid.baseline.latency);
    println!(
        "hybrid expanded: dense_k={} lexical_k={} selected_precision@k={:.3} result_change_rate={:.3}",
        report.hybrid.expanded.dense_k,
        report.hybrid.expanded.lexical_k,
        report.hybrid.expanded.mean_selected_precision_at_k,
        report.hybrid.result_change_rate
    );
    print_latency("hybrid_expanded", &report.hybrid.expanded.latency);
    println!(
        "single_write: {:.1} ops/s | batch_write: {:.1} ops/s",
        report.writes.single.operations_per_second, report.writes.batch.operations_per_second
    );
    println!(
        "seal={:.2} ms recovery={:.2} ms recovery_equivalent={}",
        report.lifecycle.seal_ms,
        report.lifecycle.recovery_ms,
        report.lifecycle.recovery_result_equivalent
    );
}

fn print_latency(name: &str, report: &LatencyReport) {
    println!(
        "{name}: p50={}us p95={}us p99={}us mean={:.1}us",
        report.p50_us, report.p95_us, report.p99_us, report.mean_us
    );
}
