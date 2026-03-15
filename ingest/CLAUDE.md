# Ingest — Claude Instructions

Read `rules.md` before making any changes to this crate. The rules document the design decisions behind the pipeline and the invariants that must be preserved.

## Key Files

| File | Purpose |
|------|---------|
| `src/stream.rs` | `batched_channel_sink` — the reusable producer/consumer abstraction |
| `src/pipeline/mod.rs` | `IngestPipeline::run` and `process_rows` — top-level orchestration |
| `src/pipeline/record.rs` | `process_record` — per-row transform pipeline |
| `src/pipeline/dimensions.rs` | Location/time dimension upserts + location cache |
| `src/pipeline/observations.rs` | Metric extraction and `NewObservation` construction |
| `src/pipeline/row.rs` | CSV row parsing and typed field extractors |
| `src/transforms/` | Field transform chain: coerce, rules, resolve |

## Adding a New Batch Processing Flow

Use `batched_channel_sink` from `crate::stream`. Do not re-implement the channel + `tokio::join!` pattern inline.

```rust
let work = some_stream.map(|item| async move {
    // process item, return Ok(Vec<Output>)
});

crate::stream::batched_channel_sink(
    work,
    CONCURRENCY,
    CHANNEL_CAP,
    BATCH_SIZE,
    |batch| async { flush(batch).await.map_err(Into::into) },
    |total| info!(observations = total, "progress"),
)
.await
```

## Adding a New Transform Rule

1. Add the rule function in `src/transforms/rules/`
2. Register it in `src/transforms/rules/mod.rs`
3. Reference it by name in `sources.toml` under the field's `rules` list

## Adding a New Canonical Field

If the field is a dimension (location or time), add it to the extraction logic in `dimensions.rs` and the skip-set in `observations.rs`. If it's a metric, no change needed — unknown fields fall through to metric extraction automatically.

## What Not to Do

- **Do not await DB writes inside the `.map()` passed to `batched_channel_sink`** — this blocks `buffer_unordered` and serialises the pipeline. Dimension upserts (location, time) are the exception because they produce IDs required to build the output.
- **Do not collect an entire file into a `Vec` before inserting** — memory is unbounded for large files.
- **Do not raise `ROW_CONCURRENCY` above the DB pool size** — connection starvation will occur.
- **Do not insert observations without an `ingest_run_id`** — partial-failure cleanup will not work.
