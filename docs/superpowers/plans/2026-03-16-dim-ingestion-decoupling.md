# Dim Ingestion Decoupling Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple dimension table ingestion from fact ingestion via a dedicated `dims.yml` config, two new CLI commands, and separate Docker build targets.

**Architecture:** `DimIngestConfig` is added to `AppConfig` and populated from `config/dims.yml` using the same YAML contract as `sources.yml`. Two new CLI commands (`reload-dims`, `run-dim`) exclusively call the dim pipeline, while `reload` and `run` become fact-only. The Dockerfile is split into lean `api` and `ingest` build targets consumed by three separate compose services.

**Tech Stack:** Rust (serde_yaml, config crate, clap), Dockerfile multi-stage builds, Docker Compose v2

**Spec:** `docs/superpowers/specs/2026-03-16-dim-ingestion-decoupling-design.md`

---

## Chunk 1: Config — `DimIngestConfig` + `dims.yml`

### Files
- Modify: `core/src/config.rs`
- Create: `config/dims.yml`

---

### Task 1: Add `DimIngestConfig` and `validate_dim_sources` to `core/src/config.rs`

**Files:**
- Modify: `core/src/config.rs`

- [ ] **Step 1: Write three failing tests**

Add the following tests to the `#[cfg(test)]` block at the bottom of `core/src/config.rs` (before the final `}`):

```rust
#[test]
fn validate_dim_sources_passes_when_all_have_target() {
    let sc: SourceConfig = serde_yaml::from_str(
        "name: locs\ntarget: dim_location\nfields: {}\nderived: {}\n"
    ).unwrap();
    assert!(validate_dim_sources(&[sc]).is_ok());
}

#[test]
fn validate_dim_sources_passes_for_empty_slice() {
    assert!(validate_dim_sources(&[]).is_ok());
}

#[test]
fn validate_dim_sources_fails_when_target_missing() {
    let sc: SourceConfig = serde_yaml::from_str(
        "name: no_target\nfields: {}\nderived: {}\n"
    ).unwrap();
    let err = validate_dim_sources(&[sc]).unwrap_err();
    assert!(err.to_string().contains("no_target"), "error was: {err}");
}
```

- [ ] **Step 2: Run tests and verify they fail to compile**

```bash
cargo test -p state-search-core 2>&1 | head -20
```

Expected: compile error — `validate_dim_sources` not found.

- [ ] **Step 3: Add `DimIngestConfig` struct and `validate_dim_sources` function**

In `core/src/config.rs`, add the following after the `IngestConfig` struct (around line 47):

```rust
#[derive(Debug, Deserialize, Default, Clone)]
pub struct DimIngestConfig {
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

pub(crate) fn validate_dim_sources(
    sources: &[SourceConfig],
) -> std::result::Result<(), config::ConfigError> {
    for source in sources {
        if source.target.is_none() {
            return Err(config::ConfigError::Message(format!(
                "dims.yml entry '{}' is missing required 'target' field",
                source.name
            )));
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Run tests and verify they pass**

```bash
cargo test -p state-search-core validate_dim_sources 2>&1
```

Expected: all three tests PASS.

- [ ] **Step 5: Add `dims` field to `AppConfig`**

In `core/src/config.rs`, update the `AppConfig` struct:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    #[serde(default)]
    pub server:   ServerConfig,
    #[serde(default)]
    pub ingest:   IngestConfig,
    #[serde(default)]
    pub dims:     DimIngestConfig,
}
```

- [ ] **Step 6: Add `dims.yml` loading to `AppConfig::load()`**

At the end of the `load()` method, after the existing `sources.yml` block (after line ~154), add:

```rust
        let dims_path = "config/dims.yml";
        if let Ok(content) = std::fs::read_to_string(dims_path) {
            #[derive(Deserialize)]
            struct DimsFile {
                #[serde(default)]
                sources: Vec<SourceConfig>,
            }
            let dims: DimsFile = serde_yaml::from_str(&content).map_err(|e| {
                config::ConfigError::Message(format!("failed to parse {dims_path}: {e}"))
            })?;
            validate_dim_sources(&dims.sources)?;
            app_config.dims.sources = dims.sources;
        }
```

- [ ] **Step 7: Run full core test suite**

```bash
cargo test -p state-search-core 2>&1
```

Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add core/src/config.rs
git commit -m "feat(core/config): add DimIngestConfig, validate_dim_sources, and dims.yml load path"
```

---

### Task 2: Create `config/dims.yml`

**Files:**
- Create: `config/dims.yml`
- (No changes to `config/sources.yml` — it currently contains no dim entries)

- [ ] **Step 1: Create `config/dims.yml`**

Create the file with an empty sources list and a comment explaining its purpose:

```yaml
# config/dims.yml
# Dimension table ingestion sources.
# Same YAML contract as sources.yml; 'target' is required for every entry.
# Run with: cargo run -p state-search-ingest -- reload-dims
sources: []
```

- [ ] **Step 2: Verify it loads cleanly**

```bash
cargo build -p state-search-ingest 2>&1
```

Expected: compiles with no errors.

- [ ] **Step 3: Commit**

```bash
git add config/dims.yml
git commit -m "config: add empty dims.yml for dimension source definitions"
```

---

## Chunk 2: CLI — `reload-dims` and `run-dim` commands

### Files
- Modify: `ingest/src/main.rs`

---

### Task 3: Add dim commands and make existing commands fact-only

**Files:**
- Modify: `ingest/src/main.rs`

- [ ] **Step 1: Delete the stale test module**

Remove the entire `#[cfg(test)] mod tests { ... }` block at the bottom of `ingest/src/main.rs` (lines 132–172). These tests verify the old `target`-based branching behavior which is being removed.

- [ ] **Step 2: Add `ReloadDims` and `RunDim` to the `Command` enum**

Replace the current `Command` enum with:

```rust
#[derive(Subcommand)]
enum Command {
    /// Ingest all fact sources from config/sources.yml, skipping already-processed files.
    Reload,
    /// Ingest files for one named fact source. Optionally specify a single file
    /// (bypasses the already-ingested skip check).
    Run {
        #[arg(long)]
        source: String,
        #[arg(long)]
        file: Option<String>,
    },
    /// Ingest all dim sources from config/dims.yml, skipping already-processed files.
    ReloadDims,
    /// Ingest files for one named dim source. Optionally specify a single file
    /// (bypasses the already-ingested skip check).
    RunDim {
        #[arg(long)]
        source: String,
        #[arg(long)]
        file: Option<String>,
    },
}
```

- [ ] **Step 3: Replace the `match cli.command` block**

Replace the entire `match cli.command { ... }` block with:

```rust
    match cli.command {
        Command::Reload => {
            let sources = &cfg.ingest.sources;
            if sources.is_empty() {
                info!("no sources configured, nothing to do");
                return Ok(());
            }
            let pipeline = pipeline::IngestPipeline::new(&pool);
            for source in sources {
                for file in &source.files {
                    if pipeline.already_ingested(file).await? {
                        info!(source = source.name, file, "skipping (already ingested)");
                        continue;
                    }
                    let count = pipeline.run(source, file).await?;
                    if count > 0 {
                        println!("{}: {} rows inserted from {}", source.name, count, file);
                    }
                }
            }
        }

        Command::Run { source: source_name, file } => {
            let source = cfg
                .ingest
                .sources
                .iter()
                .find(|s| s.name == source_name)
                .ok_or_else(|| anyhow::anyhow!(
                    "source '{}' not found in config/sources.yml",
                    source_name
                ))?;
            let pipeline = pipeline::IngestPipeline::new(&pool);
            match file {
                Some(path) => {
                    let count = pipeline.run(source, &path).await?;
                    println!("inserted {count} rows");
                }
                None => {
                    if source.files.is_empty() {
                        info!(source = source_name, "source has no files configured, nothing to do");
                        return Ok(());
                    }
                    for file in &source.files {
                        if pipeline.already_ingested(file).await? {
                            info!(source = source_name, file, "skipping (already ingested)");
                            continue;
                        }
                        let count = pipeline.run(source, file).await?;
                        if count > 0 {
                            println!("{}: {} rows inserted from {}", source.name, count, file);
                        }
                    }
                }
            }
        }

        Command::ReloadDims => {
            let sources = &cfg.dims.sources;
            if sources.is_empty() {
                info!("no dim sources configured, nothing to do");
                return Ok(());
            }
            let pipeline = pipeline::IngestPipeline::new(&pool);
            for source in sources {
                for file in &source.files {
                    if pipeline.dim_already_ingested(&source.name, file).await? {
                        info!(source = source.name, file, "skipping (already ingested)");
                        continue;
                    }
                    let count = pipeline.run_dim(source, file).await?;
                    if count > 0 {
                        println!("{}: {} rows inserted from {}", source.name, count, file);
                    }
                }
            }
        }

        Command::RunDim { source: source_name, file } => {
            let source = cfg
                .dims
                .sources
                .iter()
                .find(|s| s.name == source_name)
                .ok_or_else(|| anyhow::anyhow!(
                    "source '{}' not found in config/dims.yml",
                    source_name
                ))?;
            let pipeline = pipeline::IngestPipeline::new(&pool);
            match file {
                Some(path) => {
                    let count = pipeline.run_dim(source, &path).await?;
                    println!("inserted {count} rows");
                }
                None => {
                    if source.files.is_empty() {
                        info!(source = source_name, "source has no files configured, nothing to do");
                        return Ok(());
                    }
                    for file in &source.files {
                        if pipeline.dim_already_ingested(&source.name, file).await? {
                            info!(source = source_name, file, "skipping (already ingested)");
                            continue;
                        }
                        let count = pipeline.run_dim(source, file).await?;
                        if count > 0 {
                            println!("{}: {} rows inserted from {}", source.name, count, file);
                        }
                    }
                }
            }
        }
    }
```

- [ ] **Step 4: Build to verify compilation**

```bash
cargo build -p state-search-ingest 2>&1
```

Expected: compiles with no errors or warnings about unused imports.

- [ ] **Step 5: Verify help output lists all four commands**

```bash
cargo run -p state-search-ingest -- --help 2>&1
```

Expected output includes: `reload`, `run`, `reload-dims`, `run-dim`.

- [ ] **Step 6: Run full test suite**

```bash
cargo test 2>&1
```

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add ingest/src/main.rs
git commit -m "feat(ingest): add reload-dims/run-dim commands; make reload/run fact-only"
```

---

## Chunk 3: Docker — split build targets and compose services

### Files
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`

---

### Task 4: Split `Dockerfile` into `api` and `ingest` runtime stages

**Files:**
- Modify: `Dockerfile`

- [ ] **Step 1: Replace the single `runtime` stage with two named stages**

Replace everything from `# ── Stage 4: Minimal runtime image` to the end of the file with:

```dockerfile
# ── Stage 4a: API runtime ─────────────────────────────────────────────────────
FROM debian:bookworm-slim AS api

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=rust-builder /app/target/release/api /app/api
COPY --from=web-builder  /app/web/dist           /app/web/dist
COPY config/                                      /app/config/

ENV APP__SERVER__HOST=0.0.0.0
ENV APP__SERVER__PORT=3000

EXPOSE 3000

CMD ["/app/api"]

# ── Stage 4b: Ingest runtime ──────────────────────────────────────────────────
FROM debian:bookworm-slim AS ingest

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=rust-builder /app/target/release/ingest /app/ingest
COPY config/                                         /app/config/

ENTRYPOINT ["/app/ingest"]
```

Note: `APP__SERVER__HOST` uses double-underscore (`APP__{SECTION}__{KEY}`), fixing the pre-existing single-underscore bug from the old stage.

- [ ] **Step 2: Verify both targets build**

```bash
docker build --target api -t state-search-api . 2>&1 | tail -5
docker build --target ingest -t state-search-ingest . 2>&1 | tail -5
```

Expected: both complete with `Successfully built ...` (or equivalent BuildKit output).

- [ ] **Step 3: Verify API image does NOT contain ingest binary**

```bash
docker run --rm state-search-api ls /app
```

Expected: `api  config  web` — no `ingest` binary.

- [ ] **Step 4: Verify ingest image does NOT contain api binary or web dist**

```bash
docker run --rm state-search-ingest ls /app
```

Expected: `config  ingest` — no `api` binary, no `web` directory.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile
git commit -m "build: split Dockerfile into api and ingest runtime stages"
```

---

### Task 5: Update `docker-compose.yml`

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Replace the `ingest` and `api` services**

Replace the current `ingest` and `api` service definitions with:

```yaml
  ingest-dims:
    build:
      context: .
      target: ingest
    command: ["reload-dims"]
    environment:
      APP__DATABASE__URL: postgres://postgres:postgres@db:5432/state_search
    depends_on:
      db:
        condition: service_healthy
    volumes:
      - ./data:/app/data
      - ./config:/app/config

  ingest:
    build:
      context: .
      target: ingest
    command: ["reload"]
    environment:
      APP__DATABASE__URL: postgres://postgres:postgres@db:5432/state_search
    depends_on:
      db:
        condition: service_healthy
      ingest-dims:
        condition: service_completed_successfully
    volumes:
      - ./data:/app/data
      - ./config:/app/config
      - ./exports:/app/exports

  api:
    build:
      context: .
      target: api
    restart: unless-stopped
    ports:
      - "3000:3000"
    environment:
      APP__DATABASE__URL: postgres://postgres:postgres@db:5432/state_search
    depends_on:
      db:
        condition: service_healthy
      ingest:
        condition: service_completed_successfully
```

Note: `entrypoint:` is removed from `ingest` services — it is now baked into the `ingest` image stage. `restart: unless-stopped` is only on `api` since ingest services are one-shot.

- [ ] **Step 2: Verify compose config is valid**

```bash
docker compose config 2>&1 | head -40
```

Expected: no errors, prints resolved compose config.

- [ ] **Step 3: Verify service dependency graph**

```bash
docker compose config --services 2>&1
```

Expected: `api`, `db`, `ingest`, `ingest-dims` all listed.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "build: add ingest-dims service; use build targets; wire dim-first execution order"
```

---

## Summary

| Task | Files | Commit message |
|------|-------|----------------|
| 1 | `core/src/config.rs` | `feat(core/config): add DimIngestConfig, validate_dim_sources, and dims.yml load path` |
| 2 | `config/dims.yml` | `config: add empty dims.yml for dimension source definitions` |
| 3 | `ingest/src/main.rs` | `feat(ingest): add reload-dims/run-dim commands; make reload/run fact-only` |
| 4 | `Dockerfile` | `build: split Dockerfile into api and ingest runtime stages` |
| 5 | `docker-compose.yml` | `build: add ingest-dims service; use build targets; wire dim-first execution order` |
