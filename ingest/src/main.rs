mod pipeline;
mod stream;
mod transforms;

use clap::{Parser, Subcommand};
use state_search_core::{config::AppConfig, db};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Parser)]
#[command(name = "ingest", about = "State-search CSV ingestion tool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Ingest all fact sources from config/sources.yml, skipping already-processed files.
    Reload,
    /// Ingest files for one named fact source. Optionally specify a single file
    /// (bypasses the already-ingested skip check).
    Run {
        #[arg(long)]
        source: String,
        /// Specific file to ingest (bypasses skip check). If omitted, ingests all
        /// configured files for the source, skipping already-processed ones.
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
        /// Specific file to ingest (bypasses skip check). If omitted, ingests all
        /// configured files for the source, skipping already-processed ones.
        #[arg(long)]
        file: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let cfg = AppConfig::load()?;

    let pool = db::connect(&cfg.database).await?;
    db::migrate(&pool).await?;

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

    Ok(())
}
