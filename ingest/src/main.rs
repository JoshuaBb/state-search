mod pipeline;
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
    /// Ingest all sources from config/sources.toml, skipping already-processed files.
    Reload,
    /// Ingest files for one named source from config. Optionally specify a single file
    /// (bypasses the already-ingested skip check).
    Run {
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
                    let count = pipeline.run(source, file).await?;
                    if count > 0 {
                        println!("{}: {} observations inserted from {}", source.name, count, file);
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
                    "source '{}' not found in config — add it to config/sources.toml",
                    source_name
                ))?;

            let pipeline = pipeline::IngestPipeline::new(&pool);

            match file {
                Some(path) => {
                    // --file provided: bypass skip check by calling pipeline directly
                    let count = pipeline.run(source, &path).await?;
                    println!("inserted {count} observations");
                }
                None => {
                    if source.files.is_empty() {
                        info!(source = source_name, "source has no files configured, nothing to do");
                        return Ok(());
                    }
                    for file in &source.files {
                        let count = pipeline.run(source, file).await?;
                        if count > 0 {
                            println!("{}: {} observations inserted from {}", source.name, count, file);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
