use clap::Parser;
use sync::cli::Cli;

#[tokio::main]
async fn main() -> sync::SyncResult<()> {
    sync::logging::init();
    let cli = Cli::parse();
    cli.run().await?;
    Ok(())
}
