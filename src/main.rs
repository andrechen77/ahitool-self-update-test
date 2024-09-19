use std::path::Path;

use anyhow::{bail, Result};
use clap::Parser;
use subcommands::Subcommand;

mod apis;
mod job_tracker;
mod jobs;
mod subcommands;

#[derive(Parser, Debug)]
struct CliArgs {
    /// The command to perform.
    #[command(subcommand)]
    command: Option<Subcommand>,

    /// The JobNimbus API key. If omitted, the AHI_API_KEY environment variable
    /// will be used.
    #[arg(long, default_value = None)]
    api_key: Option<String>,
}

fn main() -> Result<()> {
    let CliArgs { api_key, command } = CliArgs::parse();

    let Some(api_key) = api_key.or(std::env::var("AHI_API_KEY").ok()) else {
        bail!("AHI_API_KEY environment variable not set");
    };

    match command {
        Some(Subcommand::Kpi(job_kpi_args)) => {
            subcommands::kpi::main(&api_key, job_kpi_args)?;
        }
        Some(Subcommand::Ar(acc_recv_args)) => {
            subcommands::acc_receivable::main(&api_key, acc_recv_args)?;
        }
        Some(Subcommand::CreateGoogleSheet) => {
            use apis::google_sheets;
            let creds = google_sheets::get_credentials()?;
            let url = google_sheets::create_sheet(&creds, "AAAAA sheet")?;
            println!("created spreadsheet at {}", url);
        }
        None => bail!("No command specified"),
    }

    Ok(())
}
