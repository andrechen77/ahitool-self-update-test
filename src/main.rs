use anyhow::{bail, Result};
use clap::Parser;

mod acc_receivable;
mod job_nimbus_api;
mod job_tracker;
mod jobs;
mod kpi;

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

#[derive(clap::Subcommand, Debug)]
enum Subcommand {
    /// Generate a KPI report for salesmen based on job milestones.
    Kpi(kpi::Args),
    /// Generate a report for all accounts receivable.
    Ar(acc_receivable::Args),
}

fn main() -> Result<()> {
    let CliArgs { api_key, command } = CliArgs::parse();

    let Some(api_key) = api_key.or(std::env::var("AHI_API_KEY").ok()) else {
        bail!("AHI_API_KEY environment variable not set");
    };

    match command {
        Some(Subcommand::Kpi(job_kpi_args)) => {
            kpi::main(&api_key, job_kpi_args)?;
        }
        Some(Subcommand::Ar(acc_recv_args)) => {
            acc_receivable::main(&api_key, acc_recv_args)?;
        }
        None => bail!("No command specified"),
    }

    Ok(())
}
