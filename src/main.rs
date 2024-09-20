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

    /// The JobNimbus API key. This key will be cached.
    #[arg(long, default_value = None, global = true)]
    jn_api_key: Option<String>,
}

fn main() -> Result<()> {
    let CliArgs { jn_api_key: api_key, command } = CliArgs::parse();

    let jn_api_key = apis::job_nimbus::get_api_key(api_key)?;

    match command {
        Some(Subcommand::Kpi(job_kpi_args)) => {
            subcommands::kpi::main(&jn_api_key, job_kpi_args)?;
        }
        Some(Subcommand::Ar(acc_recv_args)) => {
            subcommands::acc_receivable::main(&jn_api_key, acc_recv_args)?;
        }
        None => bail!("No command specified"),
    }

    Ok(())
}
