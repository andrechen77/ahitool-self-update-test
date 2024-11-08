use clap::Parser;
use subcommands::Subcommand;

mod apis;
mod job_tracker;
mod jobs;
mod subcommands;
mod utils;

#[derive(Parser, Debug)]
struct CliArgs {
    /// The command to perform.
    #[command(subcommand)]
    command: Subcommand,

    /// The JobNimbus API key. This key will be cached.
    #[arg(long, default_value = None, global = true, env)]
    jn_api_key: Option<String>,
}

fn main() -> anyhow::Result<()> {
    // set up tracing
    tracing_subscriber::fmt::init();

    let CliArgs { jn_api_key, command } = CliArgs::parse();

    let jn_api_key = apis::job_nimbus::get_api_key(jn_api_key)?;

    match command {
        Subcommand::Kpi(job_kpi_args) => {
            subcommands::kpi::main(&jn_api_key, job_kpi_args)?;
        }
        Subcommand::Ar(acc_recv_args) => {
            subcommands::acc_receivable::main(&jn_api_key, acc_recv_args)?;
        }
        Subcommand::Update(update_args) => {
            subcommands::update::main(update_args)?;
        }
    }

    Ok(())
}
