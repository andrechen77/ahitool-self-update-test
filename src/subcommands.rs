pub mod acc_receivable;
pub mod kpi;
pub mod update;

#[derive(clap::Subcommand, Debug)]
pub enum Subcommand {
    /// Update the executable to the latest version.
    Update(update::Args),
    /// Generate a KPI report for salesmen based on job milestones.
    Kpi(kpi::Args),
    /// Generate a report for all accounts receivable.
    Ar(acc_receivable::Args),
}
