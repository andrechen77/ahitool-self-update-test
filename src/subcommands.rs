pub mod acc_receivable;
pub mod kpi;

#[derive(clap::Subcommand, Debug)]
pub enum Subcommand {
    /// Generate a KPI report for salesmen based on job milestones.
    Kpi(kpi::Args),
    /// Generate a report for all accounts receivable.
    Ar(acc_receivable::Args),
}
