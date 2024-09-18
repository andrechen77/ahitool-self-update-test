use std::{collections::HashMap, io::Write};

use anyhow::Result;
use chrono::Utc;

use crate::{
    job_nimbus_api,
    jobs::{Job, Status},
};

#[derive(clap::Args, Debug)]
pub struct Args {
    /// The format in which to print the output.
    #[arg(long, value_enum, default_value = "human")]
    format: OutputFormat,

    /// The file to write the output to. "-" will write to stdout.
    #[arg(short, default_value = "-")]
    output: String,
}

#[derive(Debug, clap::ValueEnum, Clone, Copy, Eq, PartialEq)]
enum OutputFormat {
    Human,
    Csv,
}

const CATEGORIES_WE_CARE_ABOUT: &[Status] = &[
    Status::PendingPayments,
    Status::PostInstallSupplementPending,
    Status::JobsInProgress,
    Status::FinalWalkAround,
    Status::SubmitCoc,
    Status::PunchList,
    Status::JobCompleted,
    Status::Collections,
];

struct Results<'a> {
    total: i32,
    categorized_jobs: HashMap<Status, (i32, Vec<&'a Job>)>,
}

pub fn main(api_key: &str, args: Args) -> Result<()> {
    let Args { output, format } = args;

    let jobs = job_nimbus_api::get_all_jobs_from_job_nimbus(&api_key, None)?;

    let mut results = Results { total: 0, categorized_jobs: HashMap::new() };
    for category in CATEGORIES_WE_CARE_ABOUT {
        results.categorized_jobs.insert(category.clone(), (0, Vec::new()));
    }

    for job in &jobs {
        let amt = job.amt_receivable;

        if let Some((category_total, category_jobs)) = results.categorized_jobs.get_mut(&job.status)
        {
            results.total += amt;
            *category_total += amt;
            category_jobs.push(&job);
        }
    }

    let output_writer: Box<dyn Write> = match output.as_str() {
        "-" => Box::new(std::io::stdout()),
        path => Box::new(std::fs::File::create(path)?),
    };

    match format {
        OutputFormat::Human => print_human(&results, output_writer)?,
        OutputFormat::Csv => print_csv(&results, output_writer)?,
    }

    Ok(())
}

fn print_human(results: &Results, mut writer: impl Write) -> std::io::Result<()> {
    let mut zero_amt_jobs = Vec::new();

    writeln!(writer, "Total: ${}", results.total as f64 / 100.0)?;
    for (status, (category_total, jobs)) in &results.categorized_jobs {
        writeln!(writer, "    - {}: total ${}", status, *category_total as f64 / 100.0)?;
        for job in jobs {
            if job.amt_receivable == 0 {
                zero_amt_jobs.push(job);
                continue;
            }

            let name = job.job_name.as_deref().unwrap_or("");
            let number = job.job_number.as_deref().unwrap_or("Unknown Job Number");
            let amount_receivable = job.amt_receivable as f64 / 100.0;
            let days_in_status = Utc::now().signed_duration_since(job.status_mod_date).num_days();
            writeln!(
                writer,
                "        - {} (#{}): ${:.2} ({} days)",
                name, number, amount_receivable, days_in_status
            )?;
        }
    }

    writeln!(writer, "Jobs with $0 receivable:")?;
    for job in zero_amt_jobs {
        let name = job.job_name.as_deref().unwrap_or("");
        let number = job.job_number.as_deref().unwrap_or("Unknown Job Number");
        let days_in_status = Utc::now().signed_duration_since(job.status_mod_date).num_days();
        writeln!(
            writer,
            "    - {} (#{}): ({} for {} days)",
            name, number, job.status, days_in_status
        )?;
    }

    Ok(())
}

fn print_csv(results: &Results, writer: impl Write) -> std::io::Result<()> {
    let mut writer = csv::Writer::from_writer(writer);
    writer
        .write_record(&["Job Name", "Job Number", "Job Status", "Amount", "Days In Status"])
        .unwrap();
    for (_status, (_category_total, jobs)) in &results.categorized_jobs {
        for job in jobs {
            let name = job.job_name.as_deref().unwrap_or("");
            let number = job.job_number.as_deref().unwrap_or("Unknown Job Number");
            let status = format!("{}", job.status);
            let amount_receivable = (job.amt_receivable as f64) / 100.0;
            let days_in_status = Utc::now().signed_duration_since(job.status_mod_date).num_days();
            writer
                .write_record(&[
                    name,
                    number,
                    &status,
                    &amount_receivable.to_string(),
                    &days_in_status.to_string(),
                ])
                .unwrap();
        }
    }
    writer.flush().unwrap();
    Ok(())
}
