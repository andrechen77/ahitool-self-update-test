use std::collections::HashMap;

use anyhow::Result;
use chrono::Utc;

use crate::{
    job_nimbus_api,
    jobs::{Job, Status},
};

#[derive(clap::Args, Debug)]
pub struct Args {
    // none so far
    #[arg(long, value_enum, default_value = "human")]
    format: OutputFormat,
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
];

struct Results<'a> {
    total: i32,
    categorized_jobs: HashMap<Status, (i32, Vec<&'a Job>)>,
}

pub fn main(api_key: &str, args: Args) -> Result<()> {
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

    match args.format {
        OutputFormat::Human => print_human(&results),
        OutputFormat::Csv => print_csv(&results),
    }

    Ok(())
}

fn print_human(results: &Results) {
    println!("Total: ${}", results.total as f64 / 100.0);
    for (status, (category_total, jobs)) in &results.categorized_jobs {
        println!("    - {}: total ${}", status, *category_total as f64 / 100.0);
        for job in jobs {
            let name = job.job_name.as_deref().unwrap_or("");
            let number = job.job_number.as_deref().unwrap_or("Unknown Job Number");
            let amount_receivable = job.amt_receivable as f64 / 100.0;
            let days_in_status = Utc::now().signed_duration_since(job.status_mod_date).num_days();
            println!(
                "        - {} (#{}): ${:.2} ({} days)",
                name, number, amount_receivable, days_in_status
            );
        }
    }
}

fn print_csv(results: &Results) {
    let mut writer = csv::Writer::from_writer(std::io::stdout());
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
}
