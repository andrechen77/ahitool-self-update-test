use std::collections::HashMap;

use anyhow::Result;

use crate::{
    job_nimbus_api,
    jobs::{Job, Status},
};

#[derive(clap::Args, Debug)]
pub struct Args {
    // none so far
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

pub fn main(api_key: &str, _args: Args) -> Result<()> {
    let jobs = job_nimbus_api::get_all_jobs_from_job_nimbus(&api_key, None)?;

    let mut total = 0;
    let mut categorized_jobs: HashMap<Status, (i32, Vec<&Job>)> = HashMap::new();
    for category in CATEGORIES_WE_CARE_ABOUT {
        categorized_jobs.insert(category.clone(), (0, Vec::new()));
    }

    for job in &jobs {
        let amt = job.amt_receivable;

        if let Some((category_total, category_jobs)) = categorized_jobs.get_mut(&job.status) {
            total += amt;
            *category_total += amt;
            category_jobs.push(&job);
        }
    }

    println!("Total: ${}", total as f64 / 100.0);
    for (status, (category_total, jobs)) in categorized_jobs {
        println!("    - {}: total ${}", status, category_total as f64 / 100.0);
        for job in jobs {
            let name = job.job_name.as_deref().unwrap_or("");
            let number = job.job_number.as_deref().unwrap_or("Unknown Job Number");
            let amount_receivable = job.amt_receivable as f64 / 100.0;
            println!("        - {} (#{}): ${:.2}", name, number, amount_receivable);
        }
    }

    Ok(())
}
