use anyhow::Result;

use crate::job_nimbus_api;

#[derive(clap::Args, Debug)]
pub struct Args {
    // none so far
}

pub fn main(api_key: &str, _args: Args) -> Result<()> {
    let jobs = job_nimbus_api::get_all_jobs_from_job_nimbus(&api_key, None)?;

    let mut total = 0;
    for job in jobs {
        let amt = job.amt_receivable;
        total += amt;
        if amt > 0 {
            println!(
                "job #{} owes ${}\n\thttps://app.jobnimbus.com/job/{}",
                job.job_number.as_deref().unwrap_or("unknown"),
                job.amt_receivable as f64 / 100.0,
                job.jnid,
            );
        }
    }

    println!("Total: ${}", total as f64 / 100.0);

    Ok(())
}
