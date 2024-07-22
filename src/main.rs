use std::collections::HashMap;

use anyhow::{bail, Result};
use job_tracker::{CalcStatsResult, JobTracker};
use jobs::{AnalyzedJob, Job, JobAnalysisError, JobKind, Milestone, TimeDelta};
use reqwest::{self, header::CONTENT_TYPE};
use serde::Deserialize;

mod job_tracker;
mod jobs;

const ENDPOINT_JOBS: &str = "https://app.jobnimbus.com/api1/jobs";

fn main() -> Result<()> {
    let Ok(api_key) = std::env::var("AHI_API_KEY") else {
        bail!("AHI_API_KEY environment variable not set");
    };
    let jobs = get_all_jobs(&api_key)?;

    let ProcessJobsResult { global_tracker, rep_specific_trackers, invalid_jobs } =
        process_jobs(jobs.into_iter());

    println!("\nGlobal Tracker: ================");
    println!("{}", format_job_tracker_results(&global_tracker));
    for (rep, tracker) in rep_specific_trackers {
        println!(
            "\nTracker for {}: =================",
            rep.unwrap_or("Unknown Sales Rep".to_owned())
        );
        println!("{}", format_job_tracker_results(&tracker));
    }
    for (rep, jobs) in invalid_jobs {
        println!(
            "\nInvalid jobs for {}: ===============",
            rep.unwrap_or("Unknown Sales Rep".to_owned())
        );
        for (job, err) in jobs {
            println!("{}: {}", job.job_number.as_deref().unwrap_or("unknown job #"), err);
        }
    }

    Ok(())
}

// blocking
fn get_all_jobs(api_key: &str) -> Result<Vec<Job>> {
    use serde_json::Value;
    #[derive(Deserialize)]
    struct ApiResponse {
        count: u64,
        results: Vec<Value>,
    }

    println!("getting all jobs from JobNimbus");

    let url = reqwest::Url::parse(ENDPOINT_JOBS)?;
    let client = reqwest::blocking::Client::new();

    // make a request to find out the number of jobs
    let response = client
        .get(url.clone())
        .bearer_auth(&api_key)
        .header(CONTENT_TYPE, "application/json")
        .query(&[("size", 1)])
        .send()?;
    if !response.status().is_success() {
        bail!("Request failed with status code: {}", response.status());
    }
    let response: ApiResponse = response.json()?;
    let count = response.count;
    println!("detected {} jobs in JobNimbus", count);

    // make a request to actually get those jobs
    let response = client
        .get(url)
        .bearer_auth(&api_key)
        .header(CONTENT_TYPE, "application/json")
        .query(&[("size", count)])
        .send()?;
    let response: ApiResponse = response.json()?;
    println!("recieved {} jobs from JobNimbus", response.count);
    assert_eq!(response.count, count);

    let results: Result<Vec<_>, _> = response.results.into_iter().map(Job::try_from).collect();
    Ok(results?)
}

struct ProcessJobsResult {
    global_tracker: JobTracker3x5,
    rep_specific_trackers: HashMap<Option<String>, JobTracker3x5>,
    invalid_jobs: HashMap<Option<String>, Vec<(Job, JobAnalysisError)>>,
}
fn process_jobs(jobs: impl Iterator<Item = Job>) -> ProcessJobsResult {
    let mut global_tracker = build_job_tracker();
    let mut rep_specific_trackers = HashMap::new();
    let mut invalid_jobs = HashMap::new();
    for job in jobs {
        match AnalyzedJob::try_from(job) {
            Ok(analyzed) => {
                // only add settled jobs to the trackers
                if analyzed.is_settled() {
                    let kind = analyzed.kind.into_int();
                    global_tracker.add_job(kind, &analyzed.timestamps, analyzed.loss_timestamp);
                    rep_specific_trackers
                        .entry(analyzed.job.sales_rep.clone())
                        .or_insert_with(build_job_tracker)
                        .add_job(kind, &analyzed.timestamps, analyzed.loss_timestamp);
                }
            }
            Err((job, err)) => {
                invalid_jobs.entry(job.sales_rep.clone()).or_insert_with(Vec::new).push((job, err));
            }
        }
    }

    ProcessJobsResult { global_tracker, rep_specific_trackers, invalid_jobs }
}

type JobTracker3x5 = JobTracker<{ JobKind::NUM_VARIANTS }, { Milestone::NUM_VARIANTS }>;

fn build_job_tracker() -> JobTracker3x5 {
    JobTracker::new([
        [true, true, true, true, true],
        [true, true, false, true, true],
        [true, true, false, true, true],
    ])
}

fn format_job_tracker_results(tracker: &JobTracker3x5) -> String {
    let iwc = JobKind::InsuranceWithContingency.into_int(); // "insurance with contingency"
    let iwo = JobKind::InsuranceWithoutContingency.into_int(); // "insurance without contingency"
    let ret = JobKind::Retail.into_int(); // "retail"

    let num_appts = tracker.calc_stats(Milestone::AppointmentMade.into_int(), &[iwc, iwo, ret]).num_total;
    let num_installs = tracker.calc_stats(Milestone::Installed.into_int(), &[iwc, iwo, ret]).num_total;
    let (num_losses, avg_loss_time) = tracker.calc_stats_of_loss();
    let CalcStatsResult {
        num_total: continge_total,
        conversion_rate: continge_conv,
        average_time_to_achieve: continge_time,
    } = tracker.calc_stats(Milestone::ContingencySigned.into_int(), &[iwc]);
    let CalcStatsResult {
        num_total: contract_insurance_total,
        conversion_rate: contract_insurance_conv,
        average_time_to_achieve: contract_insurance_time,
    } = tracker.calc_stats(Milestone::ContractSigned.into_int(), &[iwc, iwo]);
    let CalcStatsResult {
        num_total: contract_retail_total,
        conversion_rate: contract_retail_conv,
        average_time_to_achieve: contract_retail_time,
    } = tracker.calc_stats(Milestone::ContractSigned.into_int(), &[ret]);
    let CalcStatsResult {
        num_total: install_insurance_total,
        conversion_rate: install_insurance_conv,
        average_time_to_achieve: install_insurance_time,
    } = tracker.calc_stats(Milestone::Installed.into_int(), &[iwc, iwo]);
    let CalcStatsResult {
        num_total: install_retail_total,
        conversion_rate: install_retail_conv,
        average_time_to_achieve: install_retail_time,
    } = tracker.calc_stats(Milestone::Installed.into_int(), &[ret]);

    fn into_days(time: TimeDelta) -> f64 {
        const SECONDS_PER_DAY: f64 = 86400.0;
        time.num_seconds() as f64 / SECONDS_PER_DAY
    }

    format!(
        "Appts {} | Installed {} | Lost {}\n\
        Average Loss Time: {}\n\
        Contingencies:             Rate {:.2} | Total {:2} | Avg Time (days) {:.2}\n\
        Contracts (Insurance):     Rate {:.2} | Total {:2} | Avg Time (days) {:.2}\n\
        Contracts (Retail):        Rate {:.2} | Total {:2} | Avg Time (days) {:.2}\n\
        Installations (Insurance): Rate {:.2} | Total {:2} | Avg Time (days) {:.2}\n\
        Installations (Retail):    Rate {:.2} | Total {:2} | Avg Time (days) {:.2}",
        num_appts, num_installs, num_losses,
        into_days(avg_loss_time),
        continge_conv, continge_total, into_days(continge_time),
        contract_insurance_conv, contract_insurance_total, into_days(contract_insurance_time),
        contract_retail_conv, contract_retail_total, into_days(contract_retail_time),
        install_insurance_conv, install_insurance_total, into_days(install_insurance_time),
        install_retail_conv, install_retail_total, into_days(install_retail_time),
    )
}
