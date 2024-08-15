use std::collections::HashMap;

use anyhow::{bail, Result};
use job_tracker::{CalcStatsResult, JobTracker};
use jobs::{Job, JobAnalysisError, JobKind, Milestone, TimeDelta};
use reqwest::{self, blocking::Response, header::CONTENT_TYPE};
use serde::Deserialize;

mod job_tracker;
mod jobs;

const ENDPOINT_JOBS: &str = "https://app.jobnimbus.com/api1/jobs";

fn main() -> Result<()> {
    let Ok(api_key) = std::env::var("AHI_API_KEY") else {
        bail!("AHI_API_KEY environment variable not set");
    };
    let jobs = get_all_jobs_from_job_nimbus(&api_key)?;

    let ProcessJobsResult { global_tracker, rep_specific_trackers, red_flags } =
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
    for (rep, red_flags) in red_flags {
        println!(
            "\nRed flags for {}: ===============",
            rep.unwrap_or("Unknown Sales Rep".to_owned())
        );
        for (job_number, err) in red_flags {
            println!("{}: {}", job_number.as_deref().unwrap_or("unknown job #"), err);
        }
    }

    Ok(())
}

fn request_from_job_nimbus(api_key: &str, num_jobs: usize) -> Result<Response> {
    let url = reqwest::Url::parse(ENDPOINT_JOBS)?;
    let client = reqwest::blocking::Client::new();
    let response = client
        .get(url.clone())
        .bearer_auth(&api_key)
        .header(CONTENT_TYPE, "application/json")
        .query(&[("size", num_jobs.to_string().as_str()), ("filter", include_str!("filter.json"))])
        .send()?;
    if !response.status().is_success() {
        bail!("Request failed with status code: {}", response.status());
    }
    Ok(response)
}

// blocking
fn get_all_jobs_from_job_nimbus(api_key: &str) -> Result<Vec<Job>> {
    use serde_json::Value;
    #[derive(Deserialize)]
    struct ApiResponse {
        count: u64,
        results: Vec<Value>,
    }

    println!("getting all jobs from JobNimbus");

    // make a request to find out the number of jobs
    let response = request_from_job_nimbus(api_key, 1)?;
    let response: ApiResponse = response.json()?;
    let count = response.count as usize;

    println!("detected {} jobs in JobNimbus", count);

    // make a request to actually get those jobs
    let response = request_from_job_nimbus(api_key, count)?;
    let response: ApiResponse = response.json()?;
    println!("recieved {} jobs from JobNimbus", response.count);
    assert_eq!(response.count as usize, count);

    let results: Result<Vec<_>, _> = response.results.into_iter().map(Job::try_from).collect();
    Ok(results?)
}

struct ProcessJobsResult {
    global_tracker: JobTracker3x5,
    rep_specific_trackers: HashMap<Option<String>, JobTracker3x5>,
    red_flags: HashMap<Option<String>, Vec<(Option<String>, JobAnalysisError)>>,
}
fn process_jobs(jobs: impl Iterator<Item = Job>) -> ProcessJobsResult {
    let mut global_tracker = build_job_tracker();
    let mut rep_specific_trackers = HashMap::new();
    let mut red_flags = HashMap::new();
    for job in jobs {
        let (analyzed, errors) = jobs::analyze_job(job);
        let (sales_rep, job_number) = match analyzed {
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
                (analyzed.job.sales_rep, analyzed.job.job_number)
            }
            Err(job) => (job.sales_rep, job.job_number),
        };
        let sales_rep_errors: &mut Vec<_> = red_flags.entry(sales_rep).or_default();
        for error in errors {
            sales_rep_errors.push((job_number.clone(), error));
        }
    }

    ProcessJobsResult { global_tracker, rep_specific_trackers, red_flags }
}

type JobTracker3x5 = JobTracker<{ JobKind::NUM_VARIANTS }, { Milestone::NUM_VARIANTS }>;

fn build_job_tracker() -> JobTracker3x5 {
    JobTracker::new([
        [true, true, true, true, true],
        [true, true, false, true, true],
        [true, true, false, true, true],
    ])
}

#[rustfmt::skip] // for the big format! at the end
fn format_job_tracker_results(tracker: &JobTracker3x5) -> String {
    let iwc = JobKind::InsuranceWithContingency.into_int(); // "insurance with contingency"
    let iwo = JobKind::InsuranceWithoutContingency.into_int(); // "insurance without contingency"
    let ret = JobKind::Retail.into_int(); // "retail"

    // some basic stats
    let num_appts = tracker.calc_stats(Milestone::AppointmentMade.into_int(), &[iwc, iwo, ret]).num_total;
    let num_installs = tracker.calc_stats(Milestone::Installed.into_int(), &[iwc, iwo, ret]).num_total;
    let (num_losses, avg_loss_time) = tracker.calc_stats_of_loss();
    let loss_rate = if num_appts == 0 { None } else { Some(num_losses as f64 / num_appts as f64) };

    let num_insure_appts = tracker.calc_stats(Milestone::AppointmentMade.into_int(), &[iwc, iwo]).num_total;

    // from appt to contingency (insurance)
    let (appt_continge_total, appt_continge_conv, appt_continge_time) = {
        let &job_tracker::Bucket { achieved, cum_achieve_time, .. } = tracker.get_bucket(iwc, Milestone::ContingencySigned.into_int()).unwrap();
        let rate = if num_appts == 0 { None } else { Some(achieved as f64 / num_insure_appts as f64) };
        let time = if achieved == 0 { TimeDelta::zero() } else { cum_achieve_time / achieved.try_into().unwrap() };
        (achieved, rate, time)
    };

    // from appt to contract (insurance)
    let (appt_contract_insure_total, appt_contract_insure_conv, appt_contract_insure_time) = {
        let &job_tracker::Bucket { achieved, cum_achieve_time, .. } = tracker.get_bucket(iwo, Milestone::ContractSigned.into_int()).unwrap();
        let rate = if num_appts == 0 { None } else { Some(achieved as f64 / num_insure_appts as f64) };
        let time = if achieved == 0 { TimeDelta::zero() } else { cum_achieve_time / achieved.try_into().unwrap() };
        (achieved, rate, time)
    };

    // from contingency to contract (insurance)
    let CalcStatsResult {
        num_total: continge_contract_total,
        conversion_rate: continge_contract_conv,
        average_time_to_achieve: continge_contract_time,
    } = tracker.calc_stats(Milestone::ContractSigned.into_int(), &[iwc]);

    // from appointment to contract (retail)
    let CalcStatsResult {
        num_total: appt_contract_retail_total,
        conversion_rate: appt_contract_retail_conv,
        average_time_to_achieve: appt_contract_retail_time,
    } = tracker.calc_stats(Milestone::ContractSigned.into_int(), &[ret]);

    // from contract to install (insurance)
    let CalcStatsResult {
        num_total: install_insure_total,
        conversion_rate: install_insure_conv,
        average_time_to_achieve: install_insure_time,
    } = tracker.calc_stats(Milestone::Installed.into_int(), &[iwc, iwo]);

    // from contract to install (insurance)
    let CalcStatsResult {
        num_total: install_retail_total,
        conversion_rate: install_retail_conv,
        average_time_to_achieve: install_retail_time,
    } = tracker.calc_stats(Milestone::Installed.into_int(), &[ret]);

    fn into_days(time: TimeDelta) -> f64 {
        const SECONDS_PER_DAY: f64 = 86400.0;
        time.num_seconds() as f64 / SECONDS_PER_DAY
    }
    fn percent_or_na(rate: Option<f64>) -> String {
        rate.map(|r| format!("{:6.2}%", r * 100.0)).unwrap_or_else(|| "    N/A".to_owned())
    }

    format!(
        "Appts {} | Installed {}\n\
        All Losses:                   Rate {} | Total {:2} | Avg Time {:.2} days\n\
        (I) Appt to Contingency:      Rate {} | Total {:2} | Avg Time {:.2} days\n\
        (I) Appt to Contract:         Rate {} | Total {:2} | Avg Time {:.2} days\n\
        (I) Contingency to Contract:  Rate {} | Total {:2} | Avg Time {:.2} days\n\
        (R) Appt to Contract:         Rate {} | Total {:2} | Avg Time {:.2} days\n\
        (I) Contract to Installation: Rate {} | Total {:2} | Avg Time {:.2} days\n\
        (R) Contract to Installation: Rate {} | Total {:2} | Avg Time {:.2} days",
        num_appts, num_installs,
        percent_or_na(loss_rate), num_losses, into_days(avg_loss_time),
        percent_or_na(appt_continge_conv), appt_continge_total, into_days(appt_continge_time),
        percent_or_na(appt_contract_insure_conv), appt_contract_insure_total, into_days(appt_contract_insure_time),
        percent_or_na(continge_contract_conv), continge_contract_total, into_days(continge_contract_time),
        percent_or_na(appt_contract_retail_conv), appt_contract_retail_total, into_days(appt_contract_retail_time),
        percent_or_na(install_insure_conv), install_insure_total, into_days(install_insure_time),
        percent_or_na(install_retail_conv), install_retail_total, into_days(install_retail_time),
    )
}
