use std::{collections::HashMap, fmt::Display, ops::Index};

use job_tracker::{GetStatsResult, JobTracker};
use reqwest::{self, header::CONTENT_TYPE};
use anyhow::{bail, Result};
use serde::Deserialize;
use thiserror::Error;

mod job_tracker;

const ENDPOINT_JOBS: &str = "https://app.jobnimbus.com/api1/jobs";
const KEY_JNID: &str = "jnid";
const KEY_SALES_REP: &str = "sales_rep_name";
const KEY_INSURANCE_CLAIM_NUMBER: &str = "Claim #";
const KEY_JOB_NUMBER: &str = "number";
const KEY_CUSTOMER_NAME: &str = "name";
const KEY_APPOINTMENT_DATE: &str = "Sales Appt #1 Date";
const KEY_CONTINGENCY_DATE: &str = "Signed Contingency Date";
const KEY_CONTRACT_DATE: &str = "Signed Contract Date";
const KEY_INSTALL_DATE: &str = "Install Date";
const KEY_LOSS_DATE: &str = "Loss Date (if applicable)";

fn main() -> Result<()> {
    let Ok(api_key) = std::env::var("AHI_API_KEY") else {
        bail!("AHI_API_KEY environment variable not set");
    };
    let jobs = get_all_jobs(&api_key)?;

    let ProcessJobsResult { global_tracker, rep_specific_trackers, invalid_jobs } = process_jobs(jobs.into_iter());

    println!("\nGlobal Tracker: ================");
    print_job_tracker_results(&global_tracker);
    for (rep, tracker) in rep_specific_trackers {
        println!("\nTracker for {}: =================", rep);
        print_job_tracker_results(&tracker);
    }
    for (rep, jobs) in invalid_jobs {
        println!("\nInvalid jobs for {}: ===============", rep);
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
    let response = client.get(url.clone())
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
    let response = client.get(url)
        .bearer_auth(&api_key)
        .header(CONTENT_TYPE, "application/json")
        .query(&[("size", count)])
        .send()?;
    let response: ApiResponse = response.json()?;
    println!("recieved {} jobs from JobNimbus", response.count);
    assert_eq!(response.count, count);

    response.results.into_iter().map(Job::try_from).collect::<Result<Vec<_>>>()
}

struct ProcessJobsResult {
    global_tracker: JobTracker3x5,
    rep_specific_trackers: HashMap<String, JobTracker3x5>,
    invalid_jobs: HashMap<String, Vec<(Job, JobStatusError)>>,
}
fn process_jobs(jobs: impl Iterator<Item = Job>) -> ProcessJobsResult {
    let mut global_tracker = build_job_tracker();
    let mut rep_specific_trackers = HashMap::new();
    let mut invalid_jobs = HashMap::new();
    for job in jobs {
        match job.status() {
            Ok(status) => {
                let kind = status.contingency_requirement.into_int();
                global_tracker.add_job(kind, &job.event_dates.timestamps_up_to(status.stage), status.lost);
                rep_specific_trackers.entry(job.sales_rep.clone()).or_insert_with(build_job_tracker).add_job(kind, &job.event_dates.timestamps_up_to(status.stage), status.lost);

                if status.contingency_requirement == ContingencyReq::Retail && job.event_dates.appointment_date.is_some(){
                    println!("Retail job: {}", job.job_number.as_deref().unwrap_or("unknown job #"));
                }
            },
            Err(err) => {
                invalid_jobs.entry(job.sales_rep.clone()).or_insert_with(Vec::new).push((job, err));
            }
        }
    }

    ProcessJobsResult {
        global_tracker,
        rep_specific_trackers,
        invalid_jobs,
    }
}

type JobTracker3x5 = JobTracker<{ContingencyReq::num_variants()}, {JobStage::num_variants()}>;

fn build_job_tracker() -> JobTracker3x5 {
    JobTracker::new([
        [true, true, true, true, true],
        [true, true, false, true, true],
        [true, true, false, true, true],
    ])
}

fn print_job_tracker_results(tracker: &JobTracker3x5) {
    fn print_single_conversion(get_stats_result: job_tracker::GetStatsResult) {
        let GetStatsResult { total, non_pending, conversion_rate, average_time_to_move_on } = get_stats_result;

        let average_time_to_move_on_days = average_time_to_move_on as f64 / 86400.0;
        println!("{:3} {:3} {:.2} {:.2}", total, non_pending, conversion_rate, average_time_to_move_on_days);
    }

    println!("From appt to contingency (insurance)");
    print_single_conversion(tracker.get_stats(JobStage::AppointmentMade.into_int(), Some(ContingencyReq::InsuranceWithContingency.into_int())));
    println!("From contingency to contract (insurance)");
    print_single_conversion(tracker.get_stats(JobStage::ContingencySigned.into_int(), Some(ContingencyReq::InsuranceWithContingency.into_int())));
    println!("From appt to contract (insurance)");
    print_single_conversion(tracker.get_stats(JobStage::AppointmentMade.into_int(), Some(ContingencyReq::InsuranceWithoutContingency.into_int())));
    println!("From appt to contract (retail");
    print_single_conversion(tracker.get_stats(JobStage::AppointmentMade.into_int(), Some(ContingencyReq::Retail.into_int())));
    println!("From contract to install (all)");
    print_single_conversion(tracker.get_stats(JobStage::ContractSigned.into_int(), None));
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum JobStage {
    LeadAcquired,
    AppointmentMade,
    ContingencySigned,
    ContractSigned,
    Installed,
}
impl JobStage {
    const fn num_variants() -> usize {
        5
    }

    fn ordered_iter() -> impl Iterator<Item = Self> {
        static ORDERED_VARIANTS: [JobStage; 5] = [
            JobStage::LeadAcquired,
            JobStage::AppointmentMade,
            JobStage::ContingencySigned,
            JobStage::ContractSigned,
            JobStage::Installed,
        ];
        ORDERED_VARIANTS.iter().copied()
    }

    const fn into_int(self) -> usize {
        match self {
            JobStage::LeadAcquired => 0,
            JobStage::AppointmentMade => 1,
            JobStage::ContingencySigned => 2,
            JobStage::ContractSigned => 3,
            JobStage::Installed => 4,
        }
    }
}
impl Display for JobStage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            JobStage::LeadAcquired => write!(f, "Lead Acquired"),
            JobStage::AppointmentMade => write!(f, "Appointment Made"),
            JobStage::ContingencySigned => write!(f, "Contingency Signed"),
            JobStage::ContractSigned => write!(f, "Contract Signed"),
            JobStage::Installed => write!(f, "Installed"),
        }
    }
}

#[derive(Debug)]
struct EventDates {
    appointment_date: Option<i64>,
    contingency_date: Option<i64>,
    contract_date: Option<i64>,
    install_date: Option<i64>,
    loss_date: Option<i64>,
}
impl Index<JobStage> for EventDates {
    type Output = Option<i64>;

    fn index(&self, stage: JobStage) -> &Self::Output {
        static NONE: Option<i64> = None;

        match stage {
            JobStage::LeadAcquired => &NONE,
            JobStage::AppointmentMade => &self.appointment_date,
            JobStage::ContingencySigned => &self.contingency_date,
            JobStage::ContractSigned => &self.contract_date,
            JobStage::Installed => &self.install_date,
        }
    }

}
impl EventDates {
    fn timestamps_up_to(&self, stage: JobStage) -> Vec<Option<i64>> {
        JobStage::ordered_iter().take_while(|&s| s <= stage).map(move |s| self[s]).collect()
    }
}

#[derive(Debug)]
struct Job {
    jnid: String,
    event_dates: EventDates,
    sales_rep: String,
    insurance_claim_number: Option<String>,
    job_number: Option<String>,
    customer_name: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
struct JobStatus {
    contingency_requirement: ContingencyReq,
    stage: JobStage,
    lost: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ContingencyReq {
    InsuranceWithContingency,
    InsuranceWithoutContingency,
    Retail,
}
impl ContingencyReq {
    const fn num_variants() -> usize {
        3
    }

    const fn into_int(self) -> usize {
        match self {
            ContingencyReq::InsuranceWithContingency => 0,
            ContingencyReq::InsuranceWithoutContingency => 1,
            ContingencyReq::Retail => 2,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
enum JobStatusError {
    #[error("This job has signed a contingency form, but does not have an insurance claim number.")]
    ContingencyWithoutInsurance,
    #[error("The date for {} does not follow previous dates.", .0.map(|stage| stage.to_string()).unwrap_or("Job Lost".to_owned()))]
    OutOfOrderDates(Option<JobStage>),
    #[error("This job has skipped the stage {0:?}.")]
    SkippedDates(JobStage),
}

impl Job {
    fn status(&self) -> Result<JobStatus, JobStatusError> {
        // ensure that the event dates make chronological sense

        let mut previous_date = None;
        let mut current_stage = JobStage::LeadAcquired;
        let mut in_progress = true; // whether retracing of the job's history is still in progress
        let mut contingency_requirement = if self.insurance_claim_number.is_some() {
            ContingencyReq::InsuranceWithContingency
        } else {
            ContingencyReq::Retail
        };
        for stage in JobStage::ordered_iter().skip(1) {
            let date = self.event_dates[stage];

            if in_progress {
                if let Some(date) = date {
                    // this event happened, so update the stage accordingly
                    current_stage = stage;

                    // update the contingency requirement if necessary
                    if stage == JobStage::ContingencySigned && self.insurance_claim_number.is_none() {
                        return Err(JobStatusError::ContingencyWithoutInsurance);
                    }
                    if stage == JobStage::ContractSigned && self.event_dates.contingency_date.is_none() && self.insurance_claim_number.is_some() {
                        contingency_requirement = ContingencyReq::InsuranceWithoutContingency
                    }

                    // verify that the date is greater than the previous date
                    if let Some(previous_date) = previous_date {
                        if date <= previous_date {
                            return Err(JobStatusError::OutOfOrderDates(Some(stage)));
                        }
                    }
                    previous_date = Some(date);
                } else {
                    if stage != JobStage::ContingencySigned {
                        // the job is no longer in progress
                        in_progress = false;
                    }
                    // we make a special exception for the contingency date,
                    // since not all jobs require it
                }
            } else {
                // retracing is no longer in progress, meaning that some
                // previous date was None, so this date must also be None
                if date.is_some() {
                    return Err(JobStatusError::SkippedDates(current_stage));
                }
            }
        }
        let lost = if let Some(loss_date) = self.event_dates.loss_date {
            // ensure that the loss date comes after all other dates
            if let Some(previous_date) = previous_date {
                if loss_date <= previous_date {
                    return Err(JobStatusError::OutOfOrderDates(None));
                }
            }

            true
        } else { false };

        Ok(JobStatus {
            stage: current_stage,
            contingency_requirement,
            lost,
        })
    }
}

impl TryFrom<serde_json::Value> for Job {
    type Error = anyhow::Error;

    fn try_from(value: serde_json::Value) -> Result<Self> {
        let serde_json::Value::Object(map) = value else {
            bail!("Expected a JSON object; found {}", value);
        };

        let Some(jnid) = map.get(KEY_JNID).and_then(|val| val.as_str()).map(str::to_owned) else {
            bail!("Expected a '{KEY_JNID}' field in the JSON object");
        };
        let sales_rep = map.get(KEY_SALES_REP).and_then(|val| val.as_str()).unwrap_or("").to_owned();
        let insurance_claim_number = map.get(KEY_INSURANCE_CLAIM_NUMBER).and_then(|val| val.as_str()).filter(|str| str.len() > 0).map(str::to_owned);
        let job_number = map.get(KEY_JOB_NUMBER).and_then(|val| val.as_str()).filter(|str| str.len() > 0).map(str::to_owned);
        let customer_name = map.get(KEY_CUSTOMER_NAME).and_then(|val| val.as_str()).filter(|str| str.len() > 0).map(str::to_owned);

        // the JobNimbus API sometimes returns a 0 timestamp for a date that has
        // no value, so we want to filter those out as if the value did not
        // exist
        fn get_nonzero(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<i64> {
            map.get(key).and_then(|value| value.as_i64()).filter(|&val| val != 0)
        }

        // extract all the event dates
        let appointment_date = get_nonzero(&map, KEY_APPOINTMENT_DATE);
        let contingency_date = get_nonzero(&map, KEY_CONTINGENCY_DATE);
        let contract_date = get_nonzero(&map, KEY_CONTRACT_DATE);
        let install_date = get_nonzero(&map, KEY_INSTALL_DATE);
        let loss_date = get_nonzero(&map, KEY_LOSS_DATE);

        Ok(Job {
            jnid,
            sales_rep,
            insurance_claim_number,
            job_number,
            customer_name,
            event_dates: EventDates {
                appointment_date,
                contingency_date,
                contract_date,
                install_date,
                loss_date,
            },
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn make_job(insurance: bool, date_1: Option<i64>, date_2: Option<i64>, date_3: Option<i64>, date_4: Option<i64>, date_5: Option<i64>) -> Job {
        Job {
            jnid: "0".to_owned(),
            sales_rep: "John Doe".to_owned(),
            insurance_claim_number: if insurance { Some("123".to_owned()) } else { None },
            job_number: None,
            customer_name: None,
            event_dates: EventDates {
                appointment_date: date_1,
                contingency_date: date_2,
                contract_date: date_3,
                install_date: date_4,
                loss_date: date_5,
            },
        }
    }

    #[test]
    fn job_status_retail_without_contingency() {
        let status = make_job(false, Some(1), None, Some(3), Some(4), None).status();
        assert_eq!(status, Ok(JobStatus {
            stage: JobStage::Installed,
            contingency_requirement: ContingencyReq::Retail,
            lost: false,
        }));
    }

    #[test]
    fn job_status_retail_with_contingency() {
        let status = make_job(false, Some(1), Some(2), Some(3), Some(4), None).status();
        assert_eq!(status, Err(JobStatusError::ContingencyWithoutInsurance));
    }

    #[test]
    fn job_status_insurance_without_contingency() {
        let status = make_job(true, Some(1), None, Some(3), Some(4), None).status();
        assert_eq!(status, Ok(JobStatus {
            stage: JobStage::Installed,
            contingency_requirement: ContingencyReq::InsuranceWithoutContingency,
            lost: false,
        }));
    }

    #[test]
    fn job_status_insurance_with_contingency() {
        let status = make_job(true, Some(1), Some(2), Some(3), Some(4), None).status();
        assert_eq!(status, Ok(JobStatus {
            stage: JobStage::Installed,
            contingency_requirement: ContingencyReq::InsuranceWithContingency,
            lost: false,
        }));
    }

    #[test]
    fn job_status_insurance_at_each_stage() {
        let status = make_job(true, None, None, None, None, None).status();
        assert_eq!(status, Ok(JobStatus {
            stage: JobStage::LeadAcquired,
            contingency_requirement: ContingencyReq::InsuranceWithContingency,
            lost: false,
        }));

        let status = make_job(true, Some(1), None, None, None, None).status();
        assert_eq!(status, Ok(JobStatus {
            stage: JobStage::AppointmentMade,
            contingency_requirement: ContingencyReq::InsuranceWithContingency,
            lost: false,
        }));

        let status = make_job(true, Some(1), Some(2), None, None, None).status();
        assert_eq!(status, Ok(JobStatus {
            stage: JobStage::ContingencySigned,
            contingency_requirement: ContingencyReq::InsuranceWithContingency,
            lost: false,
        }));

        let status = make_job(true, Some(1), Some(2), Some(3), None, None).status();
        assert_eq!(status, Ok(JobStatus {
            stage: JobStage::ContractSigned,
            contingency_requirement: ContingencyReq::InsuranceWithContingency,
            lost: false,
        }));

        let status = make_job(true, Some(1), Some(2), Some(3), Some(4), None).status();
        assert_eq!(status, Ok(JobStatus {
            stage: JobStage::Installed,
            contingency_requirement: ContingencyReq::InsuranceWithContingency,
            lost: false,
        }));

        let status = make_job(true, Some(1), None, None, None, Some(5)).status();
        assert_eq!(status, Ok(JobStatus {
            stage: JobStage::AppointmentMade,
            contingency_requirement: ContingencyReq::InsuranceWithContingency,
            lost: true,
        }));
    }


}
