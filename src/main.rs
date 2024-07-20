use std::{collections::HashMap, fmt::Display, ops::Index};

use anyhow::{bail, Result};
use job_tracker::{GetStatsResult, JobTracker};
use reqwest::{self, header::CONTENT_TYPE};
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

    let ProcessJobsResult { global_tracker, rep_specific_trackers, invalid_jobs } =
        process_jobs(jobs.into_iter());

    println!("\nGlobal Tracker: ================");
    print_job_tracker_results(&global_tracker);
    for (rep, tracker) in rep_specific_trackers {
        println!(
            "\nTracker for {}: =================",
            rep.unwrap_or("Unknown Sales Rep".to_owned())
        );
        print_job_tracker_results(&tracker);
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

    response.results.into_iter().map(Job::try_from).collect::<Result<Vec<_>>>()
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
        match job.into_analyzed() {
            Ok(analyzed) => {
                let kind = analyzed.kind.into_int();
                global_tracker.add_job(kind, &analyzed.timestamps, analyzed.settled);
                rep_specific_trackers
                    .entry(analyzed.job.sales_rep.clone())
                    .or_insert_with(build_job_tracker)
                    .add_job(kind, &analyzed.timestamps, analyzed.settled);
            }
            Err((job, err)) => {
                invalid_jobs.entry(job.sales_rep.clone()).or_insert_with(Vec::new).push((job, err));
            }
        }
    }

    ProcessJobsResult { global_tracker, rep_specific_trackers, invalid_jobs }
}

type JobTracker3x5 = JobTracker<{ JobKind::num_variants() }, { Milestone::num_variants() }>;

fn build_job_tracker() -> JobTracker3x5 {
    JobTracker::new([
        [true, true, true, true, true],
        [true, true, false, true, true],
        [true, true, false, true, true],
    ])
}

fn print_job_tracker_results(tracker: &JobTracker3x5) {
    fn print_single_conversion(get_stats_result: job_tracker::GetStatsResult) {
        let GetStatsResult { total, non_pending, conversion_rate, average_time_to_move_on } =
            get_stats_result;

        let average_time_to_move_on_days = average_time_to_move_on as f64 / 86400.0;
        println!(
            "{:3} {:3} {:.2} {:.2}",
            total, non_pending, conversion_rate, average_time_to_move_on_days
        );
    }

    println!("From appt to contingency (insurance)");
    print_single_conversion(tracker.get_stats(
        Milestone::AppointmentMade.into_int(),
        Some(JobKind::InsuranceWithContingency.into_int()),
    ));
    println!("From contingency to contract (insurance)");
    print_single_conversion(tracker.get_stats(
        Milestone::ContingencySigned.into_int(),
        Some(JobKind::InsuranceWithContingency.into_int()),
    ));
    println!("From appt to contract (insurance)");
    print_single_conversion(tracker.get_stats(
        Milestone::AppointmentMade.into_int(),
        Some(JobKind::InsuranceWithoutContingency.into_int()),
    ));
    println!("From appt to contract (retail");
    print_single_conversion(
        tracker.get_stats(Milestone::AppointmentMade.into_int(), Some(JobKind::Retail.into_int())),
    );
    println!("From contract to install (all)");
    print_single_conversion(tracker.get_stats(Milestone::ContractSigned.into_int(), None));
}

type Timestamp = i64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Milestone {
    LeadAcquired,
    AppointmentMade,
    ContingencySigned,
    ContractSigned,
    Installed,
}
impl Milestone {
    const fn num_variants() -> usize {
        5
    }

    fn ordered_iter() -> impl Iterator<Item = Self> {
        static ORDERED_VARIANTS: [Milestone; 5] = [
            Milestone::LeadAcquired,
            Milestone::AppointmentMade,
            Milestone::ContingencySigned,
            Milestone::ContractSigned,
            Milestone::Installed,
        ];
        ORDERED_VARIANTS.iter().copied()
    }

    const fn into_int(self) -> usize {
        match self {
            Milestone::LeadAcquired => 0,
            Milestone::AppointmentMade => 1,
            Milestone::ContingencySigned => 2,
            Milestone::ContractSigned => 3,
            Milestone::Installed => 4,
        }
    }
}
impl Display for Milestone {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Milestone::LeadAcquired => write!(f, "Lead Acquired"),
            Milestone::AppointmentMade => write!(f, "Appointment Made"),
            Milestone::ContingencySigned => write!(f, "Contingency Signed"),
            Milestone::ContractSigned => write!(f, "Contract Signed"),
            Milestone::Installed => write!(f, "Installed"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MilestoneDates {
    appointment_date: Option<Timestamp>,
    contingency_date: Option<Timestamp>,
    contract_date: Option<Timestamp>,
    install_date: Option<Timestamp>,
    loss_date: Option<Timestamp>,
}
impl Index<Milestone> for MilestoneDates {
    type Output = Option<i64>;

    fn index(&self, stage: Milestone) -> &Self::Output {
        static NONE: Option<i64> = None;

        match stage {
            Milestone::LeadAcquired => &NONE,
            Milestone::AppointmentMade => &self.appointment_date,
            Milestone::ContingencySigned => &self.contingency_date,
            Milestone::ContractSigned => &self.contract_date,
            Milestone::Installed => &self.install_date,
        }
    }
}
impl MilestoneDates {
    fn timestamps_up_to(&self, stage: Milestone) -> Vec<Option<Timestamp>> {
        Milestone::ordered_iter().take_while(|&s| s <= stage).map(move |s| self[s]).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Job {
    jnid: String,
    milestone_dates: MilestoneDates,
    sales_rep: Option<String>,
    insurance_claim_number: Option<String>,
    job_number: Option<String>,
    customer_name: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct AnalyzedJob {
    job: Job,
    /// The kind of job that we have. This may not be totally accurate if the
    /// job is not settled.
    kind: JobKind,
    /// The dates at which all of the milestones of the job were reached. These
    /// dates must be monotonically increasing. The length is equal to one more
    /// the index of the last milestone reached (e.g. a length of 1 means that
    /// only the first milestone was reached), and None indicates the earliest
    /// possible time which is still in order.
    timestamps: Vec<Option<Timestamp>>,
    /// Whether the job is settled (i.e. "black", i.e. either lost or at the
    /// last milestone).
    settled: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum JobKind {
    InsuranceWithContingency,
    InsuranceWithoutContingency,
    Retail,
}
impl JobKind {
    const fn num_variants() -> usize {
        3
    }

    const fn into_int(self) -> usize {
        match self {
            JobKind::InsuranceWithContingency => 0,
            JobKind::InsuranceWithoutContingency => 1,
            JobKind::Retail => 2,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
enum JobAnalysisError {
    #[error(
        "This job has signed a contingency form, but does not have an insurance claim number."
    )]
    ContingencyWithoutInsurance,
    #[error("The date for {} does not follow previous dates.", .0.map(|stage| stage.to_string()).unwrap_or("Job Lost".to_owned()))]
    OutOfOrderDates(Option<Milestone>),
    #[error("This job has skipped the date for the milestone {0:?}.")]
    SkippedDates(Milestone),
}

impl Job {
    fn into_analyzed(self) -> Result<AnalyzedJob, (Self, JobAnalysisError)> {
        // ensure that the milestone dates make chronological sense

        let mut previous_date = None;
        let mut current_milestone = Milestone::LeadAcquired;
        let mut in_progress = true; // whether retracing of the job's history is still in progress
        let mut kind = if self.insurance_claim_number.is_some() {
            JobKind::InsuranceWithContingency
        } else {
            JobKind::Retail
        };
        for milestone in Milestone::ordered_iter().skip(1) {
            let date = self.milestone_dates[milestone];

            if in_progress {
                if let Some(date) = date {
                    // this milestone happened, so update the current milestone accordingly
                    current_milestone = milestone;

                    // update the job kind if necessary
                    if milestone == Milestone::ContingencySigned
                        && self.insurance_claim_number.is_none()
                    {
                        return Err((self, JobAnalysisError::ContingencyWithoutInsurance));
                    }
                    if milestone == Milestone::ContractSigned
                        && self.milestone_dates.contingency_date.is_none()
                        && self.insurance_claim_number.is_some()
                    {
                        kind = JobKind::InsuranceWithoutContingency
                    }

                    // verify that the date is greater than the previous date
                    if let Some(previous_date) = previous_date {
                        if date <= previous_date {
                            return Err((self, JobAnalysisError::OutOfOrderDates(Some(milestone))));
                        }
                    }
                    previous_date = Some(date);
                } else {
                    if milestone != Milestone::ContingencySigned {
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
                    return Err((self, JobAnalysisError::SkippedDates(current_milestone)));
                }
            }
        }
        let settled = if let Some(loss_date) = self.milestone_dates.loss_date {
            // ensure that the loss date comes after all other dates
            if let Some(previous_date) = previous_date {
                if loss_date <= previous_date {
                    return Err((self, JobAnalysisError::OutOfOrderDates(None)));
                }
            }
            true
        } else {
            current_milestone == Milestone::Installed
        };

        Ok(AnalyzedJob {
            timestamps: self.milestone_dates.timestamps_up_to(current_milestone),
            kind,
            settled,
            job: self,
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

        fn get_owned_nonempty(
            map: &serde_json::Map<String, serde_json::Value>,
            key: &str,
        ) -> Option<String> {
            map.get(key).and_then(|val| val.as_str()).filter(|str| str.len() > 0).map(str::to_owned)
        }

        let sales_rep = get_owned_nonempty(&map, KEY_SALES_REP);
        let insurance_claim_number = get_owned_nonempty(&map, KEY_INSURANCE_CLAIM_NUMBER);
        let job_number = get_owned_nonempty(&map, KEY_JOB_NUMBER);
        let customer_name = get_owned_nonempty(&map, KEY_CUSTOMER_NAME);

        // the JobNimbus API sometimes returns a 0 timestamp for a date that has
        // no value, so we want to filter those out as if the value did not
        // exist
        fn get_nonzero(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<i64> {
            map.get(key).and_then(|value| value.as_i64()).filter(|&val| val != 0)
        }

        // extract all the milestone dates
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
            milestone_dates: MilestoneDates {
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
    use std::vec;

    use super::*;

    fn make_job(
        insurance: bool,
        date_1: Option<i64>,
        date_2: Option<i64>,
        date_3: Option<i64>,
        date_4: Option<i64>,
        date_5: Option<i64>,
    ) -> Job {
        Job {
            jnid: "0".to_owned(),
            sales_rep: None,
            insurance_claim_number: if insurance { Some("123".to_owned()) } else { None },
            job_number: None,
            customer_name: None,
            milestone_dates: MilestoneDates {
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
        let job = make_job(false, Some(1), None, Some(3), Some(4), None);
        assert_eq!(
            job.clone().into_analyzed(),
            Ok(AnalyzedJob {
                job,
                kind: JobKind::Retail,
                timestamps: vec![None, Some(1), None, Some(3), Some(4)],
                settled: true,
            })
        );
    }

    #[test]
    fn job_status_retail_with_contingency() {
        let job = make_job(false, Some(1), Some(2), Some(3), Some(4), None);
        assert_eq!(
            job.clone().into_analyzed(),
            Err((job, JobAnalysisError::ContingencyWithoutInsurance))
        );
    }

    #[test]
    fn job_status_insurance_without_contingency() {
        let job = make_job(true, Some(1), None, Some(3), Some(4), None);
        assert_eq!(
            job.clone().into_analyzed(),
            Ok(AnalyzedJob {
                job,
                kind: JobKind::InsuranceWithoutContingency,
                timestamps: vec![None, Some(1), None, Some(3), Some(4)],
                settled: true,
            })
        );
    }

    #[test]
    fn job_status_insurance_with_contingency() {
        let job = make_job(true, Some(1), Some(2), Some(3), Some(4), None);
        assert_eq!(
            job.clone().into_analyzed(),
            Ok(AnalyzedJob {
                job,
                kind: JobKind::InsuranceWithContingency,
                timestamps: vec![None, Some(1), Some(2), Some(3), Some(4)],
                settled: true,
            })
        );
    }

    #[test]
    fn job_status_insurance_at_each_stage() {
        let job = make_job(true, None, None, None, None, None);
        assert_eq!(
            job.clone().into_analyzed(),
            Ok(AnalyzedJob {
                job,
                kind: JobKind::InsuranceWithContingency,
                timestamps: vec![None],
                settled: false,
            })
        );

        let job = make_job(true, Some(1), None, None, None, None);
        assert_eq!(
            job.clone().into_analyzed(),
            Ok(AnalyzedJob {
                job,
                kind: JobKind::InsuranceWithContingency,
                timestamps: vec![None, Some(1)],
                settled: false,
            })
        );

        let job = make_job(true, Some(1), Some(2), None, None, None);
        assert_eq!(
            job.clone().into_analyzed(),
            Ok(AnalyzedJob {
                job,
                kind: JobKind::InsuranceWithContingency,
                timestamps: vec![None, Some(1), Some(2)],
                settled: false,
            })
        );

        let job = make_job(true, Some(1), Some(2), Some(3), None, None);
        assert_eq!(
            job.clone().into_analyzed(),
            Ok(AnalyzedJob {
                job,
                kind: JobKind::InsuranceWithContingency,
                timestamps: vec![None, Some(1), Some(2), Some(3)],
                settled: false,
            })
        );

        let job = make_job(true, Some(1), Some(2), Some(3), Some(4), None);
        assert_eq!(
            job.clone().into_analyzed(),
            Ok(AnalyzedJob {
                job,
                kind: JobKind::InsuranceWithContingency,
                timestamps: vec![None, Some(1), Some(2), Some(3), Some(4)],
                settled: true,
            })
        );

        let job = make_job(true, Some(1), None, None, None, Some(5));
        assert_eq!(
            job.clone().into_analyzed(),
            Ok(AnalyzedJob {
                job,
                kind: JobKind::InsuranceWithContingency,
                timestamps: vec![None, Some(1)],
                settled: true,
            })
        );
    }
}
