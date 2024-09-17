use std::{collections::HashMap, rc::Rc};

use crate::job_nimbus_api;
use crate::job_tracker;
use crate::jobs;
use crate::jobs::Timestamp;
use anyhow::Context;
use anyhow::Result;
use chrono::Datelike as _;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::TimeZone as _;
use chrono::Utc;
use job_tracker::{CalcStatsResult, JobTracker};
use jobs::{AnalyzedJob, Job, JobAnalysisError, JobKind, Milestone, TimeDelta};

#[derive(clap::Args, Debug)]
pub struct Args {
    /// The filter to use when query JobNimbus for jobs, using ElasticSearch
    /// syntax.
    #[arg(short, long = "filter", default_value = None)]
    filter_filename: Option<String>,

    /// The minimum date to filter jobs by. The final report will only include
    /// jobs where the date that they were settled (date of install or date of
    /// loss) is after the minimum date. Valid options are a date of the form
    /// "%Y-%m-%d", "ytd" (indicating the start of the current year), "today"
    /// (indicating the current date), or "forever" (indicating the beginning of
    /// time).
    #[arg(long = "from", default_value = "forever")]
    from_date: String,
    /// The maximum date to filter jobs by. The final report will only include
    /// jobs where the date that they were settled (date of install or date of
    /// loss) is before the maximum date. Valid options are a date of the form
    /// "%Y-%m-%d", "today" (indicating the current date), or "forever"
    /// (indicating the end of time).
    #[arg(long = "to", default_value = "today")]
    to_date: String,
}

pub fn main(api_key: &str, args: Args) -> Result<()> {
    let Args { filter_filename, from_date, to_date } = args;
    let filter = if let Some(filter_filename) = filter_filename {
        Some(std::fs::read_to_string(filter_filename)?)
    } else {
        None
    };
    let jobs = job_nimbus_api::get_all_jobs_from_job_nimbus(&api_key, filter.as_deref())?;

    let from_date = match from_date.as_str() {
        "forever" => None,
        "ytd" => Some(
            Utc.from_utc_datetime(&NaiveDateTime::new(
                NaiveDate::from_ymd_opt(Utc::now().year(), 1, 1)
                    .expect("Jan 1 should always be valid in the current year."),
                NaiveTime::MIN,
            )),
        ),
        "today" => Some(Utc::now()),
        date_string => Some(
            NaiveDate::parse_from_str(date_string, "%Y-%m-%d")
                .map(|date| Utc.from_utc_datetime(&NaiveDateTime::new(date, NaiveTime::MIN)))
                .context("Invalid date format. Use 'forever', 'ytd', 'today', or '%Y-%m-%d'.")?,
        ),
    };
    let to_date = match to_date.as_str() {
        "forever" => None,
        "today" => Some(Utc::now()),
        date_string => Some(
            NaiveDate::parse_from_str(date_string, "%Y-%m-%d")
                .map(|date| Utc.from_utc_datetime(&NaiveDateTime::new(date, NaiveTime::MIN)))
                .context("Invalid date format. Use 'forever', 'ytd', 'today', or '%Y-%m-%d'.")?,
        ),
    };

    let ProcessJobsResult { global_tracker, rep_specific_trackers, red_flags } =
        process_jobs(jobs.into_iter(), (from_date, to_date));

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
        for (job, err) in red_flags {
            println!("{}: {}", job.job.job_number.as_deref().unwrap_or("unknown job #"), err);
        }
    }

    Ok(())
}

struct ProcessJobsResult {
    global_tracker: JobTracker3x5,
    rep_specific_trackers: HashMap<Option<String>, JobTracker3x5>,
    red_flags: HashMap<Option<String>, Vec<(Rc<AnalyzedJob>, JobAnalysisError)>>,
}
fn process_jobs(
    jobs: impl Iterator<Item = Job>,
    (from_dt, to_dt): (Option<Timestamp>, Option<Timestamp>),
) -> ProcessJobsResult {
    eprintln!(
        "Processing jobs settled between {} and {}",
        from_dt.map(|dt| dt.to_string()).as_deref().unwrap_or("the beginning of time"),
        to_dt.map(|dt| dt.to_string()).as_deref().unwrap_or("the end of time")
    );

    let mut global_tracker = build_job_tracker();
    let mut rep_specific_trackers = HashMap::new();
    let mut red_flags = HashMap::new();
    for job in jobs {
        let (analyzed, errors) = jobs::analyze_job(job);
        let analyzed = Rc::new(analyzed);
        if let AnalyzedJob { analysis: Some(analysis), .. } = analyzed.as_ref() {
            // only add jobs that were settled
            if let Some(date_settled) = analysis.date_settled() {
                // only add jobs that were settled within the date range
                if (from_dt.is_none() || date_settled >= from_dt.unwrap())
                    && (to_dt.is_none() || date_settled <= to_dt.unwrap())
                {
                    let kind = analysis.kind.into_int();
                    global_tracker.add_job(
                        &analyzed,
                        kind,
                        &analysis.timestamps,
                        analysis.loss_timestamp,
                    );
                    rep_specific_trackers
                        .entry(analyzed.job.sales_rep.clone())
                        .or_insert_with(build_job_tracker)
                        .add_job(&analyzed, kind, &analysis.timestamps, analysis.loss_timestamp);
                }
            }
        }
        let sales_rep_errors: &mut Vec<_> =
            red_flags.entry(analyzed.job.sales_rep.clone()).or_default();
        for error in errors {
            sales_rep_errors.push((analyzed.clone(), error));
        }
    }

    ProcessJobsResult { global_tracker, rep_specific_trackers, red_flags }
}

type JobTracker3x5 =
    JobTracker<{ JobKind::NUM_VARIANTS }, { Milestone::NUM_VARIANTS }, Rc<AnalyzedJob>>;

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
    let num_appts = tracker.calc_stats(Milestone::AppointmentMade.into_int(), &[iwc, iwo, ret]).total.len();
    let num_installs = tracker.calc_stats(Milestone::Installed.into_int(), &[iwc, iwo, ret]).total.len();
    let (losses, avg_loss_time) = tracker.calc_stats_of_loss();
    let loss_rate = if num_appts == 0 { None } else { Some(losses.len() as f64 / num_appts as f64) };

    let num_insure_appts = tracker.calc_stats(Milestone::AppointmentMade.into_int(), &[iwc, iwo]).total.len();

    // from appt to contingency (insurance)
    let (appt_continge_total, appt_continge_conv, appt_continge_time) = {
        let job_tracker::Bucket { achieved, cum_achieve_time, .. } = tracker.get_bucket(iwc, Milestone::ContingencySigned.into_int()).unwrap();
        let num_achieved = achieved.len();
        let rate = if num_insure_appts == 0 { None } else { Some(num_achieved as f64 / num_insure_appts as f64) };
        let time = if num_achieved == 0 { TimeDelta::zero() } else { *cum_achieve_time / num_achieved.try_into().unwrap() };
        (achieved, rate, time)
    };

    // from appt to contract (insurance)
    let (appt_contract_insure_total, appt_contract_insure_conv, appt_contract_insure_time) = {
        let job_tracker::Bucket { achieved, cum_achieve_time, .. } = tracker.get_bucket(iwo, Milestone::ContractSigned.into_int()).unwrap();
        let num_achieved = achieved.len();
        let rate = if num_insure_appts == 0 { None } else { Some(num_achieved as f64 / num_insure_appts as f64) };
        let time = if num_achieved == 0 { TimeDelta::zero() } else { *cum_achieve_time / num_achieved.try_into().unwrap() };
        (achieved, rate, time)
    };

    // from contingency to contract (insurance)
    let CalcStatsResult {
        total: continge_contract_total,
        conversion_rate: continge_contract_conv,
        average_time_to_achieve: continge_contract_time,
    } = tracker.calc_stats(Milestone::ContractSigned.into_int(), &[iwc]);

    // from appointment to contract (retail)
    let CalcStatsResult {
        total: appt_contract_retail_total,
        conversion_rate: appt_contract_retail_conv,
        average_time_to_achieve: appt_contract_retail_time,
    } = tracker.calc_stats(Milestone::ContractSigned.into_int(), &[ret]);

    // from contract to install (insurance)
    let CalcStatsResult {
        total: install_insure_total,
        conversion_rate: install_insure_conv,
        average_time_to_achieve: install_insure_time,
    } = tracker.calc_stats(Milestone::Installed.into_int(), &[iwc, iwo]);

    // from contract to install (insurance)
    let CalcStatsResult {
        total: install_retail_total,
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
    fn into_list_of_job_nums(jobs: &[Rc<AnalyzedJob>]) -> String {
        jobs.iter().map(|job| job.job.job_number.as_deref().unwrap_or_else(|| &job.job.jnid)).collect::<Vec<_>>().join(", ")
    }

    format!(
        "Appts {} | Installed {}\n\
        All Losses:                   Rate {} | Total {:2} | Avg Time {:.2} days\n\
        -   {} \n\
        (I) Appt to Contingency:      Rate {} | Total {:2} | Avg Time {:.2} days\n\
        -   {} \n\
        (I) Appt to Contract:         Rate {} | Total {:2} | Avg Time {:.2} days\n\
        -   {} \n\
        (I) Contingency to Contract:  Rate {} | Total {:2} | Avg Time {:.2} days\n\
        -   {} \n\
        (R) Appt to Contract:         Rate {} | Total {:2} | Avg Time {:.2} days\n\
        -   {} \n\
        (I) Contract to Installation: Rate {} | Total {:2} | Avg Time {:.2} days\n\
        -   {} \n\
        (R) Contract to Installation: Rate {} | Total {:2} | Avg Time {:.2} days\n\
        -   {} \n",
        num_appts, num_installs,
        percent_or_na(loss_rate), losses.len(), into_days(avg_loss_time),
        into_list_of_job_nums(&losses),
        percent_or_na(appt_continge_conv), appt_continge_total.len(), into_days(appt_continge_time),
        into_list_of_job_nums(&appt_continge_total),
        percent_or_na(appt_contract_insure_conv), appt_contract_insure_total.len(), into_days(appt_contract_insure_time),
        into_list_of_job_nums(&appt_contract_insure_total),
        percent_or_na(continge_contract_conv), continge_contract_total.len(), into_days(continge_contract_time),
        into_list_of_job_nums(&continge_contract_total),
        percent_or_na(appt_contract_retail_conv), appt_contract_retail_total.len(), into_days(appt_contract_retail_time),
        into_list_of_job_nums(&appt_contract_retail_total),
        percent_or_na(install_insure_conv), install_insure_total.len(), into_days(install_insure_time),
        into_list_of_job_nums(&install_insure_total),
        percent_or_na(install_retail_conv), install_retail_total.len(), into_days(install_retail_time),
        into_list_of_job_nums(&install_retail_total),
    )
}
