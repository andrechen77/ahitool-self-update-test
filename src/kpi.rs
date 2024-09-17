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

    /// The file to write the output to. "-" will write to stdout.
    #[arg(short, default_value = "-")]
    output: String,
}

pub fn main(api_key: &str, args: Args) -> Result<()> {
    let Args { filter_filename, from_date, to_date, output } = args;
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
    let global_tracker_stats = calculate_job_tracker_stats(&global_tracker);
    let rep_tracker_stats = rep_specific_trackers
        .into_iter()
        .map(|(rep, tracker)| (rep, calculate_job_tracker_stats(&tracker)))
        .collect::<HashMap<_, _>>();

    let mut output_writer: Box<dyn std::io::Write> = match output.as_str() {
        "-" => Box::new(std::io::stdout()),
        path => Box::new(std::fs::File::create(path)?),
    };

    writeln!(
        output_writer,
        "Global Tracker: ================\n{}",
        format_job_tracker_stats(&global_tracker_stats)
    )?;
    for (rep, stats) in rep_tracker_stats {
        writeln!(
            output_writer,
            "\nTracker for {}: ================\n{}",
            rep.unwrap_or("Unknown Sales Rep".to_owned()),
            format_job_tracker_stats(&stats)
        )?;
    }
    for (rep, red_flags) in red_flags {
        writeln!(
            output_writer,
            "\nRed flags for {}: ===============",
            rep.unwrap_or("Unknown Sales Rep".to_owned())
        )?;
        for (job, err) in red_flags {
            writeln!(
                output_writer,
                "{}: {}",
                job.job.job_number.as_deref().unwrap_or("unknown job #"),
                err
            )?;
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

struct JobTrackerStats {
    appt_count: usize,
    install_count: usize,
    loss_conv: ConversionStats,
    appt_continge_conv: ConversionStats,
    appt_contract_insure_conv: ConversionStats,
    continge_contract_conv: ConversionStats,
    appt_contract_retail_conv: ConversionStats,
    install_insure_conv: ConversionStats,
    install_retail_conv: ConversionStats,
}

struct ConversionStats {
    /// All the jobs that made the conversion.
    achieved: Vec<Rc<AnalyzedJob>>,
    /// The rate of conversion. `None` if no jobs made the conversion.
    conversion_rate: Option<f64>,
    /// The average amount of time for a successful conversion. Zero if no
    /// jobs made the conversion.
    average_time_to_achieve: TimeDelta,
}

fn calculate_job_tracker_stats(tracker: &JobTracker3x5) -> JobTrackerStats {
    let iwc = JobKind::InsuranceWithContingency.into_int(); // "insurance with contingency"
    let iwo = JobKind::InsuranceWithoutContingency.into_int(); // "insurance without contingency"
    let ret = JobKind::Retail.into_int(); // "retail"

    // some basic stats
    let appt_count =
        tracker.calc_stats(Milestone::AppointmentMade.into_int(), &[iwc, iwo, ret]).achieved.len();
    let install_count =
        tracker.calc_stats(Milestone::Installed.into_int(), &[iwc, iwo, ret]).achieved.len();

    let loss_conv = {
        let (achieved, average_time_to_achieve) = tracker.calc_stats_of_loss();
        let conversion_rate =
            if appt_count == 0 { None } else { Some(achieved.len() as f64 / appt_count as f64) };
        ConversionStats { achieved, conversion_rate, average_time_to_achieve }
    };

    let num_insure_appts =
        tracker.calc_stats(Milestone::AppointmentMade.into_int(), &[iwc, iwo]).achieved.len();

    // calculate stats for each conversion
    let appt_continge_conv = {
        let job_tracker::Bucket { achieved, cum_achieve_time, .. } =
            tracker.get_bucket(iwc, Milestone::ContingencySigned.into_int()).unwrap();
        let num_achieved = achieved.len();
        let conversion_rate = if num_insure_appts == 0 {
            None
        } else {
            Some(num_achieved as f64 / num_insure_appts as f64)
        };
        let average_time_to_achieve = if num_achieved == 0 {
            TimeDelta::zero()
        } else {
            *cum_achieve_time / num_achieved.try_into().unwrap()
        };
        ConversionStats { achieved: achieved.clone(), conversion_rate, average_time_to_achieve }
    };
    let appt_contract_insure_conv = {
        let job_tracker::Bucket { achieved, cum_achieve_time, .. } =
            tracker.get_bucket(iwo, Milestone::ContractSigned.into_int()).unwrap();
        let num_achieved = achieved.len();
        let conversion_rate = if num_insure_appts == 0 {
            None
        } else {
            Some(num_achieved as f64 / num_insure_appts as f64)
        };
        let average_time_to_achieve = if num_achieved == 0 {
            TimeDelta::zero()
        } else {
            *cum_achieve_time / num_achieved.try_into().unwrap()
        };
        ConversionStats { achieved: achieved.clone(), conversion_rate, average_time_to_achieve }
    };
    let continge_contract_conv = {
        let CalcStatsResult { achieved, conversion_rate, average_time_to_achieve } =
            tracker.calc_stats(Milestone::ContractSigned.into_int(), &[iwc]);
        ConversionStats { achieved, conversion_rate, average_time_to_achieve }
    };
    let appt_contract_retail_conv = {
        let CalcStatsResult { achieved, conversion_rate, average_time_to_achieve } =
            tracker.calc_stats(Milestone::ContractSigned.into_int(), &[ret]);
        ConversionStats { achieved, conversion_rate, average_time_to_achieve }
    };
    let install_insure_conv = {
        let CalcStatsResult { achieved, conversion_rate, average_time_to_achieve } =
            tracker.calc_stats(Milestone::Installed.into_int(), &[iwc, iwo]);
        ConversionStats { achieved, conversion_rate, average_time_to_achieve }
    };
    let install_retail_conv = {
        let CalcStatsResult { achieved, conversion_rate, average_time_to_achieve } =
            tracker.calc_stats(Milestone::Installed.into_int(), &[ret]);
        ConversionStats { achieved, conversion_rate, average_time_to_achieve }
    };

    JobTrackerStats {
        appt_count,
        install_count,
        loss_conv,
        appt_continge_conv,
        appt_contract_insure_conv,
        continge_contract_conv,
        appt_contract_retail_conv,
        install_insure_conv,
        install_retail_conv,
    }
}

#[rustfmt::skip] // for the big format! at the end
fn format_job_tracker_stats(tracker_stats: &JobTrackerStats) -> String {
    let JobTrackerStats {
        appt_count,
        install_count,
        loss_conv,
        appt_continge_conv,
        appt_contract_insure_conv,
        continge_contract_conv,
        appt_contract_retail_conv,
        install_insure_conv,
        install_retail_conv,
    } = tracker_stats;

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

    let mut result = String::new();
    use std::fmt::Write as _;
    writeln!(&mut result, "Appts {} | Installed {}", appt_count, install_count).unwrap();
    for (name, conv_stats) in [
        ("All Losses", loss_conv),
        ("(I) Appt to Contingency", appt_continge_conv),
        ("(I) Appt to Contract", appt_contract_insure_conv),
        ("(I) Contingency to Contract", continge_contract_conv),
        ("(R) Appt to Contract", appt_contract_retail_conv),
        ("(I) Contract to Installation", install_insure_conv),
        ("(R) Contract to Installation", install_retail_conv),
    ] {
        writeln!(
            &mut result,
            "{:30}    Rate {} | Total {:2} | Avg Time {:.2} days\n    {}",
            name,
            percent_or_na(conv_stats.conversion_rate),
            conv_stats.achieved.len(),
            into_days(conv_stats.average_time_to_achieve),
            into_list_of_job_nums(&conv_stats.achieved),
        ).unwrap();
    }
    result
}
