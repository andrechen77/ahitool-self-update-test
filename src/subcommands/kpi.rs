use std::collections::BTreeMap;
use std::fmt::Display;
use std::path::Path;

use crate::apis::job_nimbus;
use anyhow::Context;
use anyhow::Result;
use chrono::Datelike as _;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::TimeZone as _;
use chrono::Utc;

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

    /// The format in which to print the output.
    #[arg(long, value_enum, default_value = "human")]
    format: OutputFormat,

    /// The directory to write the output to. "-" or unspecified will write
    /// concatenated file contents to stdout.
    #[arg(short, long, default_value = "-")]
    output: Option<String>,
}

#[derive(Debug, clap::ValueEnum, Clone, Copy, Eq, PartialEq)]
enum OutputFormat {
    /// Prints a set of human-readable .txt files into the output directory (or
    /// into stdout). Each file corresponds to a sales rep's stats or red flags.
    Human,
    /// Prints a set of CSV files into the output directory. Each file
    /// corresponds to a sales rep's stats, and there is also a CSV file for
    /// red flags.
    Csv,
}

pub fn main(api_key: &str, args: Args) -> Result<()> {
    let Args { filter_filename, from_date, to_date, format, output } = args;

    let filter = if let Some(filter_filename) = filter_filename {
        Some(std::fs::read_to_string(filter_filename)?)
    } else {
        None
    };
    let jobs = job_nimbus::get_all_jobs_from_job_nimbus(&api_key, filter.as_deref())?;

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

    let (trackers, red_flags) = processing::process_jobs(jobs.into_iter(), (from_date, to_date));
    let tracker_stats = trackers
        .into_iter()
        .map(|(rep, tracker)| (rep, processing::calculate_job_tracker_stats(&tracker)))
        .filter(|(_, stats)| stats.appt_count > 0)
        .collect::<BTreeMap<_, _>>();

    let output = output.filter(|s| s != "-");
    let output = output.as_deref().map(|path| Path::new(path));
    match format {
        OutputFormat::Human => output::print_report_human(&tracker_stats, &red_flags, output)?,
        OutputFormat::Csv => output::print_report_csv(&tracker_stats, &red_flags, output)?,
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum KpiSubject {
    Global,
    SalesRep(String),
    UnknownSalesRep,
}
impl Display for KpiSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KpiSubject::Global => write!(f, "[Global]"),
            KpiSubject::SalesRep(name) => write!(f, "{}", name),
            KpiSubject::UnknownSalesRep => write!(f, "[Unknown]"),
        }
    }
}

mod processing {
    use std::{collections::HashMap, rc::Rc};

    use tracing::info;

    use crate::{
        job_tracker::{self, CalcStatsResult, JobTracker},
        jobs::{
            self, AnalyzedJob, Job, JobAnalysisError, JobKind, Milestone, TimeDelta, Timestamp,
        },
    };

    use super::KpiSubject;

    pub type TrackersAndFlags = (
        HashMap<KpiSubject, JobTracker3x5>,
        HashMap<KpiSubject, Vec<(Rc<AnalyzedJob>, JobAnalysisError)>>,
    );

    pub fn process_jobs(
        jobs: impl Iterator<Item = Job>,
        (from_dt, to_dt): (Option<Timestamp>, Option<Timestamp>),
    ) -> TrackersAndFlags {
        info!(
            "Processing jobs settled between {} and {}",
            from_dt.map(|dt| dt.to_string()).as_deref().unwrap_or("the beginning of time"),
            to_dt.map(|dt| dt.to_string()).as_deref().unwrap_or("the end of time")
        );

        let mut trackers = HashMap::new();
        let mut red_flags = HashMap::new();
        for job in jobs {
            let (analyzed, errors) = jobs::analyze_job(job);
            let analyzed = Rc::new(analyzed);
            let target = match analyzed.job.sales_rep.clone() {
                Some(name) => KpiSubject::SalesRep(name),
                None => KpiSubject::UnknownSalesRep,
            };
            if let AnalyzedJob { analysis: Some(analysis), .. } = analyzed.as_ref() {
                // only add jobs that were settled
                if let Some(date_settled) = analysis.date_settled() {
                    // only add jobs that were settled within the date range
                    if (from_dt.is_none() || date_settled >= from_dt.unwrap())
                        && (to_dt.is_none() || date_settled <= to_dt.unwrap())
                    {
                        let kind = analysis.kind.into_int();
                        trackers
                            .entry(KpiSubject::Global)
                            .or_insert_with(build_job_tracker)
                            .add_job(
                                &analyzed,
                                kind,
                                &analysis.timestamps,
                                analysis.loss_timestamp,
                            );
                        trackers.entry(target.clone()).or_insert_with(build_job_tracker).add_job(
                            &analyzed,
                            kind,
                            &analysis.timestamps,
                            analysis.loss_timestamp,
                        );
                    }
                }
            }

            if !errors.is_empty() {
                let sales_rep_errors: &mut Vec<_> = red_flags.entry(target).or_default();
                for error in errors {
                    sales_rep_errors.push((analyzed.clone(), error));
                }
            }
        }

        (trackers, red_flags)
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

    #[derive(Debug)]
    pub struct JobTrackerStats {
        pub appt_count: usize,
        pub install_count: usize,
        pub loss_conv: ConversionStats,
        pub appt_continge_conv: ConversionStats,
        pub appt_contract_insure_conv: ConversionStats,
        pub continge_contract_conv: ConversionStats,
        pub appt_contract_retail_conv: ConversionStats,
        pub install_insure_conv: ConversionStats,
        pub install_retail_conv: ConversionStats,
    }

    #[derive(Debug)]
    pub struct ConversionStats {
        /// All the jobs that made the conversion.
        pub achieved: Vec<Rc<AnalyzedJob>>,
        /// The rate of conversion. `None` if no jobs made the conversion.
        pub conversion_rate: Option<f64>,
        /// The average amount of time for a successful conversion. Zero if no
        /// jobs made the conversion.
        pub average_time_to_achieve: TimeDelta,
    }

    pub fn calculate_job_tracker_stats(tracker: &JobTracker3x5) -> JobTrackerStats {
        let iwc = JobKind::InsuranceWithContingency.into_int(); // "insurance with contingency"
        let iwo = JobKind::InsuranceWithoutContingency.into_int(); // "insurance without contingency"
        let ret = JobKind::Retail.into_int(); // "retail"

        // some basic stats
        let appt_count = tracker
            .calc_stats(Milestone::AppointmentMade.into_int(), &[iwc, iwo, ret])
            .achieved
            .len();
        let install_count =
            tracker.calc_stats(Milestone::Installed.into_int(), &[iwc, iwo, ret]).achieved.len();

        let loss_conv = {
            let (achieved, average_time_to_achieve) = tracker.calc_stats_of_loss();
            let conversion_rate = if appt_count == 0 {
                None
            } else {
                Some(achieved.len() as f64 / appt_count as f64)
            };
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
}

mod output {
    use std::{
        io::{BufWriter, Write},
        path::Path,
        rc::Rc,
    };

    use crate::jobs::{AnalyzedJob, JobAnalysisError, TimeDelta};

    use super::{processing::JobTrackerStats, KpiSubject};

    pub fn print_report_human<'a>(
        tracker_stats: impl IntoIterator<Item = (&'a KpiSubject, &'a JobTrackerStats)>,
        red_flags: impl IntoIterator<
            Item = (&'a KpiSubject, &'a Vec<(Rc<AnalyzedJob>, JobAnalysisError)>),
        >,
        output_dir: Option<&Path>,
    ) -> std::io::Result<()> {
        // make sure that output_dir exists
        if let Some(output_dir) = output_dir {
            std::fs::create_dir_all(output_dir)?;
        }

        for (rep, stats) in tracker_stats {
            // create the file for this rep
            let mut out: Box<dyn Write> = if let Some(output_dir) = output_dir {
                Box::new(BufWriter::new(
                    std::fs::File::create(output_dir.join(format!("rep-{}-stats.txt", rep)))
                        .expect("the directory should exist"),
                ))
            } else {
                Box::new(std::io::stdout())
            };

            // print the report into the file
            writeln!(out, "Tracker for {}: ================", rep)?;
            writeln!(out, "Appts {} | Installed {}", stats.appt_count, stats.install_count)?;
            for (name, conv_stats) in [
                ("All Losses", &stats.loss_conv),
                ("(I) Appt to Contingency", &stats.appt_continge_conv),
                ("(I) Appt to Contract", &stats.appt_contract_insure_conv),
                ("(I) Contingency to Contract", &stats.continge_contract_conv),
                ("(R) Appt to Contract", &stats.appt_contract_retail_conv),
                ("(I) Contract to Installation", &stats.install_insure_conv),
                ("(R) Contract to Installation", &stats.install_retail_conv),
            ] {
                writeln!(
                    out,
                    "{:30}    Rate {} | Total {:2} | Avg Time {:.2} days",
                    name,
                    percent_or_na(conv_stats.conversion_rate),
                    conv_stats.achieved.len(),
                    into_days(conv_stats.average_time_to_achieve),
                )?;
                if *rep != KpiSubject::Global {
                    writeln!(out, "    - {}", into_list_of_job_nums(&conv_stats.achieved))?;
                }
            }
            writeln!(out, "")?;
            out.flush()?;
        }

        let mut out: Box<dyn Write> = if let Some(output_dir) = output_dir {
            Box::new(BufWriter::new(
                std::fs::File::create(output_dir.join("red-flags.txt"))
                    .expect("the directory should exist"),
            ))
        } else {
            Box::new(std::io::stdout())
        };
        for (rep, red_flags) in red_flags {
            writeln!(out, "Red flags for {}: ===============", rep)?;
            for (job, err) in red_flags {
                writeln!(
                    out,
                    "{}: {}",
                    job.job.job_number.as_deref().unwrap_or("unknown job #"),
                    err
                )?;
            }
            writeln!(out, "")?;
        }
        out.flush()?;

        Ok(())
    }

    pub fn print_report_csv<'a>(
        tracker_stats: impl IntoIterator<Item = (&'a KpiSubject, &'a JobTrackerStats)>,
        red_flags: impl IntoIterator<
            Item = (&'a KpiSubject, &'a Vec<(Rc<AnalyzedJob>, JobAnalysisError)>),
        >,
        output_dir: Option<&Path>,
    ) -> std::io::Result<()> {
        // make sure that output_dir exists
        if let Some(output_dir) = output_dir {
            std::fs::create_dir_all(output_dir)?;
        }

        for (rep, stats) in tracker_stats {
            // create the file for this rep
            let out: Box<dyn Write> = if let Some(output_dir) = output_dir {
                Box::new(BufWriter::new(
                    std::fs::File::create(output_dir.join(format!("rep-{}-stats.csv", rep)))
                        .expect("the directory should exist"),
                ))
            } else {
                Box::new(std::io::stdout())
            };
            let mut out = csv::Writer::from_writer(out);

            out.write_record(&["Conversion", "Rate", "Total", "Avg Time (days)", "Jobs"])?;
            for (name, conv_stats) in [
                ("All Losses", &stats.loss_conv),
                ("(I) Appt to Contingency", &stats.appt_continge_conv),
                ("(I) Appt to Contract", &stats.appt_contract_insure_conv),
                ("(I) Contingency to Contract", &stats.continge_contract_conv),
                ("(R) Appt to Contract", &stats.appt_contract_retail_conv),
                ("(I) Contract to Installation", &stats.install_insure_conv),
                ("(R) Contract to Installation", &stats.install_retail_conv),
            ] {
                out.write_record(&[
                    name,
                    &percent_or_na(conv_stats.conversion_rate),
                    &conv_stats.achieved.len().to_string(),
                    &into_days(conv_stats.average_time_to_achieve).to_string(),
                    &into_list_of_job_nums(&conv_stats.achieved),
                ])?;
            }
            out.write_record(&[
                "Appts",
                &stats.appt_count.to_string(),
                "",
                "Installed",
                &stats.install_count.to_string(),
            ])?;

            out.flush()?;
        }

        let out: Box<dyn Write> = if let Some(output_dir) = output_dir {
            Box::new(BufWriter::new(
                std::fs::File::create(output_dir.join("red-flags.csv"))
                    .expect("the directory should exist"),
            ))
        } else {
            Box::new(std::io::stdout())
        };
        let mut out = csv::Writer::from_writer(out);
        out.write_record(&["Sales Rep", "Job Number", "Error"])?;
        for (rep, red_flags) in red_flags {
            for (job, err) in red_flags {
                out.write_record(&[
                    &rep.to_string(),
                    job.job.job_number.as_deref().unwrap_or("unknown job #"),
                    &err.to_string(),
                ])?;
            }
        }
        out.flush()?;

        Ok(())
    }

    fn into_days(time: TimeDelta) -> f64 {
        const SECONDS_PER_DAY: f64 = 86400.0;
        time.num_seconds() as f64 / SECONDS_PER_DAY
    }
    fn percent_or_na(rate: Option<f64>) -> String {
        rate.map(|r| format!("{:6.2}%", r * 100.0)).unwrap_or_else(|| "    N/A".to_owned())
    }
    fn into_list_of_job_nums(jobs: &[Rc<AnalyzedJob>]) -> String {
        jobs.iter()
            .map(|job| job.job.job_number.as_deref().unwrap_or_else(|| &job.job.jnid))
            .collect::<Vec<_>>()
            .join(", ")
    }
}
