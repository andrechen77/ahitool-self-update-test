use std::{collections::HashMap, io::Write};

use chrono::Utc;
use tracing::{info, warn};

use crate::{
    apis::{
        google_sheets::{
            self,
            spreadsheet::{
                CellData, ExtendedValue, GridData, RowData, Sheet, SheetProperties, Spreadsheet,
                SpreadsheetProperties,
            },
        },
        job_nimbus,
    },
    jobs::{Job, Status},
};

#[derive(clap::Args, Debug)]
pub struct Args {
    /// The format in which to print the output.
    #[arg(long, value_enum, default_value = "human")]
    format: OutputFormat,

    /// The file to write the output to. "-" or unspecified will write to
    /// stdout. This option is ignored with `--format google-sheets`.
    #[arg(short, long, default_value = None)]
    output: Option<String>,
}

#[derive(Debug, clap::ValueEnum, Clone, Copy, Eq, PartialEq)]
enum OutputFormat {
    /// Prints a human-readable report into the output file.
    Human,
    /// Prints a CSV file into the output file.
    Csv,
    /// Creates a new Google Sheet on the user's Google Drive (requires OAuth
    /// authorization), and outputs and opens a link to the new Google Sheet.
    GoogleSheets,
}

const CATEGORIES_WE_CARE_ABOUT: &[Status] = &[
    Status::PendingPayments,
    Status::PostInstallSupplementPending,
    Status::JobsInProgress,
    Status::FinalWalkAround,
    Status::SubmitCoc,
    Status::PunchList,
    Status::JobCompleted,
    Status::Collections,
];

struct AccRecvableData<'a> {
    total: i32,
    categorized_jobs: HashMap<Status, (i32, Vec<&'a Job>)>,
}

pub fn main(api_key: &str, args: Args) -> anyhow::Result<()> {
    let Args { output, format } = args;
    if format == OutputFormat::GoogleSheets && output.is_some() {
        warn!("The `--output` option will be ignored due to `--format google-sheets`");
    }

    let jobs = job_nimbus::get_all_jobs_from_job_nimbus(&api_key, None)?;

    let mut results = AccRecvableData { total: 0, categorized_jobs: HashMap::new() };
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

    let output_writer: Box<dyn Write> = match output.as_deref() {
        Some("-") | None => Box::new(std::io::stdout()),
        Some(path) => Box::new(std::fs::File::create(path)?),
    };

    match format {
        OutputFormat::Human => print_human(&results, output_writer)?,
        OutputFormat::Csv => print_csv(&results, output_writer)?,
        OutputFormat::GoogleSheets => {
            create_google_sheet_and_print_link(&results)?;
        }
    }

    Ok(())
}

fn print_human(results: &AccRecvableData, mut writer: impl Write) -> std::io::Result<()> {
    let mut zero_amt_jobs = Vec::new();

    writeln!(writer, "Total: ${}", results.total as f64 / 100.0)?;
    for (status, (category_total, jobs)) in &results.categorized_jobs {
        writeln!(writer, "    - {}: total ${}", status, *category_total as f64 / 100.0)?;
        for job in jobs {
            if job.amt_receivable == 0 {
                zero_amt_jobs.push(job);
                continue;
            }

            let name = job.job_name.as_deref().unwrap_or("");
            let number = job.job_number.as_deref().unwrap_or("Unknown Job Number");
            let amount_receivable = job.amt_receivable as f64 / 100.0;
            let days_in_status = Utc::now().signed_duration_since(job.status_mod_date).num_days();
            writeln!(
                writer,
                "        - {} (#{}): ${:.2} ({} days)",
                name, number, amount_receivable, days_in_status
            )?;
        }
    }

    writeln!(writer, "Jobs with $0 receivable:")?;
    for job in zero_amt_jobs {
        let name = job.job_name.as_deref().unwrap_or("");
        let number = job.job_number.as_deref().unwrap_or("Unknown Job Number");
        let days_in_status = Utc::now().signed_duration_since(job.status_mod_date).num_days();
        writeln!(
            writer,
            "    - {} (#{}): ({} for {} days)",
            name, number, job.status, days_in_status
        )?;
    }

    Ok(())
}

fn print_csv(results: &AccRecvableData, writer: impl Write) -> std::io::Result<()> {
    let mut writer = csv::Writer::from_writer(writer);
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
    Ok(())
}

fn create_google_sheet_and_print_link(results: &AccRecvableData) -> anyhow::Result<()> {
    fn mk_row(cells: impl IntoIterator<Item = ExtendedValue>) -> RowData {
        RowData {
            values: cells
                .into_iter()
                .map(|cell| CellData { user_entered_value: Some(cell) })
                .collect(),
        }
    }

    let mut rows = Vec::new();
    rows.push(mk_row([
        ExtendedValue::StringValue("Job Name".to_string()),
        ExtendedValue::StringValue("Job Number".to_string()),
        ExtendedValue::StringValue("Job Status".to_string()),
        ExtendedValue::StringValue("Amount".to_string()),
        ExtendedValue::StringValue("Days In Status".to_string()),
    ]));
    for (_status, (_category_total, jobs)) in &results.categorized_jobs {
        for job in jobs {
            let name = job.job_name.as_deref().unwrap_or("");
            let number = job.job_number.as_deref().unwrap_or("Unknown Job Number");
            let status = job.status.to_string();
            let amount_receivable = (job.amt_receivable as f64) / 100.0;
            let days_in_status = Utc::now().signed_duration_since(job.status_mod_date).num_days();
            rows.push(mk_row([
                ExtendedValue::StringValue(name.to_owned()),
                ExtendedValue::StringValue(number.to_owned()),
                ExtendedValue::StringValue(status),
                ExtendedValue::NumberValue(amount_receivable),
                ExtendedValue::NumberValue(days_in_status as f64),
            ]));
        }
    }

    let spreadsheet = Spreadsheet {
        properties: SpreadsheetProperties {
            title: Some(format!("Accounts Receivable Report ({})", Utc::now())),
        },
        sheets: Some(vec![Sheet {
            properties: SheetProperties {
                title: Some("Accounts Receivable".to_string()),
                ..Default::default()
            },
            data: GridData { start_row: 0, start_column: 0, row_data: rows },
        }]),
        ..Default::default()
    };

    let creds = google_sheets::get_credentials()?;
    let url = google_sheets::create_sheet(&creds, &spreadsheet)?;
    info!("Created new Google Sheet at {}", url);
    Ok(())
}
