use anyhow::{bail, Result};
use reqwest::{self, blocking::Response, header::CONTENT_TYPE};
use serde::Deserialize;

use crate::jobs::Job;

const ENDPOINT_JOBS: &str = "https://app.jobnimbus.com/api1/jobs";

fn request_from_job_nimbus(
    api_key: &str,
    num_jobs: usize,
    filter: Option<&str>,
) -> Result<Response> {
    let url = reqwest::Url::parse(ENDPOINT_JOBS)?;
    let client = reqwest::blocking::Client::new();
    let mut request = client
        .get(url.clone())
        .bearer_auth(&api_key)
        .header(CONTENT_TYPE, "application/json")
        .query(&[("size", num_jobs.to_string().as_str())]);
    if let Some(filter) = filter {
        request = request.query(&[("filter", filter)]);
    }
    let response = request.send()?;
    if !response.status().is_success() {
        bail!("Request failed with status code: {}", response.status());
    }
    Ok(response)
}

// blocking
pub fn get_all_jobs_from_job_nimbus(api_key: &str, filter: Option<&str>) -> Result<Vec<Job>> {
    use serde_json::Value;
    #[derive(Deserialize)]
    struct ApiResponse {
        count: u64,
        results: Vec<Value>,
    }

    eprintln!("getting all jobs from JobNimbus");

    // make a request to find out the number of jobs
    let response = request_from_job_nimbus(api_key, 1, filter)?;
    let response: ApiResponse = response.json()?;
    let count = response.count as usize;

    eprintln!("detected {} jobs in JobNimbus", count);

    // make a request to actually get those jobs
    let response = request_from_job_nimbus(api_key, count, filter)?;
    let response: ApiResponse = response.json()?;
    eprintln!("recieved {} jobs from JobNimbus", response.count);
    assert_eq!(response.count as usize, count);

    let results: Result<Vec<_>, _> = response.results.into_iter().map(Job::try_from).collect();
    Ok(results?)
}
