mod oauth;
pub mod spreadsheet;

use anyhow::bail;
pub use oauth::run_with_credentials;
pub use oauth::Token;
use oauth2::TokenResponse as _;
use serde::Deserialize;
use spreadsheet::Spreadsheet;

const ENDPOINT_SPREADSHEETS: &str = "https://sheets.googleapis.com/v4/spreadsheets";

/// If successful, returns the URL of the created sheet.
pub async fn create_sheet(creds: &Token, spreadsheet: &Spreadsheet) -> anyhow::Result<String> {
    let url = reqwest::Url::parse(ENDPOINT_SPREADSHEETS)?;
    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .bearer_auth(creds.access_token().secret())
        .json(&spreadsheet)
        .send()
        .await?;

    if !response.status().is_success() {
        bail!("Request to create sheet failed with status code: {}", response.status());
    }

    #[derive(Deserialize)]
    struct ApiResponse {
        #[serde(rename = "spreadsheetUrl")]
        spreadsheet_url: String,
    }
    let spreadsheet_json: ApiResponse = response.json().await?;
    Ok(spreadsheet_json.spreadsheet_url)
}
