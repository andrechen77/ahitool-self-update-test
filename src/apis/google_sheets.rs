mod oauth;
pub mod spreadsheet;

use anyhow::anyhow;
use hyper::StatusCode;
pub use oauth::run_with_credentials;
pub use oauth::Token;
use oauth::TryWithCredentialsError;
use oauth2::TokenResponse as _;
use serde::Deserialize;
use spreadsheet::Spreadsheet;

const ENDPOINT_SPREADSHEETS: &str = "https://sheets.googleapis.com/v4/spreadsheets";

/// If successful, returns the URL of the created sheet.
pub async fn create_sheet(creds: &Token, spreadsheet: &Spreadsheet) -> Result<String, TryWithCredentialsError> {
    let url = reqwest::Url::parse(ENDPOINT_SPREADSHEETS).expect("hardcoded URL should be valid");
    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .bearer_auth(creds.access_token().secret())
        .json(&spreadsheet)
        .send()
        .await
        .map_err(anyhow::Error::from)?;

    if !response.status().is_success() {
        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(TryWithCredentialsError::Unauthorized(anyhow!("Request to create sheet was unauthorized with status code: {}", response.status())));
        } else {
            return Err(TryWithCredentialsError::Other(anyhow!("Request to create sheet failed with status code: {}", response.status())));
        }
    }

    #[derive(Deserialize)]
    struct ApiResponse {
        #[serde(rename = "spreadsheetUrl")]
        spreadsheet_url: String,
    }
    let spreadsheet_json: ApiResponse = response.json().await.map_err(anyhow::Error::from)?;
    Ok(spreadsheet_json.spreadsheet_url)
}
