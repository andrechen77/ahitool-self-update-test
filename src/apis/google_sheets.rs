mod oauth;

use std::path::Path;

use anyhow::bail;
pub use oauth::Token;
use oauth2::TokenResponse as _;
use serde::Deserialize;
use serde_json::json;

const ENDPOINT_SPREADSHEETS: &str = "https://sheets.googleapis.com/v4/spreadsheets";

pub fn get_credentials() -> anyhow::Result<Token> {
    oauth::get_credentials_with_cache(Path::new(oauth::DEFAULT_CACHE_FILE))
}

/// If successful, returns the URL of the created sheet.
pub fn create_sheet(creds: &Token, sheet_name: &str) -> anyhow::Result<String> {
    let spreadsheet_json = json!({
        "properties": {
            "title": sheet_name,
        },
    });

    let url = reqwest::Url::parse(ENDPOINT_SPREADSHEETS)?;
    let client = reqwest::blocking::Client::new();
    let response = client
        .post(url)
        .bearer_auth(creds.access_token().secret())
        .json(&spreadsheet_json)
        .send()?;

    if !response.status().is_success() {
        bail!("Request to create sheet failed with status code: {}", response.status());
    }

    #[derive(Deserialize)]
    struct ApiResponse {
        #[serde(rename = "spreadsheetUrl")]
        spreadsheet_url: String,
    }
    let spreadsheet_json: ApiResponse = response.json()?;
    Ok(spreadsheet_json.spreadsheet_url)
}
