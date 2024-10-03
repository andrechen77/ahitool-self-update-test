mod oauth;
pub mod spreadsheet;

use std::borrow::Cow;
use std::collections::HashSet;
use std::fs::File;

use std::io::BufWriter;
use std::path::Path;

use anyhow::anyhow;
use hyper::StatusCode;
pub use oauth::run_with_credentials;
pub use oauth::Token;
use oauth::TryWithCredentialsError;
use oauth2::TokenResponse as _;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use spreadsheet::update::Request;
use spreadsheet::GridCoordinate;
use spreadsheet::SheetProperties;
use spreadsheet::Spreadsheet;
use tracing::debug;
use tracing::info;
use std::collections::HashMap;
use std::io::BufReader;
use tracing::trace;
use tracing::warn;

const ENDPOINT_SPREADSHEETS: &str = "https://sheets.googleapis.com/v4/spreadsheets";
const KNOWN_SHEETS_FILE: &str = "google_sheets.json";

/// Searches the known sheets file for an existing spreadsheet with the
/// specified key. Updates that spreadsheet with the specified data, or creates
/// a new spreadsheet in the user's Google Drive if it doesn't exist. Returns
/// the URL of the Google Sheet.
pub async fn create_or_write_spreadsheet(
    creds: &Token,
    nickname: SheetNickname,
    spreadsheet: Spreadsheet,
) -> Result<String, TryWithCredentialsError> {
    let known_sheet = match read_known_sheets_file(nickname) {
        Err(e) => {
            warn!("Failed to read known sheets file: {}", e);
            None
        }
        Ok(None) => None,
        Ok(Some(spreadsheet_id)) => Some(spreadsheet_id),
    };
    if let Some(spreadsheet_id) = known_sheet {
        info!("Found existing sheet with ID {}", spreadsheet_id);
        Ok(update_spreadsheet(creds, &spreadsheet_id, spreadsheet).await?)
    } else {
        info!("No existing spreadsheet found, creating a new one");
        Ok(create_spreadsheet(creds, nickname, spreadsheet).await?)
    }
}

/// Creates the specified spreadsheet in the user's Google Drive. Saves the
/// created spreadsheet ID under the specified nickname in the known sheets file
/// and return the URL of the created sheet.
pub async fn create_spreadsheet(
    creds: &Token,
    nickname: SheetNickname,
    spreadsheet: Spreadsheet,
) -> Result<String, TryWithCredentialsError> {
    let url = reqwest::Url::parse(ENDPOINT_SPREADSHEETS).expect("hardcoded URL should be valid");
    let client = reqwest::Client::new();
    trace!("Sending request to create sheet");
    let response = client
        .post(url)
        .bearer_auth(creds.access_token().secret())
        .json(&spreadsheet)
        .send()
        .await
        .map_err(anyhow::Error::from)?;

    if !response.status().is_success() {
        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(TryWithCredentialsError::Unauthorized(anyhow!(
                "Request to create sheet was unauthorized with status code: {}",
                response.status()
            )));
        } else {
            return Err(TryWithCredentialsError::Other(anyhow!(
                "Request to create sheet failed with status code: {}",
                response.status()
            )));
        }
    }

    #[derive(Deserialize)]
    struct ApiResponse {
        #[serde(rename = "spreadsheetId")]
        spreadsheet_id: String,
        #[serde(rename = "spreadsheetUrl")]
        spreadsheet_url: String,
    }
    let ApiResponse { spreadsheet_id, spreadsheet_url } =
        response.json().await.map_err(anyhow::Error::from)?;

    debug!(
        "Saving the spreadsheet under the nickname {}",
        serde_json::to_string(&nickname).expect("should work")
    );
    if let Err(e) = update_known_sheets_file(nickname, &spreadsheet_id) {
        warn!("Failed to update known sheets file: {}", e);
    };

    info!("Created Google Sheet at {}", spreadsheet_url);
    Ok(spreadsheet_url)
}

async fn update_spreadsheet(
    creds: &Token,
    spreadsheet_id: &str,
    spreadsheet: Spreadsheet,
) -> Result<String, TryWithCredentialsError> {
    let client = reqwest::Client::new();

    // get the current spreadsheet data so we can merge the new data with it
    let existing_spreadsheet: Spreadsheet = {
        let url = reqwest::Url::parse(&format!("{ENDPOINT_SPREADSHEETS}/{spreadsheet_id}"))
            .map_err(anyhow::Error::from)?;
        let request = client
            .get(url)
            .bearer_auth(creds.access_token().secret())
            .build()
            .map_err(anyhow::Error::from)?;
        let response = client.execute(request)
            .await
            .map_err(anyhow::Error::from)?;

        if !response.status().is_success() {
            if response.status() == StatusCode::UNAUTHORIZED {
                return Err(TryWithCredentialsError::Unauthorized(anyhow!(
                    "Request to get current sheet was unauthorized with status code: {}",
                    response.status()
                )));
            } else {
                return Err(TryWithCredentialsError::Other(anyhow!(
                    "Request to get current sheet failed with status code: {}",
                    response.status()
                )));
            }
        }

        response.json().await.map_err(anyhow::Error::from)?
    };

    // keep track of existing sheet IDs so we can update existing sheets, as
    // as well as delete sheets that we don't care about, as well as assign
    // sheet ids to new sheets without conflicts
    let mut title_to_sheet_id = HashMap::new();
    let mut existing_sheet_ids = HashSet::new();
    if let Some(sheets) = existing_spreadsheet.sheets {
        for sheet in sheets {
            let SheetProperties { sheet_id, title } = sheet.properties;
            if let (Some(sheet_id), Some(title)) = (sheet_id, title) {
                title_to_sheet_id.insert(title, sheet_id);
            }
            if let Some(sheet_id) = sheet_id {
                existing_sheet_ids.insert(sheet_id);
            }
        }
    }

    // prepare the correct JSON to send with the `batchUpdate` request. see
    // https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/batchUpdate
    let mut requests = Vec::new();

    // change the spreadsheet properties
    if spreadsheet.properties.title.is_some() {
        requests.push(Request::UpdateSpreadsheetProperties {
            properties: spreadsheet.properties,
            fields: "title",
        });
    }

    // update the content of the sheets
    if let Some(sheets) = spreadsheet.sheets {
        for sheet in sheets {
            if sheet.properties.sheet_id.is_some() {
                warn!("sheet ID is ignored when updating a spreadsheet; use the title instead");
            }
            let sheet_id = 'sheet_id: {
                if let Some(title) = &sheet.properties.title {
                    if let Some(sheet_id) = title_to_sheet_id.remove(title) {
                        // we would push a request to update the sheet
                        // properties here, but there are none to update, since
                        // sheet_id and title are the only fields we currently
                        // support and they are already known to match at this
                        // point

                        break 'sheet_id sheet_id;
                    }
                }
                // getting here means that the sheet does not have an existing
                // counterpart. create a new sheet

                // find a sheet ID that is not already in use
                let mut sheet_id = 0;
                while existing_sheet_ids.contains(&sheet_id) {
                    sheet_id += 1;
                }
                existing_sheet_ids.insert(sheet_id);

                // push a request to add a new sheet with the new id
                requests.push(Request::AddSheet {
                    properties: SheetProperties { sheet_id: Some(sheet_id), ..sheet.properties },
                });
                sheet_id
            };

            if let Some(grid_data) = sheet.data {
                // push a request to update the content of the sheet
                requests.push(Request::UpdateCells {
                    rows: grid_data.row_data,
                    fields: "userEnteredValue",
                    start: GridCoordinate {
                        sheet_id,
                        row_index: grid_data.start_row,
                        column_index: grid_data.start_column,
                    },
                })
            }
        }
    }

    // remove the sheets that don't exist anymore
    for (_title, sheet_id) in title_to_sheet_id {
        requests.push(Request::DeleteSheet { sheet_id });
    }

    // construct the final request body
    let request_body = json!({
        "requests": requests,
        "includeSpreadsheetInResponse": true,
        "responseIncludeGridData": false,
    });

    let url = reqwest::Url::parse(&format!("{ENDPOINT_SPREADSHEETS}/{spreadsheet_id}:batchUpdate"))
        .map_err(anyhow::Error::from)?;
    let request = client
        .post(url)
        .bearer_auth(creds.access_token().secret())
        .json(&request_body)
        .build()
        .map_err(anyhow::Error::from)?;
    let response = client.execute(request)
        .await
        .map_err(anyhow::Error::from)?;
    if !response.status().is_success() {
        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(TryWithCredentialsError::Unauthorized(anyhow!(
                "Request to update spreadsheet was unauthorized with status code: {}",
                response.status()
            )));
        } else {
            return Err(TryWithCredentialsError::Other(anyhow!(
                "Request to update spreadsheet failed with status code: {}",
                response.status()
            )));
        }
    }

    #[derive(Deserialize)]
    struct ApiResponse {
        replies: serde_json::Value,
        #[serde(rename = "updatedSpreadsheet")]
        updated_spreadsheet: Option<Spreadsheet>,
    }
    let response_content: ApiResponse = response.json().await.map_err(anyhow::Error::from)?;
    trace!("Received replies to updating sheet: {}", response_content.replies);

    let url = 'url: {
        if let Some(updated_spreadsheet) = response_content.updated_spreadsheet {
            if let Some(spreadsheet_url) = updated_spreadsheet.spreadsheet_url {
                break 'url spreadsheet_url;
            }
        }
        warn!("No URL returned in response to updating sheet. Inferring URL from spreadsheet ID and a hardcoded pattern");
        format!("https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit", spreadsheet_id = spreadsheet_id)
    };
    info!("Updated Google Sheet at {}", url);
    Ok(url)
}

/// A HashMap of known sheets, where the key is some string, and the value is
/// the spreadsheet ID.
type KnownSheets<'a> = HashMap<SheetNickname, Cow<'a, str>>;

fn update_known_sheets_file(nickname: SheetNickname, spreadsheet_id: &str) -> std::io::Result<()> {
    let path = Path::new(KNOWN_SHEETS_FILE);

    // deserialize the existing known sheets
    let mut known_sheets: KnownSheets = if let Ok(file) = File::open(path) {
        let reader = BufReader::new(file);
        match serde_json::from_reader(reader) {
            Ok(sheets) => sheets,
            Err(e) => {
                warn!("failed to deserialize known sheets file: {}", e);
                HashMap::new()
            }
        }
    } else {
        HashMap::new()
    };

    // insert the new key-value pair
    known_sheets.insert(nickname, spreadsheet_id.into());

    // Serialize the updated known sheets back to the file
    let writer = BufWriter::new(File::create(path)?);
    serde_json::to_writer(writer, &known_sheets)?;

    Ok(())
}

/// Reads the known sheets file and returns the value associated with the
/// specified nickname.
fn read_known_sheets_file(nickname: SheetNickname) -> std::io::Result<Option<String>> {
    let file = match File::open(KNOWN_SHEETS_FILE) {
        Ok(file) => file,
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("Failed to open known sheets file: {}", e);
            }
            return Ok(None);
        }
    };
    let reader = BufReader::new(file);
    let mut known_sheets: KnownSheets = serde_json::from_reader(reader)?;
    Ok(known_sheets.remove(&nickname).map(Cow::into_owned))
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum SheetNickname {
    AccReceivable,
    Kpi,
}
