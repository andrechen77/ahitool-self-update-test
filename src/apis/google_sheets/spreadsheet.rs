use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Spreadsheet {
    #[serde(rename = "spreadsheetId", skip_serializing_if = "Option::is_none")]
    pub spreadsheet_id: Option<String>,
    pub properties: SpreadsheetProperties,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sheets: Option<Vec<Sheet>>,
    #[serde(rename = "spreadsheetUrl", skip_serializing_if = "Option::is_none")]
    pub spreadsheet_url: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct SpreadsheetProperties {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Sheet {
    pub properties: SheetProperties,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<GridData>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct SheetProperties {
    /// This value is ignored if used to create or update a sheet.
    #[serde(rename = "sheetId", skip_serializing_if = "Option::is_none")]
    pub sheet_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GridData {
    #[serde(rename = "startRow")]
    pub start_row: u64,
    #[serde(rename = "startColumn")]
    pub start_column: u64,
    #[serde(rename = "rowData")]
    pub row_data: Vec<RowData>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct RowData {
    pub values: Vec<CellData>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CellData {
    #[serde(rename = "userEnteredValue")]
    pub user_entered_value: Option<ExtendedValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExtendedValue {
    #[serde(rename = "stringValue")]
    StringValue(String),
    #[serde(rename = "numberValue")]
    NumberValue(f64),
    #[serde(rename = "boolValue")]
    BoolValue(bool),
    #[serde(rename = "formulaValue")]
    FormulaValue(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GridCoordinate {
    #[serde(rename = "sheetId")]
    pub sheet_id: u64,
    #[serde(rename = "rowIndex")]
    pub row_index: u64,
    #[serde(rename = "columnIndex")]
    pub column_index: u64,
}

pub mod update {
    use serde::Serialize;

    #[derive(Serialize, Debug, Clone)]
    pub enum Request {
        #[serde(rename = "updateSpreadsheetProperties")]
        UpdateSpreadsheetProperties {
            properties: super::SpreadsheetProperties,
            fields: &'static str,
        },
        #[serde(rename = "addSheet")]
        AddSheet { properties: super::SheetProperties },
        #[serde(rename = "updateCells")]
        UpdateCells {
            rows: Vec<super::RowData>,
            fields: &'static str,
            start: super::GridCoordinate,
        },
        #[serde(rename = "deleteSheet")]
        DeleteSheet { sheet_id: u64 },
    }
}
