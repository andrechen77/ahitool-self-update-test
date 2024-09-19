use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Spreadsheet {
    #[serde(rename = "spreadsheetId", skip_serializing_if = "Option::is_none")]
    pub spreadsheet_id: Option<String>,
    pub properties: SpreadsheetProperties,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sheets: Option<Vec<Sheet>>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct SpreadsheetProperties {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Sheet {
    pub properties: SheetProperties,
    pub data: GridData,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct SheetProperties {
    #[serde(rename = "sheetId", skip_serializing_if = "Option::is_none")]
    pub sheet_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct GridData {
    #[serde(rename = "startRow")]
    pub start_row: u64,
    #[serde(rename = "startColumn")]
    pub start_column: u64,
    #[serde(rename = "rowData")]
    pub row_data: Vec<RowData>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct RowData {
    pub values: Vec<CellData>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct CellData {
    #[serde(rename = "userEnteredValue")]
    pub user_entered_value: Option<ExtendedValue>,
}

#[derive(Serialize, Deserialize, Debug)]
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
