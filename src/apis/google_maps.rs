use hyper::{header::CONTENT_TYPE, StatusCode};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tracing::trace;
use anyhow::anyhow;

const ENDPOINT_GOOGLE_MAPS_PLACES: &str = "https://places.googleapis.com/v1/places:searchText";

#[derive(Error, Debug)]
pub enum LookupError {
	#[error("This request came too soon after a previous request, and we have been rate-limited")]
	TooFast,
    #[error("The address was not found")]
    NotFound,
	#[error(transparent)]
	Other(#[from] anyhow::Error),
}

pub async fn lookup(client: reqwest::Client, api_key: &str, address: &str) -> Result<LatLng, LookupError> {
	let url = reqwest::Url::parse(ENDPOINT_GOOGLE_MAPS_PLACES).expect("hardcoded URL should be valid");
	trace!("Sending request to look up address: {}", address);
	let response = client
		.post(url)
		.query(&[("key", api_key), ("fields", "places.id,places.location,places.displayName")])
        .json(&json!({
            "textQuery": address
        }))
        .header(CONTENT_TYPE, "application/json")
		.send()
		.await
		.map_err(anyhow::Error::from)?;

	match response.status() {
		StatusCode::TOO_MANY_REQUESTS => return Err(LookupError::TooFast),
		StatusCode::OK => (),
		status => return Err(LookupError::Other(anyhow!("Request failed with status code: {}", status)))
	}

    #[derive(Deserialize)]
    struct ApiResponse {
        places: Vec<Place>,
    }

    let response: serde_json::Value = response.json().await.map_err(anyhow::Error::from)?;
    trace!("received response: {}", response);
	let response: ApiResponse = serde_json::from_value(response).map_err(anyhow::Error::from)?;

    if let Some(place) = response.places.into_iter().next() {
        let Place { location, .. } = place;
        Ok(location)
    } else {
        Err(LookupError::NotFound)
    }
}

#[derive(Deserialize)]
struct Place {
    #[allow(dead_code)]
    pub id: String,
    pub location: LatLng,
}

#[derive(Deserialize)]
pub struct LatLng {
    pub latitude: f64,
    pub longitude: f64,
}
