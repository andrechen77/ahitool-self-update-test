use tracing::{info, warn};

pub fn open_url(url: &str) {
    match open::that(url) {
        Ok(()) => info!("Opened URL: {}", url),
        Err(e) => {
            warn!("Failed to open URL {}: {}", url, e);
            println!("Browse to the following URL: {}", url);
        }
    }
}
