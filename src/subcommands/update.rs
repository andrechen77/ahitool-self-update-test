use tracing::info;

#[derive(clap::Args, Debug)]
pub struct Args {}

pub fn main(_args: Args) -> anyhow::Result<()> {
    update_executable(GITHUB_REPO)?;
    Ok(())
}

pub const GITHUB_REPO: &str = "andrechen77/ahitool";

const USER_AGENT: &str = "andrechen77/ahitool";

/// The name of the asset to download.
#[cfg(target_os = "windows")]
const ASSET_NAME: Option<&str> = Some("ahitool-win.exe");

/// The name of the asset to download.
#[cfg(target_os = "linux")]
const ASSET_NAME: Option<&str> = Some("ahitool-linux");

/// The name of the asset to download.
#[cfg(not(any(target_os = "windows", target_os = "linux")))]
const ASSET_NAME: Option<&str> = None;

fn update_executable(github_repo: &str) -> anyhow::Result<()> {
    let Some(asset_name) = ASSET_NAME else {
        anyhow::bail!(
            "unsupported platform; I don't know how to download assets for this platform"
        );
    };

    let api_url = format!("https://api.github.com/repos/{}/releases/latest", github_repo);

    let client = reqwest::blocking::Client::builder().user_agent(USER_AGENT).build()?;

    let response: serde_json::Value = client.get(&api_url).send()?.json()?;

    let version_tag =
        response["tag_name"].as_str().ok_or(anyhow::anyhow!("No tag_name found in release"))?;
    let asset_url = response["assets"]
        .as_array()
        .ok_or(anyhow::anyhow!("No assets found in release"))?
        .iter()
        .find_map(|asset| {
            let name = asset["name"].as_str()?;
            if name == asset_name {
                asset["browser_download_url"].as_str()
            } else {
                None
            }
        })
        .ok_or(anyhow::anyhow!("No suitable asset found for this platform"))?;

    // download the asset to a temporary file
    let mut response = client.get(asset_url).send()?;
    let mut temp_file = tempfile::Builder::new().suffix(".tmp").tempfile()?;
    response.copy_to(&mut temp_file)?;

    // Replace the current executable with the new version
    self_replace::self_replace(temp_file.path())?;

    info!("Updated executable to version {}", version_tag);
    Ok(())
}
