use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Mutex;

use anyhow::bail;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{body::Incoming as IncomingBody, server::conn::http1, Request, Response};
use hyper_util::rt::TokioIo;
use oauth2::basic::BasicTokenType;
use oauth2::reqwest::async_http_client;
use oauth2::{
    basic::BasicClient, AuthUrl, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge, Scope,
    TokenUrl,
};
use oauth2::{AuthorizationCode, EmptyExtraTokenFields, RedirectUrl, StandardTokenResponse};
use tracing::info;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use tokio::{net::TcpListener, sync::oneshot};

pub type Token = StandardTokenResponse<EmptyExtraTokenFields, BasicTokenType>;

pub const DEFAULT_CACHE_FILE: &str = "google_oauth_token.json";
const CLIENT_ID: &str = "859579651850-t212eiscr880fnifmsi6ddft2bhdtplt.apps.googleusercontent.com";
// It should be fine that the secret is not actually kept secret. see
// https://developers.google.com/identity/protocols/oauth2
const CLIENT_SECRET: &str = "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";
const AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const SCOPE_DRIVE_FILE: &str = "https://www.googleapis.com/auth/drive.file";

pub fn get_credentials_with_cache(cache_file: &Path) -> anyhow::Result<Token> {
    if cache_file.exists() {
        let reader = BufReader::new(File::open(cache_file)?);
        let cached_token: Token = serde_json::from_reader(reader)?;
        // TODO check if token is valid
        Ok(cached_token)
    } else {
        let fresh_token = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(get_fresh_credentials())?;
        let writer = BufWriter::new(File::create(cache_file)?);
        serde_json::to_writer(writer, &fresh_token)?;
        Ok(fresh_token)
    }
}

async fn get_fresh_credentials() -> anyhow::Result<Token> {
    // establish a server to listen for the authorization code
    let addr: SocketAddr = ([127, 0, 0, 1], 0).into(); // request any port
    let tcp_listener = TcpListener::bind(addr).await?;

    // create OAuth2 client
    let client = BasicClient::new(
        ClientId::new(CLIENT_ID.to_owned()),
        Some(ClientSecret::new(CLIENT_SECRET.to_owned())),
        AuthUrl::new(AUTH_URL.to_owned())?,
        Some(TokenUrl::new(TOKEN_URL.to_owned())?),
    )
    .set_redirect_uri(RedirectUrl::new(format!(
        "http://localhost:{}",
        tcp_listener.local_addr().expect("should exist").port()
    ))?);
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let (auth_url, csrf_token) = client
        .authorize_url(CsrfToken::new_random)
        .add_scope(Scope::new(SCOPE_DRIVE_FILE.to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();

    let (tx, rx) = oneshot::channel();
    tokio::spawn(listen_for_code(tcp_listener, tx));

    info!("Browse to the following URL to authorize the app: {}", auth_url);
    let OAuthReply { code, state } = rx.await?;

    if *csrf_token.secret() != state {
        bail!("CSRF token does not match!, {} != {}", csrf_token.secret(), code);
    }

    let token_result = client
        .exchange_code(AuthorizationCode::new(code))
        .set_pkce_verifier(pkce_verifier)
        .request_async(async_http_client)
        .await?;

    Ok(token_result)
}

#[derive(Debug)]
struct OAuthReply {
    code: String,
    state: String,
}

async fn listen_for_code(
    tcp_listener: TcpListener,
    response_tx: oneshot::Sender<OAuthReply>,
) -> anyhow::Result<()> {
    let (tcp_stream, _) = tcp_listener.accept().await?;
    let tcp_stream = TokioIo::new(tcp_stream);

    let response_tx = Mutex::new(Some(response_tx));
    let handle_request = |req: Request<IncomingBody>| {
        let response_tx = &response_tx; // only borrow, not move, `response_tx`
        async move {
            let http_resp = if let Some(response_tx) = response_tx.lock().unwrap().take() {
                let mut code = None;
                let mut state = None;
                for (k, v) in
                    url::form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes())
                {
                    match k.as_ref() {
                        "code" => code = Some(v),
                        "state" => state = Some(v),
                        _ => (),
                    }
                }
                response_tx
                    .send(OAuthReply {
                        // FIXME: better error handling
                        code: code.expect("reply should have a code").into_owned(),
                        state: state.expect("reply should have a state").into_owned(),
                    })
                    .expect("the corresponding receiver has no way of being deallocated");
                "Authorization code received. You can now close this window."
            } else {
                "The app may have already been authorized; if not then try again."
            };
            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(http_resp))))
        }
    };

    http1::Builder::new().serve_connection(tcp_stream, service_fn(handle_request)).await?;

    Ok(())
}
