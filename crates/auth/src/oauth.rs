use rand::RngCore;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug)]
pub struct GithubOAuthConfig {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
    pub org: Option<String>,
    pub allowed_logins: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GithubUserInfo {
    pub login: String,
    pub id: u64,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
}

pub fn build_authorization_url(config: &GithubOAuthConfig, state: &str) -> String {
    build_authorization_url_with_pkce(config, state, None)
}

pub fn build_authorization_url_with_pkce(
    config: &GithubOAuthConfig,
    state: &str,
    code_challenge: Option<&str>,
) -> String {
    let mut url = format!(
        "https://github.com/login/oauth/authorize?client_id={}&redirect_uri={}&state={}&scope={}",
        url_encode(&config.client_id),
        url_encode(&config.redirect_uri),
        url_encode(state),
        url_encode("read:user read:org"),
    );
    if let Some(challenge) = code_challenge {
        url.push_str(&format!(
            "&code_challenge={}&code_challenge_method=S256",
            url_encode(challenge)
        ));
    }
    if let Some(org) = &config.org {
        url.push_str("&allow_signup=false");
        let _ = org;
    }
    url
}

pub fn exchange_code_for_token(
    config: &GithubOAuthConfig,
    code: &str,
) -> Result<String, OAuthError> {
    exchange_code_for_token_with_pkce(config, code, None)
}

pub fn exchange_code_for_token_with_pkce(
    config: &GithubOAuthConfig,
    code: &str,
    code_verifier: Option<&str>,
) -> Result<String, OAuthError> {
    let mut params = vec![
        ("client_id", config.client_id.as_str()),
        ("client_secret", config.client_secret.as_str()),
        ("code", code),
        ("redirect_uri", config.redirect_uri.as_str()),
    ];
    let verifier_str;
    if let Some(verifier) = code_verifier {
        verifier_str = verifier.to_owned();
        params.push(("code_verifier", &verifier_str));
    }

    let response = ureq::post("https://github.com/login/oauth/access_token")
        .set("Accept", "application/json")
        .set("User-Agent", "mica-auth/0.1")
        .send_form(&params)
        .map_err(|e| OAuthError::ProviderError(format!("token request failed: {e}")))?;

    let body: serde_json::Value =
        serde_json::from_str(&response.into_string().map_err(|e| {
            OAuthError::ProviderError(format!("failed to read token response: {e}"))
        })?)
        .map_err(|e| OAuthError::ProviderError(format!("failed to parse token response: {e}")))?;

    if let Some(error) = body.get("error") {
        let desc = body
            .get("error_description")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        return Err(OAuthError::ProviderError(format!("{error}: {desc}")));
    }

    body["access_token"]
        .as_str()
        .map(|s| s.to_owned())
        .ok_or_else(|| OAuthError::ProviderError("no access_token in response".to_owned()))
}

pub fn get_user_info(access_token: &str) -> Result<GithubUserInfo, OAuthError> {
    let response = ureq::get("https://api.github.com/user")
        .set("Authorization", &format!("Bearer {access_token}"))
        .set("Accept", "application/vnd.github+json")
        .set("User-Agent", "mica-auth/0.1")
        .set("X-GitHub-Api-Version", "2022-11-28")
        .call()
        .map_err(|e| OAuthError::ProviderError(format!("user info request failed: {e}")))?;

    serde_json::from_str(
        &response
            .into_string()
            .map_err(|e| OAuthError::ProviderError(format!("failed to read user info: {e}")))?,
    )
    .map_err(|e| OAuthError::ProviderError(format!("failed to parse user info: {e}")))
}

pub fn check_org_membership(
    access_token: &str,
    org: &str,
    username: &str,
) -> Result<bool, OAuthError> {
    let url = format!("https://api.github.com/orgs/{org}/memberships/{username}");

    let response = match ureq::get(&url)
        .set("Authorization", &format!("Bearer {access_token}"))
        .set("Accept", "application/vnd.github+json")
        .set("User-Agent", "mica-auth/0.1")
        .set("X-GitHub-Api-Version", "2022-11-28")
        .call()
    {
        Ok(response) => response,
        Err(ureq::Error::Status(404, _)) => return Ok(false),
        Err(ureq::Error::Status(403, _)) => {
            return Err(OAuthError::ProviderError(
                "token lacks permission to check organization membership".to_owned(),
            ));
        }
        Err(e) => {
            return Err(OAuthError::ProviderError(format!(
                "org membership check failed: {e}"
            )));
        }
    };

    let status = response.status();
    if status == 200 {
        let body: serde_json::Value =
            serde_json::from_str(&response.into_string().map_err(|_| {
                OAuthError::ProviderError("failed to read membership response".to_owned())
            })?)
            .map_err(|_| {
                OAuthError::ProviderError("failed to parse membership response".to_owned())
            })?;

        let state = body["state"].as_str().unwrap_or("");
        Ok(state == "active" || state == "pending")
    } else if status == 404 {
        Ok(false)
    } else {
        Err(OAuthError::ProviderError(format!(
            "unexpected status from org membership check: {status}"
        )))
    }
}

pub fn generate_oauth_state() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    hex_encode(&bytes)
}

pub fn generate_pkce_verifier() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    base64url_encode_no_pad(&bytes)
}

pub fn compute_pkce_challenge(verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let hash = hasher.finalize();
    base64url_encode_no_pad(&hash)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn base64url_encode_no_pad(bytes: &[u8]) -> String {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(alphabet[((n >> 18) & 0x3f) as usize] as char);
        out.push(alphabet[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(alphabet[((n >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(alphabet[(n & 0x3f) as usize] as char);
        }
    }
    out
}

fn url_encode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                result.push_str(&format!("%{byte:02X}"));
            }
        }
    }
    result
}

#[derive(Debug)]
pub enum OAuthError {
    ProviderError(String),
    InvalidState,
    OrgRequired,
    OrgAccessDenied,
}

impl std::fmt::Display for OAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProviderError(msg) => write!(f, "OAuth provider error: {msg}"),
            Self::InvalidState => write!(f, "invalid OAuth state parameter"),
            Self::OrgRequired => write!(
                f,
                "organization membership is required but no org configured"
            ),
            Self::OrgAccessDenied => write!(f, "user is not a member of the required organization"),
        }
    }
}

impl std::error::Error for OAuthError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_encode_basic() {
        assert_eq!(url_encode("hello"), "hello");
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("a+b=c"), "a%2Bb%3Dc");
        assert_eq!(
            url_encode("http://example.com/path?q=1"),
            "http%3A%2F%2Fexample.com%2Fpath%3Fq%3D1"
        );
    }

    #[test]
    fn generate_state_is_unique() {
        let s1 = generate_oauth_state();
        let s2 = generate_oauth_state();
        assert_ne!(s1, s2);
        assert_eq!(s1.len(), 64);
    }

    #[test]
    fn build_authorization_url_includes_params() {
        let config = GithubOAuthConfig {
            client_id: "test-client".to_owned(),
            client_secret: "test-secret".to_owned(),
            redirect_uri: "http://localhost:8080/auth/callback".to_owned(),
            org: None,
            allowed_logins: Vec::new(),
        };
        let url = build_authorization_url(&config, "test-state");
        assert!(url.contains("client_id=test-client"));
        assert!(url.contains("state=test-state"));
        assert!(url.contains("scope=read%3Auser%20read%3Aorg"));
    }
}
