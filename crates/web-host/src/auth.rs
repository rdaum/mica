use crate::codec::HttpResponse;
use mica_auth::{
    AuthConfig, MicaSessionStore, SessionRecord, build_authorization_url_with_pkce,
    build_clear_session_cookie, build_session_cookie, check_org_membership, compute_pkce_challenge,
    decode_session_token, encode_session_token, exchange_code_for_token_with_pkce,
    extract_session_cookie, generate_oauth_state, generate_pkce_verifier, get_user_info,
    now_rfc3339, random_session_id,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub session_id: String,
    pub user_id: String,
    pub actor_name: String,
    pub provider: String,
    pub roles_version: Option<u64>,
}

#[derive(Debug)]
pub enum AuthError {
    NoCookie,
    InvalidToken(String),
    SessionNotFound,
    SessionRevoked,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCookie => write!(f, "no session cookie"),
            Self::InvalidToken(msg) => write!(f, "invalid token: {msg}"),
            Self::SessionNotFound => write!(f, "session not found"),
            Self::SessionRevoked => write!(f, "session revoked"),
        }
    }
}

impl std::error::Error for AuthError {}

struct OAuthStateEntry {
    expires_at: Instant,
    code_verifier: Option<String>,
    return_path: String,
}

pub struct AuthSubsystem {
    pub config: AuthConfig,
    pub session_store: MicaSessionStore,
    oauth_state_store: Mutex<HashMap<String, OAuthStateEntry>>,
}

impl AuthSubsystem {
    pub fn new(config: AuthConfig, session_store: MicaSessionStore) -> Self {
        Self {
            config,
            session_store,
            oauth_state_store: Mutex::new(HashMap::new()),
        }
    }

    fn store_oauth_state(&self, state: String, code_verifier: Option<String>, return_path: String) {
        let expires_at = Instant::now() + Duration::from_secs(600);
        self.oauth_state_store.lock().unwrap().insert(
            state,
            OAuthStateEntry {
                expires_at,
                code_verifier,
                return_path,
            },
        );
    }

    fn take_oauth_state(&self, state: &str) -> Option<OAuthStateEntry> {
        let mut store = self.oauth_state_store.lock().unwrap();
        store.retain(|_, entry| entry.expires_at > Instant::now());
        store.remove(state)
    }

    pub async fn resolve_auth_context(
        &self,
        cookie_header: Option<&str>,
    ) -> Result<Option<AuthContext>, AuthError> {
        let Some(cookie_header) = cookie_header else {
            return Ok(None);
        };

        let Some(token) = extract_session_cookie(cookie_header) else {
            return Ok(None);
        };

        let claims = decode_session_token(&self.config.keyring, &token)
            .map_err(|e| AuthError::InvalidToken(e.to_string()))?;

        let session = self
            .session_store
            .lookup_session(&claims.sid)
            .await
            .map_err(|_| AuthError::SessionNotFound)?
            .ok_or(AuthError::SessionNotFound)?;

        if session.revoked_at.is_some() {
            return Err(AuthError::SessionRevoked);
        }

        Ok(Some(AuthContext {
            session_id: claims.sid,
            user_id: session.user_id,
            actor_name: session.actor,
            provider: session.provider,
            roles_version: session.roles_version.or(claims.roles_version),
        }))
    }

    pub async fn handle_auth_start_github(
        &self,
        request: &crate::codec::HttpRequest,
    ) -> Option<HttpResponse> {
        if request.method != "GET" || strip_query(&request.path) != "/auth/start/github" {
            return None;
        }

        let Some(oauth_config) = self.config.github_oauth() else {
            return Some(HttpResponse::new(
                500,
                "Internal Server Error",
                b"GitHub OAuth not configured".to_vec(),
            ));
        };

        let query_params = crate::response::query_params(&request.path);
        let return_path = validate_return_path(
            query_params
                .get("return")
                .cloned()
                .unwrap_or_else(|| "/".to_owned()),
        )
        .unwrap_or_else(|| "/".to_owned());

        let state = generate_oauth_state();
        let code_verifier = generate_pkce_verifier();
        let code_challenge = compute_pkce_challenge(&code_verifier);
        self.store_oauth_state(state.clone(), Some(code_verifier), return_path);
        let url = build_authorization_url_with_pkce(&oauth_config, &state, Some(&code_challenge));

        Some(HttpResponse::new(302, "Found", Vec::new()).with_header("Location", url.into_bytes()))
    }

    pub async fn handle_auth_callback(
        &self,
        request: &crate::codec::HttpRequest,
    ) -> Option<HttpResponse> {
        if request.method != "GET" || strip_query(&request.path) != "/auth/callback" {
            return None;
        }

        let Some(oauth_config) = self.config.github_oauth() else {
            return Some(HttpResponse::new(
                500,
                "Internal Server Error",
                b"GitHub OAuth not configured".to_vec(),
            ));
        };

        let query = crate::response::query_params(&request.path);

        if let Some(error) = query.get("error") {
            let desc = query
                .get("error_description")
                .cloned()
                .unwrap_or_else(|| "no description".to_owned());
            tracing::warn!(error = %error, description = %desc, "OAuth callback error");
            let body = format!("OAuth error: {error} - {desc}");
            return Some(HttpResponse::new(400, "Bad Request", body.into_bytes()));
        }

        let Some(state) = query.get("state").cloned() else {
            return Some(HttpResponse::new(
                400,
                "Bad Request",
                b"Missing state parameter".to_vec(),
            ));
        };

        let Some(state_entry) = self.take_oauth_state(&state) else {
            return Some(HttpResponse::new(
                400,
                "Bad Request",
                b"Invalid or expired state parameter".to_vec(),
            ));
        };

        let return_path = state_entry.return_path;

        let Some(code) = query.get("code").cloned() else {
            return Some(HttpResponse::new(
                400,
                "Bad Request",
                b"Missing code parameter".to_vec(),
            ));
        };

        let access_token = match exchange_code_for_token_with_pkce(
            &oauth_config,
            &code,
            state_entry.code_verifier.as_deref(),
        ) {
            Ok(token) => token,
            Err(e) => {
                tracing::warn!(error = %e, "OAuth token exchange failed");
                return Some(HttpResponse::new(
                    500,
                    "Internal Server Error",
                    b"Failed to exchange authorization code".to_vec(),
                ));
            }
        };

        let user_info = match get_user_info(&access_token) {
            Ok(info) => info,
            Err(e) => {
                tracing::warn!(error = %e, "failed to get GitHub user info");
                return Some(HttpResponse::new(
                    500,
                    "Internal Server Error",
                    b"Failed to retrieve user information".to_vec(),
                ));
            }
        };

        if let Some(org) = &oauth_config.org {
            match check_org_membership(&access_token, org, &user_info.login) {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(login = %user_info.login, org = %org, "user is not a member of required org");
                    return Some(HttpResponse::new(
                        403,
                        "Forbidden",
                        b"You are not a member of the required organization".to_vec(),
                    ));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "org membership check failed");
                    return Some(HttpResponse::new(
                        500,
                        "Internal Server Error",
                        b"Failed to verify organization membership".to_vec(),
                    ));
                }
            }
        }
        if !oauth_config.allowed_logins.is_empty()
            && !oauth_config
                .allowed_logins
                .iter()
                .any(|login| login.eq_ignore_ascii_case(&user_info.login))
        {
            tracing::warn!(login = %user_info.login, "user is not in the GitHub login allowlist");
            return Some(HttpResponse::new(
                403,
                "Forbidden",
                b"You are not allowed to access this Conatus instance".to_vec(),
            ));
        }

        let session_id = random_session_id();
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let user_id = match self
            .session_store
            .ensure_user_exists(&user_info.login, "github", &user_info.id.to_string())
            .await
        {
            Ok(user_id) => user_id,
            Err(e) => {
                tracing::error!(error = %e, "failed to ensure user exists");
                return Some(HttpResponse::new(
                    500,
                    "Internal Server Error",
                    b"Failed to create user identity".to_vec(),
                ));
            }
        };

        let record = SessionRecord {
            session_id: session_id.clone(),
            user_id: user_id.clone(),
            actor: user_id.clone(),
            provider: "github".to_owned(),
            provider_sub: user_info.id.to_string(),
            issued_at: now_ts,
            expires_at: now_ts + self.config.session_ttl.as_secs() as i64,
            revoked_at: None,
            last_seen_at: now_ts,
            roles_version: None,
            user_agent_hash: None,
        };

        if let Err(e) = self.session_store.create_session(&record).await {
            tracing::error!(error = %e, "failed to create session");
            return Some(HttpResponse::new(
                500,
                "Internal Server Error",
                b"Failed to create session".to_vec(),
            ));
        }

        let claims = mica_auth::SessionClaims {
            sid: session_id,
            sub: user_id.clone(),
            actor: user_id,
            provider: "github".to_owned(),
            provider_sub: user_info.id.to_string(),
            iat: now_rfc3339(),
            nbf: now_rfc3339(),
            exp: mica_auth::future_rfc3339(self.config.session_ttl),
            roles_version: None,
        };

        let token = match encode_session_token(&self.config.keyring, &claims) {
            Ok(token) => token,
            Err(e) => {
                tracing::error!(error = %e, "failed to encode session token");
                return Some(HttpResponse::new(
                    500,
                    "Internal Server Error",
                    b"Failed to create session token".to_vec(),
                ));
            }
        };

        let cookie = build_session_cookie(&token);

        Some(
            HttpResponse::new(302, "Found", Vec::new())
                .with_header("Location", return_path.into_bytes())
                .with_header("Set-Cookie", cookie.into_bytes()),
        )
    }

    pub async fn handle_auth_logout(
        &self,
        request: &crate::codec::HttpRequest,
    ) -> Option<HttpResponse> {
        if (request.method != "GET" && request.method != "POST")
            || strip_query(&request.path) != "/auth/logout"
        {
            return None;
        }

        let cookie_header = request
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case("cookie"))
            .map(|h| std::str::from_utf8(&h.value).unwrap_or(""));

        if let Some(token) = cookie_header.and_then(|h| extract_session_cookie(h)) {
            if let Ok(claims) = decode_session_token(&self.config.keyring, &token) {
                if let Err(e) = self.session_store.revoke_session(&claims.sid).await {
                    tracing::warn!(session_id = %claims.sid, error = %e, "failed to revoke session");
                }
            }
        }

        let clear_cookie = build_clear_session_cookie();

        Some(
            HttpResponse::new(302, "Found", Vec::new())
                .with_header("Location", "/".to_owned().into_bytes())
                .with_header("Set-Cookie", clear_cookie.into_bytes()),
        )
    }
}

pub fn is_unauthenticated_path(path: &str) -> bool {
    let path = strip_query(path);
    path == "/healthz"
        || path.starts_with("/sync-client.js")
        || path == "/auth/start/github"
        || path == "/auth/callback"
        || path == "/auth/logout"
}

pub fn is_pre_auth_login_path(path: &str) -> bool {
    let path = strip_query(path);
    path == "/auth/login"
}

pub fn login_redirect_response(return_path: &str) -> HttpResponse {
    let return_path =
        validate_return_path(return_path.to_owned()).unwrap_or_else(|| "/".to_owned());
    let location = format!("/auth/login?return={}", url_encode_component(&return_path));
    HttpResponse::new(302, "Found", Vec::new()).with_header("Location", location.into_bytes())
}

pub fn clear_session_login_redirect_response(return_path: &str) -> HttpResponse {
    let clear_cookie = build_clear_session_cookie();
    login_redirect_response(return_path).with_header("Set-Cookie", clear_cookie.into_bytes())
}

fn strip_query(path: &str) -> &str {
    path.split_once('?').map(|(p, _)| p).unwrap_or(path)
}

fn validate_return_path(path: String) -> Option<String> {
    if path.is_empty() || !path.starts_with('/') {
        return None;
    }
    if path.starts_with("//") {
        return None;
    }
    for c in path.chars() {
        match c {
            '\x00'..='\x1f' | '\x7f' => return None,
            ':' => return None,
            _ => {}
        }
    }
    Some(path)
}

fn url_encode_component(input: &str) -> String {
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
