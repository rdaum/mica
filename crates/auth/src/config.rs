use crate::oauth::GithubOAuthConfig;
use crate::paseto::{PasetoKey, PasetoKeyring};
use crate::session::AuthSchema;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub keyring: PasetoKeyring,
    pub schema: AuthSchema,
    pub login_actor: String,
    pub local_login_return_path: String,
    pub session_ttl: Duration,
    pub cookie_name: String,
    pub local_password_auth_enabled: bool,
    pub github_client_id: Option<String>,
    pub github_client_secret: Option<String>,
    pub github_redirect_uri: Option<String>,
    pub github_org: Option<String>,
    pub github_allowed_logins: Vec<String>,
    pub github_admin_logins: Vec<String>,
    pub github_default_role: String,
}

impl AuthConfig {
    pub fn github_oauth(&self) -> Option<GithubOAuthConfig> {
        match (
            &self.github_client_id,
            &self.github_client_secret,
            &self.github_redirect_uri,
        ) {
            (Some(client_id), Some(client_secret), Some(redirect_uri)) => Some(GithubOAuthConfig {
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                redirect_uri: redirect_uri.clone(),
                org: self.github_org.clone(),
                allowed_logins: self.github_allowed_logins.clone(),
            }),
            _ => None,
        }
    }
}

fn parse_string_env(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn parse_schema_namespace() -> Result<String, AuthConfigError> {
    let namespace = parse_string_env("MICA_AUTH_SCHEMA_NAMESPACE", "auth");
    if namespace
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Ok(namespace);
    }
    Err(AuthConfigError::InvalidSchemaNamespace(namespace))
}

fn parse_return_path() -> Result<String, AuthConfigError> {
    let path = parse_string_env("MICA_AUTH_LOCAL_LOGIN_RETURN", "/");
    if path.is_empty() || !path.starts_with('/') || path.starts_with("//") {
        return Err(AuthConfigError::InvalidLocalLoginReturnPath(path));
    }
    for c in path.chars() {
        match c {
            '\x00'..='\x1f' | '\x7f' | ':' => {
                return Err(AuthConfigError::InvalidLocalLoginReturnPath(path));
            }
            _ => {}
        }
    }
    Ok(path)
}

fn parse_allowed_logins(value: Option<String>) -> Vec<String> {
    value
        .into_iter()
        .flat_map(|raw| {
            raw.split([',', ' ', '\n', '\t'])
                .map(str::trim)
                .filter(|login| !login.is_empty())
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn parse_login_lists(values: impl IntoIterator<Item = Option<String>>) -> Vec<String> {
    values.into_iter().flat_map(parse_allowed_logins).collect()
}

fn parse_bool_env(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

fn parse_role_env(name: &str, default: &str) -> Result<String, AuthConfigError> {
    let role = std::env::var(name).unwrap_or_else(|_| default.to_owned());
    let role = role.trim().to_ascii_lowercase();
    if matches!(role.as_str(), "admin" | "operator" | "viewer") {
        return Ok(role);
    }
    Err(AuthConfigError::InvalidRole {
        name: name.to_owned(),
        value: role,
    })
}

#[derive(Debug)]
pub enum AuthConfigError {
    MissingKey,
    InvalidKeyHex(String),
    InvalidPreviousKeyHex(String),
    InvalidTtl(String),
    InvalidRole { name: String, value: String },
    InvalidSchemaNamespace(String),
    InvalidLocalLoginReturnPath(String),
}

impl std::fmt::Display for AuthConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingKey => write!(f, "PASETO key not configured"),
            Self::InvalidKeyHex(msg) => write!(f, "invalid PASETO key hex: {msg}"),
            Self::InvalidPreviousKeyHex(msg) => {
                write!(f, "invalid previous PASETO key hex: {msg}")
            }
            Self::InvalidTtl(msg) => write!(f, "invalid session TTL: {msg}"),
            Self::InvalidRole { name, value } => {
                write!(
                    f,
                    "{name} must be one of admin, operator, or viewer; got {value:?}"
                )
            }
            Self::InvalidSchemaNamespace(value) => {
                write!(
                    f,
                    "MICA_AUTH_SCHEMA_NAMESPACE must contain only ASCII letters, digits, or underscores; got {value:?}"
                )
            }
            Self::InvalidLocalLoginReturnPath(value) => {
                write!(
                    f,
                    "MICA_AUTH_LOCAL_LOGIN_RETURN must be a local absolute path; got {value:?}"
                )
            }
        }
    }
}

impl std::error::Error for AuthConfigError {}

impl AuthConfig {
    pub fn from_env() -> Result<Self, AuthConfigError> {
        let key_hex =
            std::env::var("MICA_AUTH_PASETO_KEY").map_err(|_| AuthConfigError::MissingKey)?;
        let key = PasetoKey::from_hex("active".to_owned(), &key_hex)
            .map_err(|_| AuthConfigError::InvalidKeyHex(key_hex.clone()))?;

        let ttl_secs: u64 = std::env::var("MICA_AUTH_SESSION_TTL_SECS")
            .unwrap_or_else(|_| "43200".to_owned())
            .parse()
            .map_err(|e| AuthConfigError::InvalidTtl(format!("{e}")))?;

        let previous_key = match std::env::var("MICA_AUTH_PASETO_KEY_PREVIOUS") {
            Ok(hex) => Some(
                PasetoKey::from_hex("previous".to_owned(), &hex)
                    .map_err(|_| AuthConfigError::InvalidPreviousKeyHex(hex.clone()))?,
            ),
            Err(_) => None,
        };

        let keyring = if let Some(prev) = previous_key {
            PasetoKeyring::with_previous(key, prev)
        } else {
            PasetoKeyring::new(key)
        };

        Ok(Self {
            keyring,
            schema: AuthSchema::namespaced(&parse_schema_namespace()?),
            login_actor: parse_string_env("MICA_AUTH_LOGIN_ACTOR", "auth/guest"),
            local_login_return_path: parse_return_path()?,
            session_ttl: Duration::from_secs(ttl_secs),
            cookie_name: crate::cookie::SESSION_COOKIE_NAME.to_owned(),
            local_password_auth_enabled: parse_bool_env("MICA_AUTH_LOCAL_PASSWORD"),
            github_client_id: std::env::var("MICA_AUTH_GITHUB_CLIENT_ID").ok(),
            github_client_secret: std::env::var("MICA_AUTH_GITHUB_CLIENT_SECRET").ok(),
            github_redirect_uri: std::env::var("MICA_AUTH_GITHUB_REDIRECT_URI").ok(),
            github_org: std::env::var("MICA_AUTH_GITHUB_ORG").ok(),
            github_allowed_logins: parse_allowed_logins(
                std::env::var("MICA_AUTH_GITHUB_ALLOWED_LOGINS").ok(),
            ),
            github_admin_logins: parse_login_lists([
                std::env::var("MICA_AUTH_ADMIN_GITHUB_LOGIN").ok(),
                std::env::var("MICA_AUTH_GITHUB_ADMIN_LOGINS").ok(),
            ]),
            github_default_role: parse_role_env("MICA_AUTH_GITHUB_DEFAULT_ROLE", "operator")?,
        })
    }

    pub fn dev_mode() -> Self {
        let key = PasetoKey::new("dev-key".to_owned(), *b"devkeydevkeydevkeydevkeydevkey!!");
        Self {
            keyring: PasetoKeyring::new(key),
            schema: AuthSchema::namespaced("auth"),
            login_actor: "auth/guest".to_owned(),
            local_login_return_path: "/".to_owned(),
            session_ttl: Duration::from_secs(86400),
            cookie_name: crate::cookie::SESSION_COOKIE_NAME.to_owned(),
            local_password_auth_enabled: false,
            github_client_id: None,
            github_client_secret: None,
            github_redirect_uri: None,
            github_org: None,
            github_allowed_logins: Vec::new(),
            github_admin_logins: Vec::new(),
            github_default_role: "viewer".to_owned(),
        }
    }

    pub fn with_key(key: PasetoKey, session_ttl: Duration) -> Self {
        Self {
            keyring: PasetoKeyring::new(key),
            schema: AuthSchema::namespaced("auth"),
            login_actor: "auth/guest".to_owned(),
            local_login_return_path: "/".to_owned(),
            session_ttl,
            cookie_name: crate::cookie::SESSION_COOKIE_NAME.to_owned(),
            local_password_auth_enabled: false,
            github_client_id: None,
            github_client_secret: None,
            github_redirect_uri: None,
            github_org: None,
            github_allowed_logins: Vec::new(),
            github_admin_logins: Vec::new(),
            github_default_role: "viewer".to_owned(),
        }
    }
}
