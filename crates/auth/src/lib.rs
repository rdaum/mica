pub mod config;
pub mod cookie;
pub mod oauth;
pub mod paseto;
pub mod password;
pub mod session;

pub use config::{AuthConfig, AuthConfigError};
pub use cookie::{
    SESSION_COOKIE_NAME, build_clear_session_cookie, build_session_cookie, extract_session_cookie,
};
pub use oauth::{
    GithubOAuthConfig, GithubUserInfo, OAuthError, build_authorization_url,
    build_authorization_url_with_pkce, check_org_membership, compute_pkce_challenge,
    exchange_code_for_token, exchange_code_for_token_with_pkce, generate_oauth_state,
    generate_pkce_verifier, get_user_info,
};
pub use paseto::{
    PasetoError, PasetoKey, PasetoKeyring, SessionClaims, decode_session_token,
    encode_session_token, future_rfc3339, now_rfc3339, random_session_id,
};
pub use password::{PasswordError, hash_password, verify_password};
pub use session::{
    AuthRoleSymbols, AuthSchema, LocalAuthenticatedUser, MicaSessionStore, SessionRecord,
};
