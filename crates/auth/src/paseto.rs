use rand::RngCore;
use rusty_paseto::prelude::*;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug)]
pub struct PasetoKey {
    pub id: String,
    key: [u8; 32],
}

impl PasetoKey {
    pub fn new(id: String, key: [u8; 32]) -> Self {
        Self { id, key }
    }

    pub fn generate(id: String) -> Self {
        let mut key = [0u8; 32];
        rand::rng().fill_bytes(&mut key);
        Self { id, key }
    }

    pub fn from_hex(id: String, hex: &str) -> Result<Self, PasetoError> {
        let bytes = hex_decode(hex).map_err(|_| PasetoError::InvalidKey)?;
        if bytes.len() != 32 {
            return Err(PasetoError::InvalidKey);
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(Self { id, key })
    }

    fn symmetric_key(&self) -> PasetoSymmetricKey<V4, Local> {
        PasetoSymmetricKey::<V4, Local>::from(Key::from(&self.key))
    }
}

#[derive(Clone, Debug)]
pub struct PasetoKeyring {
    pub active: PasetoKey,
    pub previous: Option<PasetoKey>,
}

impl PasetoKeyring {
    pub fn new(active: PasetoKey) -> Self {
        Self {
            active,
            previous: None,
        }
    }

    pub fn with_previous(active: PasetoKey, previous: PasetoKey) -> Self {
        Self {
            active,
            previous: Some(previous),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionClaims {
    pub sid: String,
    pub sub: String,
    pub actor: String,
    pub provider: String,
    pub provider_sub: String,
    pub iat: String,
    pub nbf: String,
    pub exp: String,
    #[serde(default)]
    pub roles_version: Option<u64>,
}

#[derive(Debug)]
pub enum PasetoError {
    InvalidKey,
    Expired,
    NotYetValid,
    Tampered,
    InvalidPayload,
    BuildError(String),
    ParseError(String),
}

impl std::fmt::Display for PasetoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidKey => write!(f, "invalid PASETO key"),
            Self::Expired => write!(f, "token has expired"),
            Self::NotYetValid => write!(f, "token is not yet valid"),
            Self::Tampered => write!(f, "token has been tampered with"),
            Self::InvalidPayload => write!(f, "token payload is invalid"),
            Self::BuildError(msg) => write!(f, "failed to build token: {msg}"),
            Self::ParseError(msg) => write!(f, "failed to parse token: {msg}"),
        }
    }
}

impl std::error::Error for PasetoError {}

#[allow(deprecated)]
impl From<rusty_paseto::generic::PasetoClaimError> for PasetoError {
    fn from(e: rusty_paseto::generic::PasetoClaimError) -> Self {
        Self::BuildError(format!("{e}"))
    }
}

impl From<rusty_paseto::Error> for PasetoError {
    fn from(e: rusty_paseto::Error) -> Self {
        Self::BuildError(format!("{e}"))
    }
}

pub fn encode_session_token(
    keyring: &PasetoKeyring,
    claims: &SessionClaims,
) -> Result<String, PasetoError> {
    let key = keyring.active.symmetric_key();
    let footer = Footer::from(keyring.active.id.as_str());

    let mut builder = PasetoBuilder::<V4, Local>::default();
    builder.set_claim(ExpirationClaim::try_from(claims.exp.as_str())?);
    builder.set_claim(IssuedAtClaim::try_from(claims.iat.as_str())?);
    builder.set_claim(NotBeforeClaim::try_from(claims.nbf.as_str())?);
    builder.set_claim(SubjectClaim::from(claims.sub.as_str()));
    builder.set_footer(footer);

    let builder = builder
        .claim("sid", claims.sid.as_str())
        .map_err(|e| PasetoError::BuildError(format!("sid claim: {e}")))?
        .claim("actor", claims.actor.as_str())
        .map_err(|e| PasetoError::BuildError(format!("actor claim: {e}")))?
        .claim("provider", claims.provider.as_str())
        .map_err(|e| PasetoError::BuildError(format!("provider claim: {e}")))?
        .claim("provider_sub", claims.provider_sub.as_str())
        .map_err(|e| PasetoError::BuildError(format!("provider_sub claim: {e}")))?;

    let mut builder = if let Some(rv) = claims.roles_version {
        builder
            .claim("roles_version", rv)
            .map_err(|e| PasetoError::BuildError(format!("roles_version claim: {e}")))?
    } else {
        builder
    };

    builder
        .build(&key)
        .map_err(|e| PasetoError::BuildError(format!("{e}")))
}

fn try_parse_token(
    token: &str,
    key: &PasetoSymmetricKey<V4, Local>,
    footer: &Footer,
) -> Result<serde_json::Value, PasetoError> {
    PasetoParser::<V4, Local>::default()
        .set_footer(footer.clone())
        .parse(token, key)
        .map_err(|e| {
            let msg = format!("{e}");
            if msg.contains("expired") || msg.contains("Expired") {
                PasetoError::Expired
            } else if msg.contains("not yet valid") || msg.contains("NotYetValid") {
                PasetoError::NotYetValid
            } else {
                PasetoError::Tampered
            }
        })
}

pub fn decode_session_token(
    keyring: &PasetoKeyring,
    token: &str,
) -> Result<SessionClaims, PasetoError> {
    let active_key = keyring.active.symmetric_key();
    let active_footer = Footer::from(keyring.active.id.as_str());

    let json_value = match try_parse_token(token, &active_key, &active_footer) {
        Ok(v) => v,
        Err(PasetoError::Tampered) => {
            if let Some(prev) = &keyring.previous {
                let prev_key = prev.symmetric_key();
                let prev_footer = Footer::from(prev.id.as_str());
                try_parse_token(token, &prev_key, &prev_footer)?
            } else {
                return Err(PasetoError::Tampered);
            }
        }
        Err(e) => return Err(e),
    };

    let sid = json_value["sid"]
        .as_str()
        .ok_or(PasetoError::InvalidPayload)?
        .to_owned();
    let sub = json_value["sub"]
        .as_str()
        .ok_or(PasetoError::InvalidPayload)?
        .to_owned();
    let actor = json_value["actor"]
        .as_str()
        .ok_or(PasetoError::InvalidPayload)?
        .to_owned();
    let provider = json_value["provider"]
        .as_str()
        .ok_or(PasetoError::InvalidPayload)?
        .to_owned();
    let provider_sub = json_value["provider_sub"]
        .as_str()
        .ok_or(PasetoError::InvalidPayload)?
        .to_owned();
    let iat = json_value["iat"]
        .as_str()
        .ok_or(PasetoError::InvalidPayload)?
        .to_owned();
    let nbf = json_value["nbf"]
        .as_str()
        .ok_or(PasetoError::InvalidPayload)?
        .to_owned();
    let exp = json_value["exp"]
        .as_str()
        .ok_or(PasetoError::InvalidPayload)?
        .to_owned();
    let roles_version = json_value["roles_version"].as_u64();

    Ok(SessionClaims {
        sid,
        sub,
        actor,
        provider,
        provider_sub,
        iat,
        nbf,
        exp,
        roles_version,
    })
}

fn hex_decode(hex: &str) -> Result<Vec<u8>, ()> {
    if hex.len() % 2 != 0 {
        return Err(());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

pub fn now_rfc3339() -> String {
    OffsetDateTime::now_utc().format(&Rfc3339).unwrap()
}

pub fn future_rfc3339(duration: std::time::Duration) -> String {
    let dt = OffsetDateTime::now_utc() + time::Duration::seconds(duration.as_secs() as i64);
    dt.format(&Rfc3339).unwrap()
}

pub fn random_session_id() -> String {
    let mut bytes = [0u8; 24];
    rand::rng().fill_bytes(&mut bytes);
    hex_encode(&bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_keyring() -> PasetoKeyring {
        let key = PasetoKey::new(
            "test-key-1".to_owned(),
            *b"wubbalubbadubdubwubbalubbadubdub",
        );
        PasetoKeyring::new(key)
    }

    fn test_claims() -> SessionClaims {
        let now = OffsetDateTime::now_utc();
        let exp = now + time::Duration::hours(12);
        SessionClaims {
            sid: "session-abc123".to_owned(),
            sub: "user-1".to_owned(),
            actor: "actor-1".to_owned(),
            provider: "github".to_owned(),
            provider_sub: "gh-12345".to_owned(),
            iat: now.format(&Rfc3339).unwrap(),
            nbf: now.format(&Rfc3339).unwrap(),
            exp: exp.format(&Rfc3339).unwrap(),
            roles_version: Some(1),
        }
    }

    #[test]
    fn encode_and_decode_roundtrip() {
        let keyring = test_keyring();
        let claims = test_claims();
        let token = encode_session_token(&keyring, &claims).unwrap();
        assert!(token.starts_with("v4.local."));

        let decoded = decode_session_token(&keyring, &token).unwrap();
        assert_eq!(decoded.sid, claims.sid);
        assert_eq!(decoded.sub, claims.sub);
        assert_eq!(decoded.actor, claims.actor);
        assert_eq!(decoded.provider, claims.provider);
        assert_eq!(decoded.provider_sub, claims.provider_sub);
        assert_eq!(decoded.roles_version, Some(1));
    }

    #[test]
    fn expired_token_is_rejected() {
        let keyring = test_keyring();
        let now = OffsetDateTime::now_utc();
        let claims = SessionClaims {
            sid: "session-expired".to_owned(),
            sub: "user-1".to_owned(),
            actor: "actor-1".to_owned(),
            provider: "github".to_owned(),
            provider_sub: "gh-12345".to_owned(),
            iat: (now - time::Duration::hours(24)).format(&Rfc3339).unwrap(),
            nbf: (now - time::Duration::hours(24)).format(&Rfc3339).unwrap(),
            exp: (now - time::Duration::hours(12)).format(&Rfc3339).unwrap(),
            roles_version: None,
        };
        let token = encode_session_token(&keyring, &claims).unwrap();
        let result = decode_session_token(&keyring, &token);
        assert!(matches!(result, Err(PasetoError::Expired)));
    }

    #[test]
    fn wrong_key_is_rejected() {
        let keyring = test_keyring();
        let claims = test_claims();
        let token = encode_session_token(&keyring, &claims).unwrap();

        let other_key = PasetoKey::new(
            "test-key-1".to_owned(),
            *b"differentkeytestkeydiffkeytest12",
        );
        let other_keyring = PasetoKeyring::new(other_key);
        let result = decode_session_token(&other_keyring, &token);
        assert!(matches!(result, Err(PasetoError::Tampered)));
    }

    #[test]
    fn unknown_kid_is_rejected() {
        let keyring = test_keyring();
        let claims = test_claims();
        let token = encode_session_token(&keyring, &claims).unwrap();

        let other_key = PasetoKey::new(
            "different-kid".to_owned(),
            *b"wubbalubbadubdubwubbalubbadubdub",
        );
        let other_keyring = PasetoKeyring::new(other_key);
        let result = decode_session_token(&other_keyring, &token);
        assert!(matches!(result, Err(PasetoError::Tampered)));
    }

    #[test]
    fn key_rotation_accepts_previous_key() {
        let old_key = PasetoKey::new("key-v1".to_owned(), *b"oldkeyoldkeyoldkeyoldkeyoldkey!!");
        let old_keyring = PasetoKeyring::new(old_key.clone());
        let claims = test_claims();
        let token = encode_session_token(&old_keyring, &claims).unwrap();

        let new_key = PasetoKey::new("key-v2".to_owned(), *b"newkeynewkeynewkeynewkeynewkey!!");
        let rotated_keyring = PasetoKeyring::with_previous(new_key, old_key);
        let decoded = decode_session_token(&rotated_keyring, &token).unwrap();
        assert_eq!(decoded.sid, claims.sid);
    }

    #[test]
    fn generate_produces_valid_key() {
        let key = PasetoKey::generate("gen-key".to_owned());
        let keyring = PasetoKeyring::new(key);
        let claims = test_claims();
        let token = encode_session_token(&keyring, &claims).unwrap();
        let decoded = decode_session_token(&keyring, &token).unwrap();
        assert_eq!(decoded.sid, claims.sid);
    }

    #[test]
    fn from_hex_parses_valid_key() {
        let hex = "77756262616c7562626164756264756277756262616c75626261647562647562";
        let key = PasetoKey::from_hex("hex-key".to_owned(), hex).unwrap();
        assert_eq!(&key.key, b"wubbalubbadubdubwubbalubbadubdub");
    }

    #[test]
    fn random_session_id_is_unique() {
        let a = random_session_id();
        let b = random_session_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 48);
    }
}
