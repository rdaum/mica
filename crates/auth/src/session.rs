use mica_driver::CompioTaskDriver;
use mica_var::{Symbol, Value};
use std::sync::Arc;

fn mica_escape(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(c),
        }
    }
    escaped
}

#[derive(Clone, Debug)]
pub struct SessionRecord {
    pub session_id: String,
    pub user_id: String,
    pub actor: String,
    pub provider: String,
    pub provider_sub: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub revoked_at: Option<i64>,
    pub last_seen_at: i64,
    pub roles_version: Option<u64>,
    pub user_agent_hash: Option<String>,
}

pub struct MicaSessionStore {
    driver: Arc<CompioTaskDriver>,
}

impl MicaSessionStore {
    pub fn new(driver: Arc<CompioTaskDriver>) -> Self {
        Self { driver }
    }

    pub async fn create_session(&self, record: &SessionRecord) -> Result<(), String> {
        let escaped_user_id = mica_escape(&record.user_id);
        let escaped_actor = mica_escape(&record.actor);
        let escaped_provider = mica_escape(&record.provider);
        let escaped_provider_sub = mica_escape(&record.provider_sub);
        let source = format!(
            r#"
let session_id = to_symbol("{session_id}")
let user_id = to_symbol("{escaped_user_id}")
let actor_id = to_symbol("{escaped_actor}")
retract source/AuthSession(session_id)
assert source/AuthSession(session_id)
retract source/SessionUser(session_id, _)
assert source/SessionUser(session_id, user_id)
retract source/SessionActor(session_id, _)
assert source/SessionActor(session_id, actor_id)
retract source/SessionProvider(session_id, _)
assert source/SessionProvider(session_id, to_symbol("{escaped_provider}"))
retract source/SessionProviderSub(session_id, _)
assert source/SessionProviderSub(session_id, "{escaped_provider_sub}")
retract source/SessionIssuedAt(session_id, _)
assert source/SessionIssuedAt(session_id, {issued_at})
retract source/SessionExpiresAt(session_id, _)
assert source/SessionExpiresAt(session_id, {expires_at})
retract source/SessionRevokedAt(session_id, _)
retract source/SessionLastSeenAt(session_id, _)
assert source/SessionLastSeenAt(session_id, {last_seen_at})
return true
"#,
            session_id = record.session_id,
            escaped_user_id = escaped_user_id,
            escaped_actor = escaped_actor,
            escaped_provider = escaped_provider,
            escaped_provider_sub = escaped_provider_sub,
            issued_at = record.issued_at,
            expires_at = record.expires_at,
            last_seen_at = record.last_seen_at,
        );
        let report = self
            .driver
            .submit_root_source_report(source)
            .await
            .map_err(|e| format!("failed to create session: {e}"))?;
        match report.outcome {
            mica_runtime::TaskOutcome::Complete { .. } => Ok(()),
            mica_runtime::TaskOutcome::Aborted { error, .. } => {
                Err(format!("create session aborted: {error}"))
            }
            mica_runtime::TaskOutcome::Suspended { .. } => {
                Err("create session suspended unexpectedly".to_owned())
            }
        }
    }

    pub async fn lookup_session(&self, session_id: &str) -> Result<Option<SessionRecord>, String> {
        let source = format!(
            r#"
let session_id = to_symbol("{session_id}")
source/AuthSession(session_id) || return nothing
let user = one source/SessionUser(session_id, ?user)
let actor = one source/SessionActor(session_id, ?actor)
let provider = one source/SessionProvider(session_id, ?provider)
let provider_sub = one source/SessionProviderSub(session_id, ?provider_sub)
let issued_at = one source/SessionIssuedAt(session_id, ?issued_at)
let expires_at = one source/SessionExpiresAt(session_id, ?expires_at)
let revoked_at = one source/SessionRevokedAt(session_id, ?revoked_at)
let last_seen_at = one source/SessionLastSeenAt(session_id, ?last_seen_at)
return {{:session_id -> "{session_id}", :user_id -> user, :actor -> actor, :provider -> provider, :provider_sub -> provider_sub, :issued_at -> issued_at, :expires_at -> expires_at, :revoked_at -> revoked_at, :last_seen_at -> last_seen_at}}
"#,
            session_id = session_id,
        );
        let report = self
            .driver
            .submit_root_source_report(source)
            .await
            .map_err(|e| format!("failed to lookup session: {e}"))?;
        match report.outcome {
            mica_runtime::TaskOutcome::Complete { value, .. } => {
                if value == Value::nothing() {
                    return Ok(None);
                }
                let map = &value;
                let user_id = map_string(map, "user_id")?;
                let actor = map_string(map, "actor")?;
                let provider = map_string(map, "provider")?;
                let provider_sub = map_string(map, "provider_sub")?;
                let issued_at = map_int(map, "issued_at")?;
                let expires_at = map_int(map, "expires_at")?;
                let revoked_at = map_optional_int(map, "revoked_at")?;
                let last_seen_at = map_int(map, "last_seen_at")?;

                Ok(Some(SessionRecord {
                    session_id: session_id.to_owned(),
                    user_id,
                    actor,
                    provider,
                    provider_sub,
                    issued_at,
                    expires_at,
                    revoked_at,
                    last_seen_at,
                    roles_version: None,
                    user_agent_hash: None,
                }))
            }
            mica_runtime::TaskOutcome::Aborted { error, .. } => {
                Err(format!("lookup session aborted: {error}"))
            }
            mica_runtime::TaskOutcome::Suspended { .. } => {
                Err("lookup session suspended unexpectedly".to_owned())
            }
        }
    }

    pub async fn revoke_session(&self, session_id: &str) -> Result<(), String> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let source = format!(
            r#"
let session_id = to_symbol("{session_id}")
source/AuthSession(session_id) || return false
retract source/SessionRevokedAt(session_id, _)
assert source/SessionRevokedAt(session_id, {now})
return true
"#,
            session_id = session_id,
            now = now,
        );
        let report = self
            .driver
            .submit_root_source_report(source)
            .await
            .map_err(|e| format!("failed to revoke session: {e}"))?;
        match report.outcome {
            mica_runtime::TaskOutcome::Complete { .. } => Ok(()),
            mica_runtime::TaskOutcome::Aborted { error, .. } => {
                Err(format!("revoke session aborted: {error}"))
            }
            mica_runtime::TaskOutcome::Suspended { .. } => {
                Err("revoke session suspended unexpectedly".to_owned())
            }
        }
    }

    pub async fn update_last_seen(&self, session_id: &str) -> Result<(), String> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let source = format!(
            r#"
let session_id = to_symbol("{session_id}")
source/AuthSession(session_id) || return false
retract source/SessionLastSeenAt(session_id, _)
assert source/SessionLastSeenAt(session_id, {now})
return true
"#,
            session_id = session_id,
            now = now,
        );
        let _ = self
            .driver
            .submit_root_source_report(source)
            .await
            .map_err(|e| format!("failed to update last seen: {e}"))?;
        Ok(())
    }

    pub async fn ensure_user_exists(
        &self,
        login: &str,
        provider: &str,
        _provider_sub: &str,
    ) -> Result<(), String> {
        let escaped_login = mica_escape(login);
        let escaped_provider = mica_escape(provider);
        let source = format!(
            r#"
let identity = make_identity(to_symbol("{escaped_login}"))
assert source/User(identity)
retract source/UserProvider(identity, _)
assert source/UserProvider(identity, to_symbol("{escaped_provider}"))
retract source/UserLogin(identity, _)
assert source/UserLogin(identity, "{escaped_login}")
source/UserRole(identity, :source/role_viewer) || assert source/UserRole(identity, :source/role_viewer)
return true
"#,
            escaped_login = escaped_login,
            escaped_provider = escaped_provider,
        );
        let report = self
            .driver
            .submit_root_source_report(source)
            .await
            .map_err(|e| format!("failed to ensure user exists: {e}"))?;
        match report.outcome {
            mica_runtime::TaskOutcome::Complete { .. } => Ok(()),
            mica_runtime::TaskOutcome::Aborted { error, .. } => {
                Err(format!("ensure user aborted: {error}"))
            }
            mica_runtime::TaskOutcome::Suspended { .. } => {
                Err("ensure user suspended unexpectedly".to_owned())
            }
        }
    }
}

fn map_string(value: &Value, key: &str) -> Result<String, String> {
    let key_val = Value::symbol(Symbol::intern(key));
    let Some(inner) = value.map_get(&key_val) else {
        return Err(format!("missing session field: {key}"));
    };
    if let Some(value) = inner.with_str(str::to_owned) {
        return Ok(value);
    }
    if let Some(symbol) = inner.as_symbol().and_then(Symbol::name) {
        return Ok(symbol.to_owned());
    }
    Err(format!("session field {key} is not a string or symbol"))
}

fn map_int(value: &Value, key: &str) -> Result<i64, String> {
    let key_val = Value::symbol(Symbol::intern(key));
    let Some(inner) = value.map_get(&key_val) else {
        return Err(format!("missing session field: {key}"));
    };
    inner
        .as_int()
        .ok_or_else(|| format!("session field {key} is not an integer"))
}

fn map_optional_int(value: &Value, key: &str) -> Result<Option<i64>, String> {
    let key_val = Value::symbol(Symbol::intern(key));
    let Some(inner) = value.map_get(&key_val) else {
        return Ok(None);
    };
    if inner == Value::nothing() {
        return Ok(None);
    }
    inner
        .as_int()
        .map(Some)
        .ok_or_else(|| format!("session field {key} is not an integer or nothing"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_driver::CompioTaskDriver;
    use mica_runtime::SourceRunner;

    fn session_schema() -> &'static str {
        r#"
make_relation(:source/AuthSession, 1)
make_functional_relation(:source/SessionUser, 2, [0])
make_functional_relation(:source/SessionActor, 2, [0])
make_functional_relation(:source/SessionProvider, 2, [0])
make_functional_relation(:source/SessionProviderSub, 2, [0])
make_functional_relation(:source/SessionIssuedAt, 2, [0])
make_functional_relation(:source/SessionExpiresAt, 2, [0])
make_functional_relation(:source/SessionRevokedAt, 2, [0])
make_functional_relation(:source/SessionLastSeenAt, 2, [0])
"#
    }

    #[test]
    fn create_session_uses_unary_auth_session_relation() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let mut runner = SourceRunner::new_empty();
            runner.run_filein(session_schema()).unwrap();
            let store = MicaSessionStore::new(Arc::new(CompioTaskDriver::spawn(runner).unwrap()));
            let record = SessionRecord {
                session_id: "session-1".to_owned(),
                user_id: "alice".to_owned(),
                actor: "alice".to_owned(),
                provider: "github".to_owned(),
                provider_sub: "1001".to_owned(),
                issued_at: 10,
                expires_at: 20,
                revoked_at: None,
                last_seen_at: 10,
                roles_version: None,
                user_agent_hash: None,
            };

            store.create_session(&record).await.unwrap();

            let loaded = store
                .lookup_session("session-1")
                .await
                .unwrap()
                .expect("session should be stored");
            assert_eq!(loaded.user_id, "alice");
            assert_eq!(loaded.actor, "alice");
            assert_eq!(loaded.provider, "github");
            assert_eq!(loaded.provider_sub, "1001");
            assert_eq!(loaded.issued_at, 10);
            assert_eq!(loaded.expires_at, 20);
            assert_eq!(loaded.revoked_at, None);
            assert_eq!(loaded.last_seen_at, 10);
        });
    }
}
