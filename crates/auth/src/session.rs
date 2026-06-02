use mica_driver::CompioTaskDriver;
use mica_var::{Symbol, Value};
use std::sync::Arc;

use crate::{hash_password, verify_password};

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

fn stable_user_symbol(provider: &str, provider_sub: &str) -> String {
    stable_subject_symbol("source/user", provider, provider_sub)
}

fn stable_person_symbol(provider: &str, provider_sub: &str) -> String {
    stable_subject_symbol("source/person", provider, provider_sub)
}

fn stable_subject_symbol(prefix: &str, provider: &str, provider_sub: &str) -> String {
    fn push_sanitized(out: &mut String, value: &str) {
        let mut last_was_separator = false;
        for c in value.chars() {
            if c.is_ascii_alphanumeric() {
                out.push(c.to_ascii_lowercase());
                last_was_separator = false;
            } else if !last_was_separator {
                out.push('_');
                last_was_separator = true;
            }
        }
        while out.ends_with('_') {
            out.pop();
        }
    }

    let mut symbol = prefix.to_owned();
    symbol.push('_');
    push_sanitized(&mut symbol, provider);
    symbol.push('_');
    push_sanitized(&mut symbol, provider_sub);
    symbol
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalAuthenticatedUser {
    pub user_id: String,
    pub provider_sub: String,
    pub login: String,
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
        provider_sub: &str,
    ) -> Result<String, String> {
        let escaped_login = mica_escape(login);
        let escaped_provider = mica_escape(provider);
        let escaped_provider_sub = mica_escape(provider_sub);
        let user_symbol = stable_user_symbol(provider, provider_sub);
        let escaped_user_symbol = mica_escape(&user_symbol);
        let source = format!(
            r#"
let provider = to_symbol("{escaped_provider}")
let provider_sub = "{escaped_provider_sub}"
let user_symbol = to_symbol("{escaped_user_symbol}")
let identity = make_identity(user_symbol)
source/UserExternalIdentity(provider, provider_sub, identity) || assert source/UserExternalIdentity(provider, provider_sub, identity)
assert source/User(identity)
retract source/UserProvider(identity, _)
assert source/UserProvider(identity, provider)
retract source/UserLogin(identity, _)
assert source/UserLogin(identity, "{escaped_login}")
source/UserRole(identity, :source/role_viewer) || assert source/UserRole(identity, :source/role_viewer)
return "{escaped_user_symbol}"
"#,
            escaped_login = escaped_login,
            escaped_provider = escaped_provider,
            escaped_provider_sub = escaped_provider_sub,
            escaped_user_symbol = escaped_user_symbol,
        );
        let report = self
            .driver
            .submit_root_source_report(source)
            .await
            .map_err(|e| format!("failed to ensure user exists: {e}"))?;
        match report.outcome {
            mica_runtime::TaskOutcome::Complete { value, .. } => value
                .with_str(str::to_owned)
                .ok_or_else(|| "ensure user returned non-string identity name".to_owned()),
            mica_runtime::TaskOutcome::Aborted { error, .. } => {
                Err(format!("ensure user aborted: {error}"))
            }
            mica_runtime::TaskOutcome::Suspended { .. } => {
                Err("ensure user suspended unexpectedly".to_owned())
            }
        }
    }

    pub async fn ensure_user_person(
        &self,
        user_id: &str,
        provider: &str,
        provider_sub: &str,
        display_name: &str,
    ) -> Result<String, String> {
        let escaped_user_id = mica_escape(user_id);
        let escaped_display_name = mica_escape(display_name);
        let person_symbol = stable_person_symbol(provider, provider_sub);
        let escaped_person_symbol = mica_escape(&person_symbol);
        let source = format!(
            r#"
let user = make_identity(to_symbol("{escaped_user_id}"))
let person_symbol = to_symbol("{escaped_person_symbol}")
let person = make_identity(person_symbol)
assert source/Person(person)
source/UserPerson(user, person) || assert source/UserPerson(user, person)
let current_default = one source/DefaultUserPerson(user, ?person)
current_default != nothing || assert source/DefaultUserPerson(user, person)
retract source/DisplayName(person, _)
assert source/DisplayName(person, "{escaped_display_name}")
retract source/Description(person, _)
assert source/Description(person, "{escaped_display_name}, present through authenticated login.")
let room = one source/DefaultRoom(?room)
if room != nothing && person.source/locatedIn == nothing
  assert source/LocatedIn(person, room)
end
return "{escaped_person_symbol}"
"#,
            escaped_user_id = escaped_user_id,
            escaped_person_symbol = escaped_person_symbol,
            escaped_display_name = escaped_display_name,
        );
        let report = self
            .driver
            .submit_root_source_report(source)
            .await
            .map_err(|e| format!("failed to ensure user person exists: {e}"))?;
        match report.outcome {
            mica_runtime::TaskOutcome::Complete { value, .. } => value
                .with_str(str::to_owned)
                .ok_or_else(|| "ensure user person returned non-string identity name".to_owned()),
            mica_runtime::TaskOutcome::Aborted { error, .. } => {
                Err(format!("ensure user person aborted: {error}"))
            }
            mica_runtime::TaskOutcome::Suspended { .. } => {
                Err("ensure user person suspended unexpectedly".to_owned())
            }
        }
    }

    pub async fn create_local_user(
        &self,
        login: &str,
        password: &str,
        display_name: &str,
    ) -> Result<LocalAuthenticatedUser, String> {
        let provider_sub = normalize_local_login(login)?;
        let user_id = self
            .ensure_user_exists(login, "local", &provider_sub)
            .await?;
        let password_hash =
            hash_password(password).map_err(|error| format!("failed to hash password: {error}"))?;
        self.set_local_password_hash(&user_id, &password_hash)
            .await?;
        self.ensure_user_person(&user_id, "local", &provider_sub, display_name)
            .await?;
        Ok(LocalAuthenticatedUser {
            user_id,
            provider_sub,
            login: login.to_owned(),
        })
    }

    async fn set_local_password_hash(
        &self,
        user_id: &str,
        password_hash: &str,
    ) -> Result<(), String> {
        let escaped_user_id = mica_escape(user_id);
        let escaped_password_hash = mica_escape(password_hash);
        let source = format!(
            r#"
let user = make_identity(to_symbol("{escaped_user_id}"))
retract source/LocalPasswordHash(user, _)
assert source/LocalPasswordHash(user, "{escaped_password_hash}")
return true
"#,
            escaped_user_id = escaped_user_id,
            escaped_password_hash = escaped_password_hash,
        );
        let report = self
            .driver
            .submit_root_source_report(source)
            .await
            .map_err(|e| format!("failed to set local password hash: {e}"))?;
        match report.outcome {
            mica_runtime::TaskOutcome::Complete { .. } => Ok(()),
            mica_runtime::TaskOutcome::Aborted { error, .. } => {
                Err(format!("set local password hash aborted: {error}"))
            }
            mica_runtime::TaskOutcome::Suspended { .. } => {
                Err("set local password hash suspended unexpectedly".to_owned())
            }
        }
    }

    pub async fn authenticate_local_user(
        &self,
        login: &str,
        password: &str,
    ) -> Result<Option<LocalAuthenticatedUser>, String> {
        let provider_sub = normalize_local_login(login)?;
        let user_symbol = stable_user_symbol("local", &provider_sub);
        let escaped_provider_sub = mica_escape(&provider_sub);
        let escaped_user_symbol = mica_escape(&user_symbol);
        let source = format!(
            r#"
let user = one source/UserExternalIdentity(:local, "{escaped_provider_sub}", ?user)
user != nothing || return nothing
let password_hash = one source/LocalPasswordHash(user, ?password_hash)
password_hash != nothing || return nothing
return {{:user_id -> "{escaped_user_symbol}", :password_hash -> password_hash}}
"#,
            escaped_provider_sub = escaped_provider_sub,
            escaped_user_symbol = escaped_user_symbol,
        );
        let report = self
            .driver
            .submit_root_source_report(source)
            .await
            .map_err(|e| format!("failed to authenticate local user: {e}"))?;
        let map = match report.outcome {
            mica_runtime::TaskOutcome::Complete { value, .. } => {
                if value == Value::nothing() {
                    return Ok(None);
                }
                value
            }
            mica_runtime::TaskOutcome::Aborted { error, .. } => {
                return Err(format!("authenticate local user aborted: {error}"));
            }
            mica_runtime::TaskOutcome::Suspended { .. } => {
                return Err("authenticate local user suspended unexpectedly".to_owned());
            }
        };
        let user_id = map_string(&map, "user_id")?;
        let password_hash = map_string(&map, "password_hash")?;
        let valid = verify_password(password, &password_hash)
            .map_err(|error| format!("failed to verify password: {error}"))?;
        if !valid {
            return Ok(None);
        }
        Ok(Some(LocalAuthenticatedUser {
            user_id,
            provider_sub,
            login: login.to_owned(),
        }))
    }
}

fn normalize_local_login(login: &str) -> Result<String, String> {
    let normalized = login.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err("local login must not be empty".to_owned());
    }
    Ok(normalized)
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
make_relation(:source/User, 1)
make_functional_relation(:source/UserExternalIdentity, 3, [0, 1])
make_relation(:source/UserProvider, 2)
make_functional_relation(:source/UserLogin, 2, [0])
make_relation(:source/UserRole, 2)
make_functional_relation(:source/LocalPasswordHash, 2, [0])
make_relation(:source/Person, 1)
make_relation(:source/UserPerson, 2)
make_functional_relation(:source/DefaultUserPerson, 2, [0])
make_functional_relation(:source/DisplayName, 2, [0])
make_functional_relation(:source/Description, 2, [0])
make_functional_relation(:source/LocatedIn, 2, [0])
make_relation(:source/DefaultRoom, 1)
make_identity(:source/room)
assert source/DefaultRoom(#source/room)
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

    #[test]
    fn ensure_user_uses_provider_subject_as_stable_identity() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let mut runner = SourceRunner::new_empty();
            runner.run_filein(session_schema()).unwrap();
            let store = MicaSessionStore::new(Arc::new(CompioTaskDriver::spawn(runner).unwrap()));

            let user_id = store
                .ensure_user_exists("alice-login", "github", "1001")
                .await
                .unwrap();

            assert_eq!(user_id, "source/user_github_1001");
            let report = store
                .driver
                .submit_root_source_report(
                    r#"
source/User(#source/user_github_1001) || return false
source/UserExternalIdentity(:github, "1001", #source/user_github_1001) || return false
source/UserProvider(#source/user_github_1001, :github) || return false
source/UserLogin(#source/user_github_1001, "alice-login") || return false
source/UserRole(#source/user_github_1001, :source/role_viewer) || return false
return true
"#
                    .to_owned(),
                )
                .await
                .unwrap();
            assert!(matches!(
                report.outcome,
                mica_runtime::TaskOutcome::Complete { value, .. } if value == Value::from(true)
            ));
        });
    }

    #[test]
    fn ensure_user_preserves_identity_when_login_changes() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let mut runner = SourceRunner::new_empty();
            runner.run_filein(session_schema()).unwrap();
            let store = MicaSessionStore::new(Arc::new(CompioTaskDriver::spawn(runner).unwrap()));

            let first = store
                .ensure_user_exists("old-login", "github", "1001")
                .await
                .unwrap();
            let second = store
                .ensure_user_exists("new-login", "github", "1001")
                .await
                .unwrap();

            assert_eq!(first, second);
            let report = store
                .driver
                .submit_root_source_report(
                    r#"
source/UserExternalIdentity(:github, "1001", #source/user_github_1001) || return false
source/UserLogin(#source/user_github_1001, "new-login") || return false
source/UserLogin(#source/user_github_1001, "old-login") && return false
return true
"#
                    .to_owned(),
                )
                .await
                .unwrap();
            assert!(matches!(
                report.outcome,
                mica_runtime::TaskOutcome::Complete { value, .. } if value == Value::from(true)
            ));
        });
    }

    #[test]
    fn ensure_user_person_attaches_default_person() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let mut runner = SourceRunner::new_empty();
            runner.run_filein(session_schema()).unwrap();
            let store = MicaSessionStore::new(Arc::new(CompioTaskDriver::spawn(runner).unwrap()));

            let user_id = store
                .ensure_user_exists("alice-login", "github", "1001")
                .await
                .unwrap();
            let person_id = store
                .ensure_user_person(&user_id, "github", "1001", "Alice Liddell")
                .await
                .unwrap();

            assert_eq!(person_id, "source/person_github_1001");
            let report = store
                .driver
                .submit_root_source_report(
                    r#"
source/Person(#source/person_github_1001) || return false
source/UserPerson(#source/user_github_1001, #source/person_github_1001) || return false
source/DefaultUserPerson(#source/user_github_1001, #source/person_github_1001) || return false
source/DisplayName(#source/person_github_1001, "Alice Liddell") || return false
source/Description(#source/person_github_1001, "Alice Liddell, present through authenticated login.") || return false
source/LocatedIn(#source/person_github_1001, #source/room) || return false
return true
"#
                    .to_owned(),
                )
                .await
                .unwrap();
            assert!(matches!(
                report.outcome,
                mica_runtime::TaskOutcome::Complete { value, .. } if value == Value::from(true)
            ));
        });
    }

    #[test]
    fn local_password_authenticates_against_stored_hash() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let mut runner = SourceRunner::new_empty();
            runner.run_filein(session_schema()).unwrap();
            let store = MicaSessionStore::new(Arc::new(CompioTaskDriver::spawn(runner).unwrap()));

            let created = store
                .create_local_user("Alice", "correct horse", "Alice")
                .await
                .unwrap();
            assert_eq!(created.user_id, "source/user_local_alice");
            assert_eq!(created.provider_sub, "alice");

            let rejected = store
                .authenticate_local_user("alice", "wrong")
                .await
                .unwrap();
            assert_eq!(rejected, None);

            let authenticated = store
                .authenticate_local_user("ALICE", "correct horse")
                .await
                .unwrap()
                .expect("local password should authenticate");
            assert_eq!(authenticated.user_id, "source/user_local_alice");
            assert_eq!(authenticated.provider_sub, "alice");
        });
    }
}
