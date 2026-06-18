pub const SESSION_COOKIE_NAME: &str = "__Host-mica_session";
pub const INSECURE_SESSION_COOKIE_NAME: &str = "mica_session";

pub fn build_session_cookie(token: &str) -> String {
    build_session_cookie_with_options(SESSION_COOKIE_NAME, token, true)
}

pub fn build_clear_session_cookie() -> String {
    build_clear_session_cookie_with_options(SESSION_COOKIE_NAME, true)
}

pub fn build_session_cookie_with_options(name: &str, token: &str, secure: bool) -> String {
    if secure {
        return format!("{name}={token}; HttpOnly; Secure; SameSite=Lax; Path=/");
    }
    format!("{name}={token}; HttpOnly; SameSite=Lax; Path=/")
}

pub fn build_clear_session_cookie_with_options(name: &str, secure: bool) -> String {
    if secure {
        return format!("{name}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0");
    }
    format!("{name}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0")
}

pub fn extract_session_cookie(cookie_header: &str) -> Option<String> {
    extract_session_cookie_named(cookie_header, SESSION_COOKIE_NAME)
}

pub fn extract_session_cookie_named(cookie_header: &str, name: &str) -> Option<String> {
    for part in cookie_header.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix(name) {
            let value = value.strip_prefix('=')?;
            if value.is_empty() {
                return None;
            }
            return Some(value.to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_cookie_has_secure_attributes() {
        let cookie = build_session_cookie("test-token");
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("Secure"));
        assert!(cookie.contains("SameSite=Lax"));
        assert!(cookie.contains("Path=/"));
        assert!(cookie.contains("__Host-mica_session=test-token"));
    }

    #[test]
    fn build_insecure_cookie_omits_secure_attribute() {
        let cookie = build_session_cookie_with_options("mica_session", "test-token", false);
        assert!(cookie.contains("HttpOnly"));
        assert!(!cookie.contains("Secure"));
        assert!(cookie.contains("SameSite=Lax"));
        assert!(cookie.contains("Path=/"));
        assert!(cookie.contains("mica_session=test-token"));
    }

    #[test]
    fn clear_cookie_has_max_age_zero() {
        let cookie = build_clear_session_cookie();
        assert!(cookie.contains("Max-Age=0"));
        assert!(cookie.contains("__Host-mica_session="));
    }

    #[test]
    fn extract_cookie_from_header() {
        let header = "other=value; __Host-mica_session=v4.local.abc123; another=thing";
        let result = extract_session_cookie(header);
        assert_eq!(result, Some("v4.local.abc123".to_owned()));
    }

    #[test]
    fn extract_named_cookie_from_header() {
        let header = "other=value; mica_session=v4.local.abc123; another=thing";
        let result = extract_session_cookie_named(header, "mica_session");
        assert_eq!(result, Some("v4.local.abc123".to_owned()));
    }

    #[test]
    fn extract_cookie_returns_none_when_missing() {
        let header = "other=value; another=thing";
        let result = extract_session_cookie(header);
        assert!(result.is_none());
    }

    #[test]
    fn extract_cookie_returns_none_for_empty_value() {
        let header = "__Host-mica_session=";
        let result = extract_session_cookie(header);
        assert!(result.is_none());
    }

    #[test]
    fn extract_cookie_single_cookie() {
        let header = "__Host-mica_session=token123";
        let result = extract_session_cookie(header);
        assert_eq!(result, Some("token123".to_owned()));
    }
}
