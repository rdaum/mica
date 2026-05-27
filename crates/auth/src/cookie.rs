pub const SESSION_COOKIE_NAME: &str = "__Host-conatus_session";

pub fn build_session_cookie(token: &str) -> String {
    format!("{SESSION_COOKIE_NAME}={token}; HttpOnly; Secure; SameSite=Lax; Path=/")
}

pub fn build_clear_session_cookie() -> String {
    format!("{SESSION_COOKIE_NAME}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0")
}

pub fn extract_session_cookie(cookie_header: &str) -> Option<String> {
    for part in cookie_header.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix(SESSION_COOKIE_NAME) {
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
        assert!(cookie.contains("__Host-conatus_session=test-token"));
    }

    #[test]
    fn clear_cookie_has_max_age_zero() {
        let cookie = build_clear_session_cookie();
        assert!(cookie.contains("Max-Age=0"));
        assert!(cookie.contains("__Host-conatus_session="));
    }

    #[test]
    fn extract_cookie_from_header() {
        let header = "other=value; __Host-conatus_session=v4.local.abc123; another=thing";
        let result = extract_session_cookie(header);
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
        let header = "__Host-conatus_session=";
        let result = extract_session_cookie(header);
        assert!(result.is_none());
    }

    #[test]
    fn extract_cookie_single_cookie() {
        let header = "__Host-conatus_session=token123";
        let result = extract_session_cookie(header);
        assert_eq!(result, Some("token123".to_owned()));
    }
}
