# mica-auth

`mica-auth` provides authentication and session management for Mica
applications. It issues PASETO-signed session tokens, manages session records
in the live Mica relation store, and supports both OAuth-based (GitHub) and
local password authentication.

## What's Here

- `src/config.rs`: `AuthConfig` loaded from environment variables or
  constructed for development.
- `src/paseto.rs`: `PasetoKey`, `PasetoKeyring`, `SessionClaims`, and token
  encode/decode with support for key rotation.
- `src/session.rs`: `AuthSchema` plus `MicaSessionStore`, which creates, looks
  up, revokes, and updates sessions as app-selected Mica relation facts via the
  task driver.
- `src/oauth.rs`: GitHub OAuth helpers for building authorization URLs (with
  optional PKCE), exchanging codes for tokens, fetching user info, and checking
  organization membership.
- `src/password.rs`: Argon2 password hashing and verification for local
  password authentication.
- `src/cookie.rs`: Session cookie construction with secure attributes and
  extraction from `Cookie` headers.

## Role In Mica

This crate is the authentication layer between a Mica host surface (such as the
daemon's web host) and the running Mica runtime. It does not own HTTP routing,
OAuth callback handling, or transport-level concerns. Those stay in the host or
daemon code that calls into this crate.

Sessions are stored as Mica relation facts selected by `AuthSchema`, such as
`auth/AuthSession` and `auth/SessionUser` for the default namespace. Mica code
running inside the runtime can inspect and reason about active sessions through
ordinary relation queries without the auth crate knowing the application model.

Session tokens use PASETO v4 local (symmetric) encryption. The keyring supports
an active key and an optional previous key, making it safe to rotate keys
without immediately invalidating all existing sessions.

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
