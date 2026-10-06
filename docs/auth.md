# Cesta account authentication

Ticketing uses a Cesta account, never a ČD account. There is no guest ticketing session in this release. Public journey search is available without authentication, but live ČD prices and all `/ticketing` routes require a Cesta access token.

## Routes

- `POST /auth/register` accepts `email`, `password` (minimum 8 characters), and optional `display_name`.
- `GET/PATCH /me/profile` stores travel preferences in PostgreSQL with a monotonic version.
- `POST /auth/change-password` requires the current password, writes a new Argon2id hash, increments
  the account authentication version and revokes all refresh sessions. Existing access JWTs stop
  working because every authenticated request compares the signed authentication version with the
  current database row.
- `POST /auth/password-reset` always returns the same accepted response. It stores only a SHA-256
  token hash, expires the token after 30 minutes, invalidates older tokens and rate-limits requests.
  Delivery uses `PASSWORD_RESET_URL`, `PASSWORD_RESET_DELIVERY_URL` and the secret
  `PASSWORD_RESET_DELIVERY_API_KEY`. `POST /auth/password-reset/complete` consumes the token once,
  replaces the Argon2id hash and revokes all sessions.
- `GET/POST/PATCH/DELETE /me/saved-routes` persists complete cross-device routes. Incremental reads
  accept `since`; deletes remain as versioned tombstones. Updates and deletes require the caller's
  last `expected_version`, so concurrent changes fail instead of overwriting a newer device.

Account deletion is a soft deletion of the account identity plus hard deletion of profiles, saved
places/routes, notification preferences, reset tokens, devices and push subscriptions. The email
and display name are removed, credentials are replaced and all sessions are revoked. Ticket orders,
issued documents, refunds and their audit trail remain linked to the pseudonymous user UUID where
contractual, tax, fraud-prevention or transport-law retention requires them; they are not exposed by
the deleted account and must be removed later by the operator's retention schedule.
- `POST /auth/login` accepts `email`, `password`, and optional `device_name`.
- `POST /auth/refresh` accepts `refresh_token` and rotates it. A refresh token is single-use; concurrent or repeated reuse returns `401 unauthorized`.
- `POST /auth/logout` accepts `refresh_token` and revokes it. Logout is idempotent.

Register, login, and refresh return:

```json
{
  "access_token": "jwt",
  "refresh_token": "opaque-token",
  "token_type": "Bearer",
  "expires_in_seconds": 900,
  "user": {
    "id": "uuid",
    "email": "person@example.cz",
    "display_name": "Name",
    "roles": ["user"]
  }
}
```

Access tokens expire after 15 minutes. Refresh tokens expire after 30 days, are stored only as SHA-256 hashes by the backend, and must be stored in platform secure storage by the app. Passwords use Argon2id. Tokens and passwords must never be logged.

Send authenticated requests with `Authorization: Bearer <access_token>`. On an expired access token, perform one refresh, persist the returned replacement refresh token, and retry the request once. If refresh returns `401`, clear the local session and require login.

Request and response schemas are authoritative in `GET /openapi.json`.
