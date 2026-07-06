# Cesta account authentication

Ticketing uses a Cesta account, never a ČD account. There is no guest ticketing session in this release. Public journey search is available without authentication, but live ČD prices and all `/ticketing` routes require a Cesta access token.

## Routes

- `POST /auth/register` accepts `email`, `password` (minimum 8 characters), and optional `display_name`.
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
