# API architecture

The Cesta API follows the same separation-of-concerns principles as the referenced
`project-structure-api` template, expressed in Rust and Axum rather than TypeScript and Express.

## Request flow

```text
request
  -> HTTP middleware (CORS, body limit, request ID, tracing)
  -> route
  -> controller (extract and validate HTTP input, format HTTP output)
  -> service (business and security rules)
  -> repository (SQL and row mapping)
  -> PostgreSQL
```

Dependencies point inward. Repositories do not depend on controllers, and services do not format
HTTP responses. Cross-cutting infrastructure is configured once during startup.

## API service layout

```text
services/api/src/
  main.rs                  thin executable entrypoint
  lib.rs                   application composition and legacy domain internals
  config.rs                environment parsing and fail-fast validation
  error.rs                 API error-to-status mapping
  controllers/             HTTP handlers grouped by API concern
  http/routes.rs           route registration and HTTP middleware
  services/                business and security operations
  repositories/            database access and row mapping
  infrastructure/          database connection and migration lifecycle
  cd.rs                    external ČD adapter
  ticketing.rs             ticketing domain module and adapter orchestration
```

`lib.rs` still contains the mature admin, routing-query, fixture, and data-quality code. Move a
domain out only behind a tested seam; do not perform a mechanical rewrite that risks losing source
tracking or validation behavior.

## Rules for new endpoints

1. Register paths in the matching function in `http/routes.rs` and update `/openapi.json`.
2. Put request extraction and response formatting in a controller.
3. Put reusable business rules in a service. Keep controllers thin.
4. Put SQL and database row conversion in a repository. Never build SQL in a route.
5. Return `ApiError` for expected failures; do not expose internal error details or credentials.
6. Add controller/integration tests for status, body, authentication, and validation behavior.
7. Keep development fixtures explicit in response metadata and separate from imported data.

## Operational behavior

- `APP_ENV=production` rejects the development JWT secret, short JWT secrets, and wildcard CORS.
- `X-Request-Id` is accepted/generated, included in tracing spans, and returned on responses.
- Request bodies are limited by `REQUEST_BODY_LIMIT_BYTES` (1 MiB by default).
- Database pool bounds and acquisition timeout are configured through `DB_POOL_*` variables.
- Baseline content-type, framing, and referrer security headers are attached to responses.
- `/health` reports database connectivity without exposing connection details.
- SIGINT/SIGTERM trigger graceful Axum shutdown.
- Startup applies only tracked schema migrations. It never starts a public-transport import.
- Production logs are JSON; local logs remain human-readable.

The repository security and data rules remain authoritative: Argon2id password hashes, hashed
refresh tokens, JWT access tokens, preserved source identifiers, import validation/data-quality
reporting, and no real payment or ticket-purchase behavior without explicit approval.
