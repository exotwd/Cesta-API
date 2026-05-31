# Cesta API Agent Rules

- This repository is API-only. Do not create mobile, web, admin frontend, landing page or visual prototype projects unless explicitly requested later.
- Preserve Czech public transport source tracking for imported stops, routes, trips and stop times.
- Do not bypass import validation or data-quality reporting.
- Do not add production credentials or hardcode secrets.
- Do not implement payments or ticket purchase without explicit instruction.
- Keep mock data visibly separated from real integrations in code and API metadata.
- Do not run a full import on API startup. The API serves the last successful import or explicit development fixtures.
- Preserve account security rules: Argon2id password hashing, hashed refresh tokens, JWT access tokens and no token/password logging.
- Preserve OpenAPI documentation and update it when endpoints change.

