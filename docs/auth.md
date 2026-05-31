# Auth

Account model:

- users
- user profiles
- sessions with hashed refresh tokens
- saved places
- favorite stops and routes
- notification preferences
- user roles

Security requirements:

- Argon2id password hashing
- short-lived JWT access tokens
- hashed refresh tokens
- no token/password logging
- soft-delete accounts
- admin/data_admin roles for protected operations

