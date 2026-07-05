# ČD Ticket API 1.0.0 capability matrix

The internal client implements all 31 operations in the authoritative specification: all location, connection, ticket, reservation, bicycle, dog, combined-service, schema, payment, document, refund, passenger-constant, and location-constant calls. Signed payment, refund, and OneTicket release calls use RSA-SHA1. Raw responses are stored internally so additive upstream fields are retained.

The authenticated app API implements the complete route inventory in `cd-ticketing-frontend-handoff.md`. Raw `handle`, `connId`, `bookingId`, `documentId`, `ticketId`, partner credentials, and signatures are never app authorization tokens or response fields.

Backend-only capabilities have no direct mobile route by design:

- `connections/set`, location detail/constants, sold-ticket lookup, fixed-offer info, refund reconciliation, and OneTicket reservation release remain typed internal operations.
- OneTicket reservation release is not automatically invoked because ČD documents it as CENDIS-only and the repository has no product policy indicating that Cesta is an authorized CENDIS partner.
- Checkout is disabled until the configurable backend payment verifier is configured. The repository contained no existing payment provider; the isolated adapter contract is documented in the handoff.

Upstream ambiguities are isolated rather than guessed: passenger/reduction and add-on selection payloads preserve the documented ČD named fields, unknown response fields are retained, and operational product policy (including CENDIS eligibility and partner market) remains configuration/application policy.
