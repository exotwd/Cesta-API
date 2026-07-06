# ČD Ticket API 1.0.0 capability matrix

The internal typed client implements all 31 operations in the authoritative specification: all location, connection, ticket, reservation, bicycle, dog, combined-service, coach-schema, payment, document, refund, passenger-constant, and location-constant calls. Signed payment, refund, and OneTicket release calls use RSA-SHA1. Additive upstream fields are retained internally.

The mobile API implements:

- public Cesta journey results with opaque ČD-ticketing intent references and per-segment candidate metadata;
- authenticated server-side Cesta-to-ČD connection correlation and draft quote creation;
- location suggestions, passenger/reduction reference data, standalone connection search/paging/details;
- quote refresh, offer selection, reservations, bicycles, dogs, and coach schemas;
- verified checkout, ČD issuance, order/ticket collection, document streaming;
- refund quote, submission, status, and scheduled reconciliation with a durable cursor.

Public live fare previews and guest ticketing are intentionally omitted. The selected policy requires a Cesta account before contacting ČD for a fare. Public `/journeys/search` therefore returns `indicativePriceHellers: null` and `authenticationRequired: true`.

Backend-only operations have no direct mobile route by design:

- `connections/set`, location detail/constants, sold-ticket lookup, fixed-offer info, refund reconciliation, and OneTicket reservation release remain internal operations.
- OneTicket reservation release is not invoked automatically because ČD documents it as CENDIS-only and there is no product authorization establishing Cesta as an eligible CENDIS partner.
- Checkout remains unavailable until the backend payment adapter and trusted mobile return/cancel URLs are configured.

Raw `handle`, `connId`, `bookingId`, `documentId`, `ticketId`, partner credentials, private keys, and signatures are never client authorization tokens or response identifiers. Upstream ambiguity is isolated through stable errors and configuration; it is not guessed by the client.
