# ČD Ticket API integration boundary

Source contract: `https://ticket-api.cd.cz/swaggerUI/cdapi-1.0.0.yml`

Reviewed against ČD API version `1.0.0` on 2026-07-05.

## Decision

The Flutter application must not call `https://ticket-api.cd.cz/v1` directly. Every documented operation requires the partner `X-User` access key. Sale, refund, and OneTicket reservation-release operations additionally require an RSA-SHA1 signature in `X-Hash`. A distributed mobile binary cannot keep either credential private.

The backend configured by `CESTA_API_BASE_URL` owns the ČD integration. It exposes a user-scoped API containing only data and actions the signed-in user may access. The app never receives `X-User`, the RSA private key, `X-Hash`, or unrestricted ČD booking handles.

## Required backend responsibilities

1. Store the ČD partner key and signing key in a secret manager.
2. Authenticate the app user and authorize every order, document, and refund action.
3. Keep a server-side mapping from opaque app IDs to ČD `handle`, `connId`, `bookingId`, `documentId`, and `ticketId` values.
4. Generate signatures server-side for payment, refund, and OneTicket calls.
5. Make mutations idempotent and serialize operations per order.
6. Allowlist request bodies; never expose a generic ČD proxy.
7. Rate-limit location and connection searches and cache reference data.
8. Avoid logging personal data, partner keys, hashes, documents, or unredacted ČD errors.
9. Download documents server-side and stream them only to their authenticated owner.
10. Persist payment/refund audit events and reconcile asynchronous refund changes.

## Complete upstream endpoint inventory

| Area | ČD operation | Backend use | Mobile exposure |
| --- | --- | --- | --- |
| Locations | `GET /locations` | Search all destination types | Normalized suggestions |
| Locations | `GET /locations/{type}` | Search a destination type | Suggestion type filter |
| Locations | `GET /locations/{type}/{key}` | Validate a stored location | Normalized detail |
| Connections | `POST /connections/set` | Resolve a partner description | Backend-only |
| Connections | `POST /connections/search` | Start search | Opaque search and summaries |
| Connections | `POST /connections/{handle}` | Page result set | Opaque cursor |
| Connections | `POST /connections/{handle}/{connId}` | Connection/offer detail | User-facing detail |
| Tickets | `POST /tickets` | Create offers and `bookingId` | Create/update app order |
| Tickets | `GET /tickets/{bookingId}` | Recalculate offers | Refresh quote |
| Tickets | `PUT /tickets/{bookingId}` | Select offer type | Select app offer |
| Tickets | `DELETE /tickets/{bookingId}` | Release resources | Cancel draft |
| Tickets | `GET /tickets/{bookingId}/info` | Fixed-offer detail | Order review |
| Tickets | `PUT /tickets/{bookingId}/book` | Fix before payment | Checkout orchestration |
| Add-ons | `GET /reservations/{bookingId}` | Reservation choices | Owned draft choices |
| Add-ons | `PUT /reservations/{bookingId}` | Set reservation | Owned draft mutation |
| Add-ons | `POST /bikeprice/{bookingId}` | Bicycle price | Bicycle options/prices |
| Add-ons | `PUT /bikes/{bookingId}` | Set bicycles | Owned draft mutation |
| Add-ons | `GET /dogsprice/{bookingId}` | Dog price | Dog price |
| Add-ons | `PUT /dogs/{bookingId}` | Set dogs | Owned draft mutation |
| Add-ons | `GET /addservices/{bookingId}` | Combined state | Normalized summary |
| Schemas | `POST /schemas` | Coach image/seats/legend | Sanitized owned schema |
| Payments | `PUT /payments/{bookingId}` | Signed issuance | Checkout backend only |
| Payments | `GET /payments/{bookingId}` | Sold metadata | Owned tickets/documents |
| Documents | `GET /documents/{documentId}` | PDF/PNG bytes | Authenticated stream |
| Refunds | `GET /refundpossible/{ticketId}` | Eligibility | Owned ticket quote |
| Refunds | `POST /refunds/{ticketId}` | Signed refund | Confirmed owned mutation |
| Refunds | `GET /refunds/{ticketId}` | Refund status | Owned ticket status |
| Refunds | `GET /refunds` | Reconcile changes | Scheduled backend-only |
| Refunds | `POST /releaseres/{sjtTicketId}` | CENDIS-only release | Backend policy only |
| Constants | `GET /consts/passengers` | Passenger/reductions | Cached catalogue |
| Constants | `GET /consts/locations/{type}` | Fixed destinations | Cache/search input |

## App-facing API

```text
GET    /ticketing/locations?q=&type=&limit=
GET    /ticketing/reference/passengers
POST   /ticketing/searches
GET    /ticketing/searches/{searchId}?cursor=
GET    /ticketing/searches/{searchId}/connections/{connectionId}
POST   /ticketing/orders
GET    /ticketing/orders/{orderId}
PATCH  /ticketing/orders/{orderId}/offer
DELETE /ticketing/orders/{orderId}

GET   /ticketing/orders/{orderId}/add-ons
PATCH /ticketing/orders/{orderId}/reservations
PATCH /ticketing/orders/{orderId}/bicycles
PATCH /ticketing/orders/{orderId}/dogs
GET   /ticketing/orders/{orderId}/coach-schema?trainId=&coach=

POST /ticketing/orders/{orderId}/checkout-session
POST /ticketing/orders/{orderId}/complete
GET  /ticketing/orders/{orderId}/documents
GET  /ticketing/documents/{appDocumentId}
GET  /ticketing/tickets/{appTicketId}/refund-quote
POST /ticketing/tickets/{appTicketId}/refunds
GET  /ticketing/tickets/{appTicketId}/refunds/latest
```

`searchId`, `cursor`, `connectionId`, `orderId`, app document IDs, and app ticket IDs are opaque and user-scoped. `complete` verifies the backend-owned payment result before fixing the offer and making the signed ČD payment call.

## Data preservation

The backend preserves location identity and labels; connection, train, station, notice, fixed-code, route, availability, and duration data; booking flags, class, passengers, ages, and reductions; prices in integer hellers, offer flags, tariffs, conditions, and validity; reservations, places, coaches, compartment types, notices, and allocation errors; bicycle/dog state and prices; coach images, geometry, seats, direction, and legend; sold document/ticket metadata, tariffs, validity, reservations, print/return state; and refund amounts, errors, rejection state, events, replacement tickets, and replacement documents.

Upstream exceptions are mapped to stable app errors while sanitized diagnostics remain internal. A generic proxy, direct mobile credential use, raw document authorization, client-asserted payment success, and direct refund/OneTicket actions are forbidden.
