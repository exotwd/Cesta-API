# ČD ticketing frontend handoff

All `/ticketing` routes require `Authorization: Bearer <access-token>`. Commercial mutations also require a stable `Idempotency-Key` (8–128 ASCII letters, digits, `-_.:`). IDs are opaque UUIDs scoped to the authenticated Cesta user. Monetary values are integer hellers; currency is `CZK`.

The concrete request and response schemas for every route are in `GET /openapi.json`.

## Journey integration and price policy

`POST /journeys/search` remains public. Every returned journey now includes `ticketing`:

```json
{
  "provider": "cd",
  "journeyReference": "opaque-uuid",
  "authenticationRequired": true,
  "availability": "authentication_required",
  "indicativePriceHellers": null,
  "currency": "CZK",
  "completeJourneyTicketable": null,
  "scope": "complete_journey_candidate",
  "segments": [
    {"legIndex": 0, "provider": "cd", "availability": "authentication_required", "ticketable": null}
  ]
}
```

Unsupported legs have `provider: null` and `availability: unsupported_mode`. Mixed journeys identify candidate ČD segments explicitly. These are candidates, not a promise of sale: actual ticketability and price are resolved after login. The app must never correlate ČD connections by time, station name, or train number.

After authentication, call `POST /ticketing/intents` with an `Idempotency-Key`:

```json
{
  "journeyReference": "opaque-uuid",
  "segmentIndexes": [0],
  "class": 2,
  "passengerGroups": [
    {"passengerTypeId": 5, "count": 1, "age": null, "reductionIds": []}
  ]
}
```

Omit `segmentIndexes` to select all candidate train segments. The backend resolves stations, runs a fresh ČD search, correlates the selected Cesta journey using schedule and train evidence, obtains a quote, and returns `201` with `journeyReference`, opaque `searchId`, opaque `connectionId`, a draft `order`, the matched normalized `connection`, and `correlation`. Expired, ambiguous, or unmatched journeys return stable errors; the client must run journey search again rather than attempt its own match.

## Discovery and standalone search

- `GET /ticketing/locations?q=Praha&type=3&limit=10`
- `GET /ticketing/reference/passengers`
- `POST /ticketing/searches`
- `GET /ticketing/searches/{searchId}?cursor={connectionId}`
- `GET /ticketing/searches/{searchId}/connections/{connectionId}`

The standalone flow remains available for ticket-only searches. Location, search, and connection IDs are opaque; raw ČD keys, handles, and connection IDs are never returned.

## Orders, add-ons, and ticket collection

- `GET /ticketing/orders?status=issued&cursor={orderId}&limit=20` lists the authenticated user's orders. `limit` is 1–50. It returns `orders` and nullable `nextCursor`; each order includes available document metadata, tickets, validity/tariff details, and each ticket's `latestRefund`.
- `POST /ticketing/orders` creates a draft from an owned standalone search and connection.
- `GET /ticketing/orders/{orderId}` returns one owned order.
- `PATCH /ticketing/orders/{orderId}/offer` selects an offer.
- `POST /ticketing/orders/{orderId}/quote-refresh` refreshes a quote.
- `DELETE /ticketing/orders/{orderId}` cancels/releases an editable draft.
- `GET /ticketing/orders/{orderId}/add-ons`
- `PATCH /ticketing/orders/{orderId}/reservations`
- `PATCH /ticketing/orders/{orderId}/bicycles`
- `PATCH /ticketing/orders/{orderId}/dogs`
- `GET /ticketing/orders/{orderId}/coach-schema?trainId=0&coach=12&width=800&maxHeight=1200`

Allowed order-list statuses are `draft`, `checkout_pending`, `paid`, `issued`, `cancelled`, `issuance_failed`, `refunded`, and `refund_pending`.

## Checkout and mobile return

`POST /ticketing/orders/{orderId}/checkout-session` accepts the documented customer object and returns the order plus:

```json
{
  "id": "provider-session-id",
  "redirectUrl": "https://provider.example/checkout/...",
  "returnUrl": "jedes://ticketing/checkout/return?orderId=opaque-uuid",
  "cancelUrl": "jedes://ticketing/checkout/cancel?orderId=opaque-uuid",
  "expiresAt": null
}
```

The deployed values are configured by `MOBILE_CHECKOUT_RETURN_URL` and `MOBILE_CHECKOUT_CANCEL_URL`. The app opens `redirectUrl`, handles either deep link, extracts only the opaque Cesta `orderId`, and returns to that order. A return deep link does not prove payment. On the return path call `POST /ticketing/orders/{orderId}/complete` using the original idempotency key policy; the backend verifies provider status, amount, and currency before ČD issuance. On cancel, refresh `GET /ticketing/orders/{orderId}` and do not call complete automatically.

## Documents and refunds

- `GET /ticketing/orders/{orderId}/documents` returns owned opaque document and ticket IDs.
- `GET /ticketing/documents/{documentId}` streams owned PDF/PNG content with private, no-store headers.
- `GET /ticketing/tickets/{ticketId}/refund-quote?email=...`
- `POST /ticketing/tickets/{ticketId}/refunds`
- `GET /ticketing/tickets/{ticketId}/refunds/latest`

Never persist or infer an upstream ČD identifier in the app. Stable errors use `{"code":"...","message":"..."}`; schemas and the full error inventory are in OpenAPI.
