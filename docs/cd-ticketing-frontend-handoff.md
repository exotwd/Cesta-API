# ČD ticketing frontend handoff

All routes require `Authorization: Bearer <access-token>`. Commercial mutations also require a stable `Idempotency-Key` (8–128 ASCII letters, digits, `-_.:`). IDs returned by these routes are opaque UUIDs scoped to the authenticated user. Monetary fields are integer hellers and currency is always `CZK`.

## Discovery

- `GET /ticketing/locations?q=Praha&type=3&limit=10` → `{"locations":[{"id":"uuid","name":"Praha hl.n.","type":3,"typeName":"stanice","state":"Česká republika","region":"..."}]}`. The upstream location key is not returned.
- `GET /ticketing/reference/passengers` → `{"passengers": ...}` preserving the ČD passenger/reduction catalogue.
- `POST /ticketing/searches` accepts:

```json
{
  "fromLocationId": "uuid",
  "toLocationId": "uuid",
  "viaLocationId": null,
  "changeLocationId": null,
  "dateTime": "2026-07-06 08:00",
  "previous": false,
  "maxCount": 5,
  "maxChanges": 4,
  "class": 2,
  "passengerGroups": [{"passengerTypeId": 5, "count": 1, "age": null, "reductionIds": []}]
}
```

It returns `searchId`, `allowPrevious`, `allowNext`, and normalized `connections`. Each connection has an opaque `id`; the remaining train, route, notice, fixed-code, station, duration, availability, and indicative-price fields mirror the named ČD fields. Raw handles and connection IDs are removed.

- `GET /ticketing/searches/{searchId}?cursor={connectionId}` pages the owned search.
- `GET /ticketing/searches/{searchId}/connections/{connectionId}` returns full details with the same identifier sanitization.

## Orders and add-ons

- `POST /ticketing/orders`: `{"searchId":"uuid","connectionId":"uuid","class":2,"passengerGroups":[...]}`.
- `GET /ticketing/orders/{orderId}` returns `id`, `searchId`, `connectionId`, `status`, `selectedOfferType`, `amountHellers`, `currency`, `quote`, and `version`.
- `PATCH /ticketing/orders/{orderId}/offer`: `{"offerType":1}`.
- `POST /ticketing/orders/{orderId}/quote-refresh` refreshes an editable quote without changing its opaque order ID.
- `DELETE /ticketing/orders/{orderId}` releases a draft.
- `GET /ticketing/orders/{orderId}/add-ons` returns combined ČD reservation/bicycle/dog state.
- `PATCH /ticketing/orders/{orderId}/reservations`: `{"trains":[<allowlisted ČD reservation selection objects>]}`.
- `PATCH /ticketing/orders/{orderId}/bicycles`: `{"count":1,"trains":[...],"priceOnly":false}`.
- `PATCH /ticketing/orders/{orderId}/dogs`: `{"count":1,"direction":2,"priceOnly":false}`; direction is `1`, `2`, or `4` per ČD.
- `GET /ticketing/orders/{orderId}/coach-schema?trainId=0&coach=12&width=800&maxHeight=1200` returns coaches, PNG data, seat rectangles/status/class/features, direction, and legend.

## Checkout, documents, and refunds

- `POST /ticketing/orders/{orderId}/checkout-session`: `{"customer":{"email":"person@example.cz","name":"Name","inCardNumber":null,"birthDate":null,"companyName":null}}` → `{"order":...,"checkoutSession":{"id":"...","redirectUrl":"https://...","expiresAt":null}}`.
- `POST /ticketing/orders/{orderId}/complete` has no body. The backend reads and verifies the provider session, amount, currency, and settled state before fixing and issuing the ČD offer.
- `GET /ticketing/orders/{orderId}/documents` returns opaque document and ticket IDs plus sanitized ticket tariff/reservation data.
- `GET /ticketing/documents/{documentId}` streams `application/pdf` or `image/png` with an attachment disposition and `Cache-Control: private, no-store`.
- `GET /ticketing/tickets/{ticketId}/refund-quote?email=...` returns `price2Refund` and errors.
- `POST /ticketing/tickets/{ticketId}/refunds`: `{"email":"person@example.cz"}` → `202` with `id`, `ticketId`, `status`, `amountHellers`, and details.
- `GET /ticketing/tickets/{ticketId}/refunds/latest` returns the local latest refund and current sanitized ČD status.

Stable errors use `{"code":"...","message":"..."}`. Relevant codes include `unauthorized`, `not_found`, `validation_error`, `rate_limited`, `idempotency_key_required`, `idempotency_conflict`, `operation_in_progress`, `invalid_order_state`, `upstream_timeout`, `upstream_unavailable`, `upstream_resource_expired`, `upstream_rejected`, `payment_not_settled`, and `payment_verification_failed`.

## Payment-provider adapter assumption

The repository had no payment provider. The optional adapter is intentionally narrow and configurable:

- `POST {PAYMENT_PROVIDER_BASE_URL}/checkout-sessions` with bearer authentication and `{merchantReference, amountHellers, currency}`.
- `GET {PAYMENT_PROVIDER_BASE_URL}/checkout-sessions/{id}` with bearer authentication.
- Both return `{id, status, amountHellers, currency, redirectUrl?, expiresAt?}`; completion accepts only exact amount/currency and `status: "settled"`.

Without both payment-provider variables, checkout returns `payment_provider_unavailable`; it never falls back to a client success assertion.
