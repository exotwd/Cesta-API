# Backend API

The API exposes:

- `GET /health`
- auth endpoints under `/auth`
- user data endpoints under `/me`
- stops under `/stops`
- departures under `/departures`
- journey search at `POST /journeys/search`
- realtime status under `/realtime`
- offline package metadata under `/offline`
- ticket recommendation placeholders under `/tickets`
- admin imports and data quality under `/admin`
- public board data under `/public/boards`

Every schedule/realtime response should include data-status metadata and warnings where data is mock, stale, unavailable or partial.

