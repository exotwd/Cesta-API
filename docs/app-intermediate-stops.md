# App integration: journey stop calls

Journey search can return every scheduled stop between boarding and alighting for each leg. Request this only when opening route detail because it increases the response size.

## Request

Send `include_intermediate_stops: true` to the existing endpoint:

```http
POST /journeys/search
Content-Type: application/json
```

```json
{
  "from": {"type": "stop", "id": "pid_gtfs:U1Z1P"},
  "to": {"type": "stop", "id": "pid_gtfs:U237Z2P"},
  "datetime": "2026-07-04T14:00:00+02:00",
  "mode": "depart_at",
  "transport_modes": ["metro", "tram", "bus", "train"],
  "max_transfers": 4,
  "walking_speed": "normal",
  "prefer_reliable_transfers": true,
  "offline_compatible": false,
  "include_intermediate_stops": true
}
```

The camelCase alias `includeIntermediateStops` is accepted. Prefer the snake_case field in new code. When the field is false or omitted, `stop_calls` is omitted to preserve the compact response.

## Response

Every item in `journeys[].legs[]` receives:

```json
{
  "intermediate_stop_count": 2,
  "stop_calls": [
    {
      "trip_id": "pid_gtfs:trip-id",
      "stop_id": "pid_gtfs:U1Z1P",
      "stop_sequence": 4,
      "name": "Muzeum",
      "municipality": "Praha",
      "lat": 50.0796,
      "lon": 14.4304,
      "platform": "A",
      "scheduled_arrival_seconds": 50400,
      "scheduled_departure_seconds": 50460,
      "scheduled_arrival": "14:00:00",
      "scheduled_departure": "14:01:00",
      "pickup_type": 0,
      "drop_off_type": 0,
      "timepoint": true,
      "is_origin": true,
      "is_destination": false,
      "is_intermediate": false,
      "realtime": {
        "status": "realtime",
        "delay_seconds": 90,
        "estimated_arrival": "2026-07-04T12:01:30Z",
        "estimated_departure": "2026-07-04T12:02:30Z",
        "cancellation_status": null,
        "platform_change": null,
        "source": "pid_gtfs_rt",
        "fetched_at": "2026-07-04T12:00:20Z",
        "valid_until": "2026-07-04T12:01:50Z",
        "confidence": "estimated"
      }
    }
  ]
}
```

`stop_calls` is ordered by `stop_sequence` and includes the leg origin and destination. Only entries with `is_intermediate=true` are intermediate stops. A transfer appears as the destination of one leg and the origin of the next; keep the legs separate so their platforms and times are not lost.

## Flutter model

Add optional fields to the existing leg model:

```dart
class JourneyLeg {
  final List<JourneyStopCall>? stopCalls;
  final int? intermediateStopCount;

  JourneyLeg.fromJson(Map<String, dynamic> json)
      : stopCalls = (json['stop_calls'] as List?)
            ?.map((item) => JourneyStopCall.fromJson(item))
            .toList(),
        intermediateStopCount = json['intermediate_stop_count'] as int?;
}

class JourneyStopCall {
  final String stopId;
  final String name;
  final int? stopSequence;
  final String scheduledArrival;
  final String scheduledDeparture;
  final bool isOrigin;
  final bool isDestination;
  final bool isIntermediate;
  final Map<String, dynamic> realtime;

  JourneyStopCall.fromJson(Map<String, dynamic> json)
      : stopId = json['stop_id'],
        name = json['name'],
        stopSequence = json['stop_sequence'],
        scheduledArrival = json['scheduled_arrival'],
        scheduledDeparture = json['scheduled_departure'],
        isOrigin = json['is_origin'] ?? false,
        isDestination = json['is_destination'] ?? false,
        isIntermediate = json['is_intermediate'] ?? false,
        realtime = json['realtime'] ?? const {'status': 'scheduled'};
}
```

## Display rules

1. Initially search without intermediate stops and show the returned journey cards.
2. When route detail opens, repeat the same search with `include_intermediate_stops=true`. Match the selected journey by its ordered leg `trip_id`, departure time and arrival time, not by `journey-N`, because result IDs are response-local.
3. Render `stop_calls` in their returned order. Do not sort names or formatted time strings.
4. Use `realtime.estimated_arrival` or `estimated_departure` when present; otherwise use the scheduled value.
5. Show delay from `realtime.delay_seconds`. Negative values mean early running.
6. Display cancelled calls distinctly when `realtime.status == "cancelled"`.
7. Treat missing `stop_calls` as “details not requested”, and an empty list as unavailable source detail.
8. Keep scheduled second values as service-day seconds. They may exceed 86400 for trips after midnight.

The API removes source-level duplicate connections and invalid transfer candidates before ranking. The app should not independently merge journeys with different times or transfer patterns.

The requested `datetime` date is checked against GTFS `calendar.txt` and `calendar_dates.txt`. A latest successful import with no calendar data remains searchable as an unverified legacy fallback. Always display top-level API `warnings`.
