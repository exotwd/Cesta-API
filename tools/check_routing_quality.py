#!/usr/bin/env python3
"""Read-only public API regression for the documented PID timetable.

Only public search requests are sent. No accounts, purchases or imports are used.
"""

import argparse
import datetime
import json
import time
import urllib.request
from zoneinfo import ZoneInfo


def direct_six(journey):
    legs = journey["legs"]
    return (
        len(legs) == 1
        and legs[0]["mode"] == "tram"
        and legs[0]["line"] == "6"
        and journey["transfer_count"] == 0
        and journey["walking_distance_meters"] == 0
    )


def signature(journey):
    return tuple(
        (
            leg["mode"], leg["line"], leg["from_stop_id"], leg["to_stop_id"],
            leg["departure_time"], leg["arrival_time"],
        )
        for leg in journey["legs"]
    )


def search(base_url, date, origin, destination, hour, modes, transfers):
    body = {
        "from": {"type": "stop", "id": origin},
        "to": {"type": "stop", "id": destination},
        "datetime": datetime.datetime.fromisoformat(f"{date}T{hour:02}:00:00")
        .replace(tzinfo=ZoneInfo("Europe/Prague")).isoformat(),
        "mode": "depart_at",
        "transport_modes": modes,
        "max_transfers": transfers,
        "walking_speed": "normal",
        "prefer_reliable_transfers": True,
        "offline_compatible": False,
    }
    request = urllib.request.Request(
        f"{base_url}/journeys/search",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    started = time.monotonic()
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.load(response)
    elapsed = time.monotonic() - started
    journeys = payload["journeys"]
    assert 0 < len(journeys) <= 20, "missing journeys or response limit exceeded"
    seen = set()
    for journey in journeys:
        assert journey["departure_time"] >= hour * 3600, "already-departed journey"
        assert journey["transfer_count"] <= transfers, "transfer limit exceeded"
        assert journey["arrival_time"] >= journey["departure_time"], "negative duration"
        identity = signature(journey)
        assert identity not in seen, "duplicate visible itinerary"
        seen.add(identity)
        for leg in journey["legs"]:
            if leg["line"] is not None:
                assert leg["mode"] in modes, "disallowed transit mode"
                assert leg["run_id"], "missing dated transit run identity"
            geometry = leg["geometry"]
            assert geometry and geometry["type"] == "LineString", "missing real geometry"
            coordinates = geometry["coordinates"]
            assert len(coordinates) >= 2, "incomplete geometry"
            assert all(
                len(point) >= 2 and -180 <= point[0] <= 180 and -90 <= point[1] <= 90
                for point in coordinates
            ), "invalid WGS84 geometry"
    fastest = min(journey["arrival_time"] for journey in journeys)
    assert any(
        journey["arrival_time"] == fastest and "nejrychlejsi" in journey["labels"]
        for journey in journeys
    ), "incorrect fastest label"
    direct = [(index + 1, journey) for index, journey in enumerate(journeys) if direct_six(journey)]
    assert direct, "direct tram 6 is missing"
    return journeys, direct, elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="https://api.pojedes.cz")
    parser.add_argument("--date", default="2026-10-05", help="reference service day in Europe/Prague")
    args = parser.parse_args()
    endpoints = ("pid_gtfs:U237S1", "pid_gtfs:U717Z1P")
    all_modes = ["metro", "tram", "bus", "train", "trolleybus", "ferry"]
    observations = []
    for origin, destination in (endpoints, endpoints[::-1]):
        for hour in (8, 10, 12, 14):
            for modes in (all_modes, ["tram"]):
                control = None
                for transfers in (0, 4):
                    journeys, direct, elapsed = search(
                        args.base_url.rstrip("/"), args.date, origin, destination, hour, modes, transfers,
                    )
                    first_direct = min(direct, key=lambda item: item[1]["departure_time"])
                    if transfers == 0:
                        control = signature(first_direct[1])
                    else:
                        assert any(signature(journey) == control for _, journey in direct), (
                            f"earliest direct tram disappeared with transfers enabled: {origin} {hour}:00"
                        )
                    record = {
                        "from": origin, "to": destination, "hour": hour,
                        "modes": "all" if modes == all_modes else "tram",
                        "max_transfers": transfers, "count": len(journeys),
                        "direct6_rank": first_direct[0],
                        "direct6_departure": first_direct[1]["departure_time"],
                        "direct6_arrival": first_direct[1]["arrival_time"],
                        "direct6_duration": first_direct[1]["duration_seconds"],
                        "fastest_arrival": min(journey["arrival_time"] for journey in journeys),
                        "http_seconds": round(elapsed, 3),
                    }
                    observations.append(record)
                    print(json.dumps(record, ensure_ascii=False), flush=True)
    latencies = sorted(record["http_seconds"] for record in observations)
    print(json.dumps({
        "passed": len(observations), "failed": 0,
        "median_http_seconds": latencies[len(latencies) // 2],
        "max_http_seconds": max(latencies),
    }), flush=True)


if __name__ == "__main__":
    main()
