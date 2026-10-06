#!/usr/bin/env python3
"""Bounded, read-only API benchmark. Reports first probes separately from repeated requests.

Reports contain fixture labels and measurements, never account or client identities.
"""
import argparse
import concurrent.futures
import datetime
import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo


def request(base, path, payload=None, headers=None):
    req = urllib.request.Request(
        base.rstrip('/') + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={'Content-Type': 'application/json', **(headers or {})},
    )
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            body = response.read()
            elapsed = (time.perf_counter() - start) * 1000
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                data = {}
            return {'status': response.status, 'elapsed_ms': round(elapsed, 2),
                    'bytes': len(body), 'results': len(data.get('journeys', [])),
                    'warnings': bool(data.get('warnings')), 'etag': response.headers.get('ETag')}, data
    except urllib.error.HTTPError as error:
        return {'status': error.code, 'elapsed_ms': round((time.perf_counter()-start)*1000, 2),
                'bytes': 0, 'error': 'http_error'}, {}
    except (urllib.error.URLError, TimeoutError):
        return {'status': 0, 'elapsed_ms': round((time.perf_counter()-start)*1000, 2),
                'bytes': 0, 'error': 'transport_error'}, {}


def summary(samples):
    values = sorted(sample['elapsed_ms'] for sample in samples)
    def percentile(p):
        return values[max(0, math.ceil(len(values)*p)-1)] if values else None
    return {'samples': len(samples), 'failures': sum(s['status'] not in (200, 304) for s in samples),
            'p50_ms': percentile(.5), 'p95_ms': percentile(.95), 'max_ms': max(values, default=None),
            'mean_bytes': round(sum(s['bytes'] for s in samples)/len(samples)) if samples else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:8070')
    parser.add_argument('--date', default=(datetime.datetime.now(ZoneInfo('Europe/Prague')).date()+datetime.timedelta(days=1)).isoformat())
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--concurrency', type=int, default=2)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 20 or not 1 <= args.concurrency <= 8:
        parser.error('repetitions must be 1–20 and concurrency 1–8')
    fixtures = []
    for label, origin, destination, feed in [
        ('prague_tram', 'Karlovo náměstí', 'Strossmayerovo náměstí', 'pid_gtfs:'),
        ('prague_metro', 'Dejvická', 'Hradčanská', 'pid_gtfs:'),
        ('brno_city', 'Česká', 'Mendlovo náměstí', 'ids_jmk_gtfs:'),
    ]:
        points = []
        for name in (origin, destination):
            result, data = request(args.base_url, '/stops/search?' + urllib.parse.urlencode({'q': name, 'limit': 50}))
            matches = [s for s in data.get('stops', []) if s['id'].startswith(feed)]
            if result['status'] != 200 or not matches:
                raise SystemExit(f'Fixture unavailable: {label} ({name})')
            exact = [s for s in matches if s['name'].casefold() == name.casefold()]
            points.append({'type': 'stop', 'id': (exact or matches)[0]['id']})
        fixtures.append((label, points[0], points[1], False))
    fixtures.append(('prague_coordinates', {'type': 'coordinate', 'lat': 50.0755, 'lon': 14.419}, {'type': 'coordinate', 'lat': 50.0993, 'lon': 14.433}, False))
    fixtures.append(('prague_detail', fixtures[0][1], fixtures[0][2], True))
    fixtures.append(('prague_overnight', fixtures[1][1], fixtures[1][2], False))
    probes, jobs = [], []
    for label, origin, destination, detail in fixtures:
        timestamp = datetime.datetime.fromisoformat(f'{args.date}T' + ('23:50:00' if label.endswith('overnight') else '10:00:00')).replace(tzinfo=ZoneInfo('Europe/Prague'))
        payload = {'from': origin, 'to': destination, 'datetime': timestamp.isoformat(), 'mode': 'depart_at',
                   'transport_modes': ['train', 'metro', 'tram', 'bus', 'trolleybus', 'ferry'], 'max_transfers': 4,
                   'walking_speed': 'normal', 'prefer_reliable_transfers': True, 'offline_compatible': False,
                   'include_intermediate_stops': detail}
        result, _ = request(args.base_url, '/journeys/search', payload)
        probes.append({'fixture': label, **result})
        for _ in range(args.repetitions):
            jobs.append((label, '/journeys/search', payload, {}))
    catalog, _ = request(args.base_url, '/stops/catalog')
    probes.append({'fixture': 'stop_catalog_first', **catalog})
    if catalog.get('etag'):
        jobs.extend(('stop_catalog_revalidate', '/stops/catalog', None, {'If-None-Match': catalog['etag']}) for _ in range(args.repetitions))
    def run(job):
        label, path, payload, headers = job
        result, _ = request(args.base_url, path, payload, headers)
        return {'fixture': label, **result}
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        samples = list(executor.map(run, jobs))
    groups = {label: summary([s for s in samples if s['fixture'] == label]) for label in sorted({s['fixture'] for s in samples})}
    report = {'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'service_date': args.date,
              'concurrency': args.concurrency, 'repetitions': args.repetitions,
              'notes': ['First probes may already have warm server caches.', 'A small benchmark is not a production SLO.',
                        'Empty journeys and warnings are recorded separately from HTTP failures.'],
              'first_probes': probes, 'summary': summary(samples), 'fixtures': groups, 'samples': samples}
    with open(args.output, 'w', encoding='utf-8') as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    print(json.dumps({'summary': report['summary'], 'fixtures': groups}, ensure_ascii=False))
    if any(sample['status'] not in (200,304) for sample in probes+samples):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
