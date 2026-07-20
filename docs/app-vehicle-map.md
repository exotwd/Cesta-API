# Napojení mapy ve Flutteru

Mobilní aplikace se připojuje pouze k Cesta API. Klíč Golemio ani adresy regionálních feedů nesmí být součástí aplikace.

## Dostupné zdroje

| Provider | Stav | Interval backendu | Licence | Rozšířené vybavení vozidla |
| --- | --- | ---: | --- | --- |
| PID / Golemio | aktivní po nastavení `PID_API_TOKEN` | 20 s | CC BY; uvést PID/Golemio | bezbariérovost, klimatizace, USB, typ, rychlost, evidenční číslo, dopravce |
| IDS JMK | aktivní, bez tajného klíče | 30 s | CC BY 4.0; uvést KORDIS JMK | standardní GTFS-RT; nedostupné položky jsou `null` |
| DÚK | výchozí stav vypnuto | 30 s po explicitním zapnutí | další poskytování nepotvrzeno | zdrojově závislé |

Backend vrací jen poslední úspěšně uložený stav. Pohyb mapy tedy nikdy nespouští volání Golemio nebo KORDIS.

## Vozidla ve viditelné mapě

```http
GET /vehicles?bbox=14.30,49.95,14.70,50.20&limit=2000
```

Volitelné filtry jsou `provider=pid`, `provider=ids_jmk` a kompatibilní technický filtr `source`. Starší cesta `GET /realtime/vehicles` vrací stejný kontrakt.

```json
{
  "vehicles": [
    {
      "id": "pid:registration:8826",
      "provider": "pid",
      "source": {
        "feedId": "pid_realtime",
        "license": "CC-BY",
        "attribution": "Pražská integrovaná doprava / Golemio",
        "redistributionAllowed": true
      },
      "vehicleId": "registration:8826",
      "registrationNumber": "8826",
      "latitude": 50.0755,
      "longitude": 14.4378,
      "heading": 125.0,
      "speedKmh": 42.0,
      "route": {
        "id": "pid_gtfs:L119",
        "shortName": "119",
        "tripId": "pid_gtfs:119_1_260720",
        "destination": "Letiště",
        "nextStopId": "pid_gtfs:U1Z1P"
      },
      "vehicleType": "bus",
      "accessibility": {"wheelchairAccessible": true},
      "amenities": {"airConditioned": true, "usbChargers": false},
      "occupancyStatus": null,
      "operatorName": "DPP",
      "tracking": true,
      "state": "on_track",
      "delaySeconds": 30,
      "updatedAt": "2026-07-20T12:34:56Z",
      "validUntil": "2026-07-20T12:36:26Z",
      "confidence": "estimated"
    }
  ]
}
```

`null` znamená „poskytovatel údaj neposkytl“, nikoliv „vozidlo vlastnost nemá“. V UI proto nezobrazuj přeškrtnutou klimatizaci nebo bezbariérovost, pokud je hodnota `null`.

## Zastávky ve viditelné mapě

```http
GET /stops/in-bounds?south=49.95&west=14.30&north=50.20&east=14.70&limit=1000
```

Při `nextCursor != null` opakuj stejné hranice s `cursor=<nextCursor>`. Každá zastávka obsahuje:

- `marker_type`: přímo použitelný typ ikony, například `bus_stop`, `bus_station`, `tram_stop`, `metro_station`, `railway_station`, `railway_stop`, `ferry_terminal`, `airport` nebo `station_entrance`;
- `modes`: všechny druhy dopravy, které zastávku skutečně obsluhují;
- `location_type`: GTFS strukturu `stop`, `station`, `entrance_exit`, `generic_node` nebo `boarding_area`;
- `wheelchair_boarding`: `accessible`, `inaccessible` nebo `unknown`;
- `map_visible`: pro `generic_node` a `boarding_area` je `false`, takže je v běžném mapovém zoomu nezobrazuj.

Doporučené barvy ikon: autobus modrá, tramvaj červená, metro tmavě modrá, vlak fialová, trolejbus tyrkysová, přívoz světle modrá a vstup do stanice šedá. U multimodální zastávky použij `marker_type` jako hlavní ikonu a `modes` v detailu.

## Doporučený Flutter tok

1. Po dokončení pohybu mapy (`onCameraIdle`) spočítej bounds; neposílej request v `onCameraMove`.
2. Změnu viewportu debounce alespoň 300–500 ms.
3. Vozidla obnovuj HTTP pollingem každých 15 sekund, ale jen pokud je mapa viditelná a aplikace je v popředí.
4. Zastávky načti po změně bounds; není nutné je obnovovat každých 15 sekund.
5. Vozidla aktualizuj podle stabilního `id`, ne podle pořadí v poli. Staré markery odeber až po úspěšném response.
6. Pokud request selže, ponech poslední stav a označ jej jako zastaralý podle `updatedAt`/`validUntil`.
7. V mapě nebo v informační obrazovce zobraz unikátní `source.attribution` všech právě zobrazených providerů.

Příklad sestavení URL:

```dart
Uri vehiclesUri(String apiBase, LatLngBounds bounds) {
  final bbox = [
    bounds.southwest.longitude,
    bounds.southwest.latitude,
    bounds.northeast.longitude,
    bounds.northeast.latitude,
  ].join(',');

  return Uri.parse('$apiBase/vehicles').replace(
    queryParameters: {'bbox': bbox, 'limit': '2000'},
  );
}
```

Pro první verzi není potřeba WebSocket. Polling 15 sekund je v souladu s obnovou upstream zdrojů a výrazně zjednoduší obnovu spojení, lifecycle aplikace i diagnostiku. WebSocket lze později přidat nad stejný model bez změny mapových objektů.

## Konfigurace backendu

1. Vytvoř bezplatný Golemio API klíč na `https://api.golemio.cz/api-keys`.
2. Nastav jej pouze na serveru jako `PID_API_TOKEN`.
3. Ponech `IDS_JMK_VEHICLES_URL=https://kordis-jmk.cz/gtfs/gtfsReal.dat`.
4. Ponech `DUK_ENABLED=false`, dokud nebude potvrzeno další poskytování dat.
5. Spusť `docker compose up --build api realtime-worker schedule-updater`.
6. Ověř `GET /data-sources/status`, `GET /vehicles?bbox=...` a `GET /openapi.json`.
