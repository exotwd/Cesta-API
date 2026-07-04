# Czech municipality registry

`cz-municipalities.csv` is sourced from
https://github.com/33bcdd/souradnice-mest and contains Czech municipalities,
official municipality codes, regions, postal codes and coordinates.

The upstream README describes the data as freely usable and current to
January 1, 2018. Cesta API uses the official municipality code as the stable
part of `city:CZ:<code>` identifiers.

Import or refresh the complete registry explicitly:

```bash
docker compose --profile tools run --rm data-pipeline import-cities
```

The API startup only applies the schema and a small compatibility seed. It
does not download or run the complete city import.
