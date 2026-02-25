# CSV-Driven Scrapy MVP

Single-spider Scrapy project driven by a CSV configuration. Each row in the CSV defines:
1. Generator: how to build start requests
2. Evaluator: how to enqueue follow-up requests (pagination, etc.)
3. Parser: how to extract items and map them into the standard data model

## Quickstart
1. Update the pipeline file (default: `foodbankscrapy/static/pipelines/test.json`).
2. Run the spider:
```bash
python3 -m foodbankscrapy.main
```

Run by state:
```bash
python3 -m foodbankscrapy.main --state TX
```

Run by organization id:
```bash
python3 -m foodbankscrapy.main --org-id 37
```

Smoke test capture:
- Use `--test-record` to save fixtures.
- Each response will be saved to `static/tests/<domain>/input.data`.
- Parsed output will be saved to `static/tests/<domain>/output.jsonl`.
- A test config will be written to `static/pipelines/test_config.json`.

Run smoke tests (prod pipeline + local fixtures):
```bash
python3 -m foodbankscrapy.main --test-smoke
```

## Output
Each run writes a single JSONL file to:
- `foodbankscrapy/output/<run_timestamp>.jsonl`

The run timestamp is printed at startup.

## CSV Columns
Required:
- `MailAddress_State`
- `OrganizationID`
- `FullName`
- `Spider` (source type label)
- `Source` (URL if applicable)

Optional configuration (JSON format):
- `generator`: JSON object with `name` and kwargs; `source` lives here. Example: `{"name":"url","source":"https://example.com"}`
- `evaluator`: JSON object with `name` and kwargs, e.g. `{"name":"none"}`
- `parser`: JSON list of objects, e.g. `[{"name":"html","css":"script"}, {"name":"regex","regex":"..."}, {"name":"json"}]`

Defaults:
- `AccessFood`: `Generator=accessfood`, `Evaluator=accessfood_pagination`, `Parser Kwargs=["item1"]`
- All others: `Generator=url`, `Evaluator=none`

AccessFood helper:
- If `Generator Kwargs` includes `"use_row_latlng": true`, the spider will use
  `MailAddress_Latitude` and `MailAddress_Longitude` from the CSV row as the
  `lat`/`lng` query params.

HTML parser:
- Use `Parser=html` and `Parser Kwargs` as a JSON object.
- Supported keys: `id` (script tag id), `css`, `xpath`, `attr` (attribute name), `json_path` (JSON list path),
  `regex` (pattern), `regex_flags` (`i`, `m`, `s`, `x`), `regex_group` (int or group name).
  Example: `{"id": "storelocator-script-js-before", "regex": "storeLocatorData\\s*=\\s*(\\{.*?\\});", "regex_flags": "s"}`

FoodFinder encrypted parser chain step:
- Use chain step `{"name":"foodfinder_decrypt","timestamp_param":"_time"}` followed by `{"name":"json"}`.
- It decrypts the raw response bytes using CryptoJS-compatible AES passphrase mode where passphrase is `md5(_time)`,
  then inflates the payload.

Evaluators:
- `zip_iterator`
  - Enqueues one request per zip code by updating a query param (default `zip`).
  - Resolution order:
    1. `zipcodes` / `zip_codes` list in evaluator kwargs.
    2. `zipcodes_file` (default `static/zipcodes.csv`) filtered by `state` and optional `city`.
    3. If `state` + `city` are provided and no file match, fallback to `api.zippopotam.us`.
  - Useful kwargs: `zip_param`, `state`, `city`, `zipcodes_file`, `max_zipcodes`, `params`.
  - Example:
    `{"name":"zip_iterator","zip_param":"postal","state":"CA","city":"Indio","zipcodes_file":"static/zipcodes.csv"}`

- `box_search`
  - Enqueues lat/lng/radius searches on a gap-free grid around a starting point.
  - Required-ish kwargs: `start_lat`, `start_lng`, `query_radius_miles` (or `radius_miles`).
  - Coverage kwargs: `coverage_radius_miles` (defaults to `query_radius_miles`), `overlap_ratio` (default `0.10`).
  - Param names are configurable with `lat_param`, `lng_param`, `radius_param`.
  - Example:
    `{"name":"box_search","start_lat":34.0522,"start_lng":-118.2437,"query_radius_miles":10,"coverage_radius_miles":60,"lat_param":"latitude","lng_param":"longitude","radius_param":"distance"}`

- `foodfinder_bbox_split`
  - For FoodFinder encrypted bbox endpoints with a hard cap (default `300` rows per query).
  - Decrypts response in evaluator, and when result length equals `cap_size`, splits the request bbox into two child bboxes and re-queues.
  - Repeats recursively until each query returns `< cap_size`, or safety limits are hit.
  - Useful kwargs: `cap_size`, `max_split_depth`, `min_lat_span`, `min_lon_span`, `refresh_time_per_request`, `time_param`.

  - `bbox_search`
  - Enqueues min/max-lat/lon bounding-box requests over a rectangular grid.
  - Useful for APIs that require `min_lat`, `max_lat`, `min_lon`, `max_lon` instead of center/radius.
  - Key kwargs: `start_lat`, `start_lng`, `coverage_width_miles`, `coverage_height_miles`,
    `bbox_width_miles`, `bbox_height_miles`, `bbox_overlap_miles`.
  - Param names are configurable with `min_lat_param`, `max_lat_param`, `min_lon_param`, `max_lon_param`.
  - State mode:
    - Set `states` (list or string) to generate boxes from built-in US/PR state bounds.
    - If `states` is not provided, generator falls back to `start_lat`/`start_lng` coverage mode.
    - Optional `state_param` appends state code to query params.
  - Timestamp refresh:
    - Set `refresh_time_per_request: true` to inject a fresh millisecond timestamp on every request.
    - `time_param` controls the key name (defaults to `_time`).
  - Example:
    `{"name":"bbox_search","source":"https://api.example.com/search","start_lat":39.8,"start_lng":-98.5,"coverage_width_miles":3000,"coverage_height_miles":1800,"bbox_width_miles":250,"bbox_height_miles":250,"bbox_overlap_miles":25,"params":{"portal":"0","_time":"1771709485027"}}`

## Project Structure
- `foodbankscrapy/scrapy.cfg` — Scrapy config.
- `foodbankscrapy/foodbankscrapy/main.py` — CLI runner.
- `foodbankscrapy/foodbankscrapy/spiders/pipeline_spider.py` — single spider that executes CSV rows.
- `foodbankscrapy/foodbankscrapy/schemas.py` — standard Pydantic data model.
- `foodbankscrapy/foodbankscrapy/pipelines.py` — writes normalized JSONL output.
- `foodbankscrapy/foodbankscrapy/utils/` — CSV loader, parser helpers, normalizers.
- `foodbankscrapy/static/pipelines/test.json` — JSON input (default).
- `foodbankscrapy/static/pipelines/prod.json` — JSON input (prod).
- `foodbankscrapy/static/pipeline.csv` — CSV input (legacy).
- `foodbankscrapy/output/` — run outputs.
