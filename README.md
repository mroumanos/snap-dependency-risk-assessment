# SNAP Dependency Risk Assessment

This repository contains tools to build a dataset on SNAP retailers and Emergency Food Organizations (EFOs) — food banks, pantries, etc. — and analyze them locally using Postgres/PostGIS. It includes:

- collecting source datasets (Scrapy web crawls + API/file pulls),
- loading normalized tables into Postgres/PostGIS,
- running downstream analysis in Jupyter notebooks.

## Layout

```
compose.yml                          # local infrastructure (Postgres, app, Jupyter)
queries.sql                          # ad-hoc SQL queries for exploration
notebooks/                           # post-processing and analysis notebooks
app/
  Dockerfile                         # Python runtime image build context
  requirements.txt
  code/
    main.py                          # ETL orchestrator entrypoint
    scrape.py                        # scraper-only entrypoint (test pipeline)
    etl/                             # non-Scrapy ETL jobs
      fa.py                          # Feeding America API → feeding_america_foodbanks_raw
      census.py                      # Census TIGER + CPS FSS + ERS → *_raw tables
      nhgis.py                       # NHGIS ACS extracts → acs_*_raw tables
      snap.py                        # SNAP retailer ArcGIS Hub → snap_retailers_raw
      efo.py                         # EFO JSONL loader → efos_raw
    foodbankscrapy/                  # Scrapy subsystem
      static/pipelines/              # prod.json and test.json pipeline configs
      output/raw/                    # raw crawl envelopes (per org, per run)
      output/conformed/              # conformed JSONL (per run)
    static/
      efos/                          # latest.jsonl (conformed EFO snapshot loaded by efo.py)
      nhgis/                         # pre-downloaded NHGIS CSV extracts (see Prerequisites)
      cps_fss/                       # CPS FSS supplemental data
    settings/
      db.py                          # Postgres connection settings (env-driven)
      geo.py                         # CRS + state list settings (env-driven)
    utils/
      db.py                          # shared load_into_pg / get_conn helpers
```

## Data Sources

| Source | ETL | Table(s) produced |
|---|---|---|
| Feeding America API | `etl/fa.py` | `feeding_america_foodbanks_raw` |
| Census TIGER 2024 geometries | `etl/census.py` | `census_2024_state_raw`, `census_2024_county_raw`, `census_2024_tract_raw`, `census_2024_blkgrp_raw` |
| Census CPS FSS (2019–2023) | `etl/census.py` | `census_cps_fss_raw` |
| ERS County Typology 2025 | `etl/census.py` | `ers_county_typology_raw` |
| NHGIS ACS 2023 5-year estimates | `etl/nhgis.py` | `acs_2023_county_raw`, `acs_2023_blkgrp_raw`, `acs_2023_state_raw`, `acs_2023_tract_raw` |
| SNAP retailer ArcGIS Hub | `etl/snap.py` | `snap_retailers_raw` |
| EFO JSONL snapshot (from scraper) | `etl/efo.py` | `efos_raw` |

The `notebooks/etl.ipynb` notebook then transforms these raw tables into conformed tables: `states`, `counties`, `tracts`, `blkgrps`, `efos`, `efos_clusters`, and `snap_retailers`.

## Prerequisites

- Docker + Docker Compose
- `.env` populated (copy from `.env.default` and fill in values)
- NHGIS CSV extracts placed in `app/code/static/nhgis/` (see below)

Required `.env` keys:
```
POSTGRES_DB=
POSTGRES_USER=
POSTGRES_PASSWORD=
JUPYTER_ACCESS_TOKEN=
```

Optional env keys (override geo defaults):
```
GEO_CRS=4326           # coordinate reference system (default: EPSG:4326)
GEO_STATES=AL,AK,...   # comma-separated state abbreviations (default: all 50)
```

### NHGIS data (manual download required)

The NHGIS ETL reads from pre-downloaded CSV exports. Download the following from [NHGIS](https://www.nhgis.org) and place them in `app/code/static/nhgis/`:

- `nhgis0001_ds267_20235_county.csv`
- `nhgis0004_ds267_20235_county.csv`
- `nhgis0008_ds268_20235_county.csv`
- `nhgis0001_ds267_20235_blck_grp.csv`
- `nhgis0001_ds267_20235_state.csv`
- `nhgis0001_ds267_20235_tract.csv`

## Workflow

### 1) Start Infrastructure

Start the database and Jupyter server:

```bash
docker compose up -d db jupyter
```

Optional health check:

```bash
docker compose ps
```

### 2) Run Food Bank Scraper (writes latest EFO snapshot)

This crawls configured EFO sources and writes a conformed JSONL snapshot that the ETL reads in step 3.

```bash
docker compose run --build app python -m foodbankscrapy.foodbankscrapy.main \
  --pipeline static/pipelines/prod.json \
  --conform \
  --conform-output-path /code/static/efos/latest.jsonl
```

What this does:
- crawls sources from `app/code/foodbankscrapy/static/pipelines/prod.json`
- writes raw envelopes to `app/code/foodbankscrapy/output/raw/<org_id>/<run_id>.jsonl`
- writes conformed output to `app/code/foodbankscrapy/output/conformed/<run_id>.jsonl`
- publishes latest conformed snapshot to `app/code/static/efos/latest.jsonl`

To re-conform the latest raw crawl without re-crawling:

```bash
docker compose run app python -m foodbankscrapy.foodbankscrapy.main \
  --pipeline static/pipelines/prod.json \
  --conform-only \
  --conform-output-path /code/static/efos/latest.jsonl
```

To run a test scrape (uses `test.json` pipeline, writes to `static/efos/test.jsonl`):

```bash
docker compose run --build app python scrape.py
```

### 3) Run Main ETL and Load Postgres

Runs all ETL jobs in sequence and loads results into PostGIS raw tables:

```bash
docker compose run --build app python main.py
```

ETL execution order (from `main.py`):
1. Feeding America
2. Census (TIGER + CPS FSS + ERS)
3. NHGIS (ACS 2023 extracts)
4. SNAP retailers
5. EFO loader

### 4) Open Jupyter for Post-Processing

- Open: `http://localhost:8888`
- Use token from `JUPYTER_ACCESS_TOKEN` in `.env`
- Notebooks are mounted from local `./notebooks`

Suggested notebook flow:
1. `notebooks/setup.ipynb` — configure connection
2. `notebooks/etl.ipynb` — build conformed tables from raw
3. `notebooks/analytics.ipynb` — analysis and outputs

## Useful Commands

Tail app logs:
```bash
docker compose logs -f app
```

Open psql shell:
```bash
docker compose exec db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

## Notes

- Scrapy pipeline configs:
  - `app/code/foodbankscrapy/static/pipelines/test.json` — subset of orgs for development
  - `app/code/foodbankscrapy/static/pipelines/prod.json` — full production crawl
- `queries.sql` contains ad-hoc SQL used during exploration; not part of the automated workflow.
- `backup/` contains earlier prototype scripts and is not part of the current pipeline.
