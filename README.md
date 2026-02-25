# SNAP Dependency Risk Assessment

This repository contains all of the tools to build a dataset on SNAP retailers
and Emergency Food Organizations -- food banks, pantries, etc. -- and analyze them
locally in Postgres. Specifically, it includes:
- collecting source datasets (Scrapy + API pulls),
- loading normalized tables into Postgres/PostGIS,
- running downstream analysis in Jupyter notebooks.

## Layout
- `compose.yml`: local infrastructure (Postgres, app, Jupyter).
- `app/`: Python runtime image build context.
- `app/code/main.py`: ETL orchestrator entrypoint.
- `app/code/etl/`: non-Scrapy ETL jobs (Feeding America, Census, NHGIS, SNAP, EFO loader).
- `app/code/foodbankscrapy/`: Scrapy subsystem + pipeline configs + raw/conformed outputs.
- `notebooks/`: post-processing and analysis notebooks.

## Prerequisites
- Docker + Docker Compose
- `.env` populated (or copied from `.env.default`)

Required `.env` keys:
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `JUPYTER_ACCESS_TOKEN`

## 1) Start Infrastructure
Run the EFO pull:
```bash
docker compose --build run app python main.py
```

```bash
docker compose up -d db
```

Optional health check:

```bash
docker compose ps
```

## 2) Run Food Bank Scraper + Conform (writes latest EFO snapshot)
Run inside the `app` container:

```bash
docker compose exec app python -m foodbankscrapy.foodbankscrapy.main \
  --pipeline static/pipelines/prod.json \
  --conform \
  --conform-output-path /code/static/efos/latest.jsonl
```

What this does:
- crawls sources from `app/code/foodbankscrapy/static/pipelines/prod.json`
- writes raw envelopes to `app/code/foodbankscrapy/output/raw/<org_id>/<run_id>.jsonl`
- writes conformed output to `app/code/foodbankscrapy/output/conformed/<run_id>.jsonl`
- publishes latest conformed snapshot to `app/code/static/efos/latest.jsonl`

## 3) Run Main ETL and Load Postgres
Run the orchestrator:

```bash
docker compose exec app python /code/main.py
```

This runs ETLs in sequence from `app/code/main.py` and writes tables via `utils/db.py` (`to_postgis` / `to_sql`).

## 4) Open Jupyter for Post-Processing
- Open: `http://localhost:8888`
- Use token from `JUPYTER_ACCESS_TOKEN` in `.env`
- Notebooks are mounted from local `./notebooks`

Suggested notebook flow:
1. `notebooks/setup.ipynb`
2. `notebooks/etl.ipynb`
3. `notebooks/analytics.ipynb`

## Useful Commands
- Tail app logs:
```bash
docker compose logs -f app
```

- Open psql shell:
```bash
docker compose exec db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

- Re-run only conform on latest raw (no crawl):
```bash
docker compose exec app python -m foodbankscrapy.foodbankscrapy.main \
  --pipeline static/pipelines/prod.json \
  --conform-only \
  --conform-output-path /code/static/efos/latest.jsonl
```

## Notes
- Scrapy pipeline configs live in:
  - `app/code/foodbankscrapy/static/pipelines/test.json`
  - `app/code/foodbankscrapy/static/pipelines/prod.json`
- The EFO loader (`app/code/etl/efo.py`) reads from:
  - `app/code/static/efos/latest.jsonl`
