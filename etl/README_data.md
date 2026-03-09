# ETL Data Pipeline (Person 2) - Open Food Facts

This folder implements Person 2 scope for Group Project #2 using Open Food Facts:
- 1.1 dataset selection with payload >1GB
- 1.3 raw upload to MinIO (AS IS)
- 1.4 load to Iceberg via Trino
- 1.5 transforms + marts + analytics queries
- 3.2 repeatable demo flow

## Dataset and size strategy

Source dataset:
- Kaggle landing page: https://www.kaggle.com/datasets/konradb/open-food-facts
- Direct source used by pipeline: https://openfoodfacts-ds.s3.eu-west-3.amazonaws.com/en.openfoodfacts.org.products.csv.gz

`full` mode downloads one official compressed source file (`en.openfoodfacts.org.products.csv.gz`) with size just above 1GB (around 1.17GB, verify with `--dry-run`).
This satisfies the strict minimum requirement while avoiding downloading the full uncompressed 12+ GB dump.

## Prerequisites

1. Start infrastructure:
```bash
make up
```

2. Install Python dependency (uploader):
```bash
python3 -m pip install -r etl/requirements.txt
```

3. Ensure `.env` exists:
```bash
test -f .env || cp .example.env .env
```

## Storage layout in MinIO

Bucket: `warehouse`

Prefixes:
- raw: `raw/open_food_facts/...`
- iceberg: `iceberg/open_food_facts/...`
- marts: `marts/open_food_facts/...`

Raw object layout:
- sample mode:
  - `raw/open_food_facts/sample/processing/openfoodfacts_products_sample.csv`
- full mode:
  - `raw/open_food_facts/full/source/en.openfoodfacts.org.products.csv.gz` (AS IS source)
  - `raw/open_food_facts/full/processing/openfoodfacts_products_full.csv` (projected CSV for Trino raw table)

## Download commands

### Sample mode (fast development)
```bash
python3 etl/scripts/download_dataset.py --mode sample
```

### Full mode (>1GB)
```bash
python3 etl/scripts/download_dataset.py --mode full
```

### Check expected size without downloading
```bash
python3 etl/scripts/download_dataset.py --mode full --dry-run
```

### Clean extra local files for deterministic reruns
```bash
python3 etl/scripts/download_dataset.py --mode full --clean-extra
```

### Optional: rebuild sample from local full source
```bash
python3 etl/scripts/make_sample.py --rows 250000 --overwrite
```

## Upload raw files to MinIO

### Sample
```bash
python3 etl/scripts/upload_to_minio.py --mode sample --dataset open_food_facts --prune-extra
```

### Full
```bash
python3 etl/scripts/upload_to_minio.py --mode full --dataset open_food_facts --prune-extra
```

The script is idempotent:
- skips objects that already exist with the same size
- optionally deletes stale objects under mode prefix when `--prune-extra` is enabled
- writes run report to `etl/reports/raw_upload_<timestamp>.md`

## SQL pipeline

Run full SQL chain (setup -> raw table -> staging -> core -> marts -> analytics examples):
```bash
MODE=sample DATASET=open_food_facts ./etl/scripts/run_sql.sh
```

`run_sql.sh` now waits for Trino readiness before executing SQL files, which prevents `Trino server is still initializing` failures right after `make up`.

Run only acceptance checks:
```bash
MODE=sample DATASET=open_food_facts ./etl/scripts/run_sql.sh etl/sql/99_acceptance_checks.sql
```

## One-command targets

Sample end-to-end:
```bash
make etl-sample
```

Full end-to-end:
```bash
make etl-full
```

Verify (acceptance SQL only):
```bash
make etl-verify MODE=sample
make etl-verify MODE=full
```

Notes about `etl-verify`:
- it always refreshes raw external table registration for the selected `MODE` (`sample` or `full`);
- it validates the currently loaded staging/fact/marts tables as they exist in Iceberg;
- for strict mode-specific end-to-end checks, run `make etl-sample` or `make etl-full` immediately before `make etl-verify`.

## Data model summary

Schemas:
- raw external: `raw.raw_ext`
- staging: `warehouse.open_food_facts_stg`
- marts: `warehouse.open_food_facts_marts`

Key tables:
- raw external table: `raw.raw_ext.open_food_facts_products`
- staging Iceberg table: `warehouse.open_food_facts_stg.products_stg`
- curated fact: `warehouse.open_food_facts_marts.fact_products_current`
- dimensions:
  - `warehouse.open_food_facts_marts.dim_main_category`
  - `warehouse.open_food_facts_marts.dim_nutriscore`
- marts:
  - `warehouse.open_food_facts_marts.mart_category_quality`
  - `warehouse.open_food_facts_marts.mart_brand_quality`
  - `warehouse.open_food_facts_marts.mart_country_profile`

## Quick checks

```bash
docker compose exec -T trino-coordinator-and-worker trino http://localhost:8080 --execute "SHOW CATALOGS"

docker compose exec -T trino-coordinator-and-worker trino http://localhost:8080 --execute "SELECT count(*) FROM raw.raw_ext.open_food_facts_products"

docker compose exec -T trino-coordinator-and-worker trino http://localhost:8080 --execute "SELECT count(*) FROM warehouse.open_food_facts_marts.fact_products_current"

docker compose exec -T trino-coordinator-and-worker trino http://localhost:8080 --execute "SELECT * FROM warehouse.open_food_facts_marts.mart_category_quality ORDER BY products_cnt DESC LIMIT 10"
```
