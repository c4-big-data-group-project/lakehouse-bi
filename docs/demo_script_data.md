# Demo Script (Part 3.2): Open Food Facts Pipeline and Marts

## Preconditions

1. Start infra:

```bash
make up
```

1. Build sample pipeline:

```bash
make etl-sample
```

## Live demo flow

### Step 1 - Show raw data is visible in Trino

```bash
docker compose exec -T trino-coordinator-and-worker trino http://localhost:8080 --execute "SELECT count(*) AS raw_rows FROM raw.raw_ext.open_food_facts_products"
```

### Step 2 - Show Iceberg staging and curated fact

```bash
docker compose exec -T trino-coordinator-and-worker trino http://localhost:8080 --execute "SELECT count(*) AS stg_rows FROM warehouse.open_food_facts_stg.products_stg"

docker compose exec -T trino-coordinator-and-worker trino http://localhost:8080 --execute "SELECT count(*) AS fact_rows, min(last_modified_date) AS min_date, max(last_modified_date) AS max_date FROM warehouse.open_food_facts_marts.fact_products_current"
```

### Step 3 - Show mart #1 (category quality)

```bash
docker compose exec -T trino-coordinator-and-worker trino http://localhost:8080 --execute "SELECT main_category_en, products_cnt, pct_nutriscore_ab, avg_sugars_100g FROM warehouse.open_food_facts_marts.mart_category_quality ORDER BY products_cnt DESC LIMIT 10"
```

### Step 4 - Show mart #2 (brand quality)

```bash
docker compose exec -T trino-coordinator-and-worker trino http://localhost:8080 --execute "SELECT brand_name, products_cnt, pct_nutriscore_ab, avg_additives_n FROM warehouse.open_food_facts_marts.mart_brand_quality WHERE products_cnt >= 50 ORDER BY products_cnt DESC LIMIT 10"
```

### Step 5 - Show MinIO artifacts (raw + iceberg + marts)

```bash
docker compose exec -T minio sh -lc 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && echo "RAW:" && mc ls --recursive local/warehouse/raw/open_food_facts/sample/ | head -n 20 && echo "ICEBERG:" && mc ls --recursive local/warehouse/iceberg/open_food_facts/ | head -n 20 && echo "MARTS:" && mc ls --recursive local/warehouse/marts/open_food_facts/ | head -n 20'
```

## Optional close

Run acceptance checks:

```bash
make etl-verify MODE=sample
```

Stop infra:

```bash
make down
```
