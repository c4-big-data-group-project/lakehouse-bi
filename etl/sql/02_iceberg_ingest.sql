-- 02_iceberg_ingest.sql
-- Casts and normalizes raw Open Food Facts records into Iceberg staging table.

SET SESSION task_concurrency = 1;
SET SESSION task_scale_writers_enabled = false;
SET SESSION scale_writers = false;

DROP TABLE IF EXISTS warehouse.${DATASET}_stg.products_stg;

CREATE TABLE warehouse.${DATASET}_stg.products_stg (
    product_code VARCHAR,
    product_name VARCHAR,
    brand_name VARCHAR,
    main_category_en VARCHAR,
    primary_country_en VARCHAR,
    nutriscore_grade VARCHAR,
    nutriscore_score INTEGER,
    nova_group INTEGER,
    energy_kcal_100g DOUBLE,
    fat_100g DOUBLE,
    saturated_fat_100g DOUBLE,
    carbohydrates_100g DOUBLE,
    sugars_100g DOUBLE,
    fiber_100g DOUBLE,
    proteins_100g DOUBLE,
    salt_100g DOUBLE,
    sodium_100g DOUBLE,
    additives_n INTEGER,
    ingredients_text VARCHAR,
    serving_quantity DOUBLE,
    completeness DOUBLE,
    unique_scans_n BIGINT,
    environmental_score_grade VARCHAR,
    environmental_score_score DOUBLE,
    no_nutrition_data BOOLEAN,
    last_modified_ts TIMESTAMP(3),
    last_modified_date DATE,
    last_modified_month DATE,
    source_path VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['last_modified_month']
);

INSERT INTO warehouse.${DATASET}_stg.products_stg
WITH typed AS (
    SELECT
        NULLIF(TRIM(product_code), '') AS product_code,
        COALESCE(
            NULLIF(TRIM(product_name), ''),
            NULLIF(TRIM(abbreviated_product_name), ''),
            NULLIF(TRIM(generic_name), '')
        ) AS product_name,
        COALESCE(NULLIF(TRIM(brands_en), ''), NULLIF(TRIM(brands), ''), 'Unknown') AS brand_name,
        COALESCE(NULLIF(TRIM(main_category_en), ''), 'Unknown') AS main_category_en,
        COALESCE(NULLIF(TRIM(countries_en), ''), 'Unknown') AS countries_en,
        COALESCE(UPPER(NULLIF(TRIM(nutriscore_grade), '')), 'UNKNOWN') AS nutriscore_grade,
        TRY_CAST(nutriscore_score AS INTEGER) AS nutriscore_score,
        TRY_CAST(nova_group AS INTEGER) AS nova_group,
        TRY_CAST(energy_kcal_100g AS DOUBLE) AS energy_kcal_100g,
        TRY_CAST(fat_100g AS DOUBLE) AS fat_100g,
        TRY_CAST(saturated_fat_100g AS DOUBLE) AS saturated_fat_100g,
        TRY_CAST(carbohydrates_100g AS DOUBLE) AS carbohydrates_100g,
        TRY_CAST(sugars_100g AS DOUBLE) AS sugars_100g,
        TRY_CAST(fiber_100g AS DOUBLE) AS fiber_100g,
        TRY_CAST(proteins_100g AS DOUBLE) AS proteins_100g,
        TRY_CAST(salt_100g AS DOUBLE) AS salt_100g,
        TRY_CAST(sodium_100g AS DOUBLE) AS sodium_100g,
        TRY_CAST(additives_n AS INTEGER) AS additives_n,
        NULLIF(TRIM(ingredients_text), '') AS ingredients_text,
        TRY_CAST(serving_quantity AS DOUBLE) AS serving_quantity,
        TRY_CAST(completeness AS DOUBLE) AS completeness,
        TRY_CAST(unique_scans_n AS BIGINT) AS unique_scans_n,
        COALESCE(UPPER(NULLIF(TRIM(environmental_score_grade), '')), 'UNKNOWN') AS environmental_score_grade,
        TRY_CAST(environmental_score_score AS DOUBLE) AS environmental_score_score,
        CASE
            WHEN LOWER(TRIM(no_nutrition_data)) IN ('1', 'true', 'yes') THEN TRUE
            WHEN LOWER(TRIM(no_nutrition_data)) IN ('0', 'false', 'no') THEN FALSE
            ELSE NULL
        END AS no_nutrition_data,
        COALESCE(
            TRY(CAST(from_iso8601_timestamp(last_modified_datetime) AS TIMESTAMP(3))),
            TRY(CAST(from_unixtime(CAST(last_modified_datetime AS DOUBLE)) AS TIMESTAMP(3))),
            TRY(CAST(date_parse(last_modified_datetime, '%Y-%m-%d %H:%i:%s') AS TIMESTAMP(3)))
        ) AS last_modified_ts,
        "$path" AS source_path
    FROM raw.raw_ext.open_food_facts_products
)
SELECT
    product_code,
    product_name,
    brand_name,
    main_category_en,
    COALESCE(NULLIF(TRIM(split_part(countries_en, ',', 1)), ''), 'Unknown') AS primary_country_en,
    nutriscore_grade,
    nutriscore_score,
    nova_group,
    energy_kcal_100g,
    fat_100g,
    saturated_fat_100g,
    carbohydrates_100g,
    sugars_100g,
    fiber_100g,
    proteins_100g,
    salt_100g,
    sodium_100g,
    additives_n,
    ingredients_text,
    serving_quantity,
    completeness,
    unique_scans_n,
    environmental_score_grade,
    environmental_score_score,
    no_nutrition_data,
    last_modified_ts,
    COALESCE(CAST(date(last_modified_ts) AS DATE), DATE '1970-01-01') AS last_modified_date,
    COALESCE(CAST(date(date_trunc('month', last_modified_ts)) AS DATE), DATE '1970-01-01') AS last_modified_month,
    source_path
FROM typed
WHERE product_code IS NOT NULL
  AND product_name IS NOT NULL;
