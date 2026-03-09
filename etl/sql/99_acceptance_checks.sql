-- 99_acceptance_checks.sql
-- Acceptance checks for Open Food Facts data pipeline.

-- A) Connectivity / visibility checks.
SELECT 'catalog_raw_exists' AS check_name, COUNT(*) > 0 AS passed
FROM system.metadata.catalogs
WHERE catalog_name = 'raw';

SELECT 'catalog_warehouse_exists' AS check_name, COUNT(*) > 0 AS passed
FROM system.metadata.catalogs
WHERE catalog_name = 'warehouse';

SELECT 'schema_raw_ext_exists' AS check_name, COUNT(*) > 0 AS passed
FROM raw.information_schema.schemata
WHERE schema_name = 'raw_ext';

SELECT 'schema_stg_exists' AS check_name, COUNT(*) > 0 AS passed
FROM warehouse.information_schema.schemata
WHERE schema_name = '${DATASET}_stg';

SELECT 'schema_marts_exists' AS check_name, COUNT(*) > 0 AS passed
FROM warehouse.information_schema.schemata
WHERE schema_name = '${DATASET}_marts';

-- B) Raw checks.
SELECT 'raw_products_count' AS check_name, COUNT(*) AS value
FROM raw.raw_ext.open_food_facts_products;

SELECT
    'raw_key_nulls' AS check_name,
    SUM(CASE WHEN product_code IS NULL OR TRIM(product_code) = '' OR product_name IS NULL OR TRIM(product_name) = '' THEN 1 ELSE 0 END) AS value
FROM raw.raw_ext.open_food_facts_products;

-- C) Iceberg ingestion checks.
SELECT 'stg_products_count' AS check_name, COUNT(*) AS value
FROM warehouse.${DATASET}_stg.products_stg;

SELECT 'stg_last_modified_date_nulls' AS check_name,
       SUM(CASE WHEN last_modified_date IS NULL THEN 1 ELSE 0 END) AS value
FROM warehouse.${DATASET}_stg.products_stg;

SELECT 'stg_last_modified_month_nulls' AS check_name,
       SUM(CASE WHEN last_modified_month IS NULL THEN 1 ELSE 0 END) AS value
FROM warehouse.${DATASET}_stg.products_stg;

SELECT 'fact_products_count' AS check_name, COUNT(*) AS value
FROM warehouse.${DATASET}_marts.fact_products_current;

SELECT 'fact_product_code_nulls' AS check_name,
       SUM(CASE WHEN product_code IS NULL OR TRIM(product_code) = '' THEN 1 ELSE 0 END) AS value
FROM warehouse.${DATASET}_marts.fact_products_current;

SELECT 'fact_date_range' AS check_name,
       MIN(last_modified_date) AS min_last_modified_date,
       MAX(last_modified_date) AS max_last_modified_date
FROM warehouse.${DATASET}_marts.fact_products_current;

-- D) Mart checks.
SELECT 'mart_category_quality_count' AS check_name, COUNT(*) AS value
FROM warehouse.${DATASET}_marts.mart_category_quality;

SELECT 'mart_brand_quality_count' AS check_name, COUNT(*) AS value
FROM warehouse.${DATASET}_marts.mart_brand_quality;

SELECT 'mart_country_profile_count' AS check_name, COUNT(*) AS value
FROM warehouse.${DATASET}_marts.mart_country_profile;

SELECT 'mart_category_non_positive_products_rows' AS check_name,
       SUM(CASE WHEN products_cnt <= 0 THEN 1 ELSE 0 END) AS value
FROM warehouse.${DATASET}_marts.mart_category_quality;

SELECT 'mart_brand_negative_sugar_rows' AS check_name,
       SUM(CASE WHEN avg_sugars_100g < 0 THEN 1 ELSE 0 END) AS value
FROM warehouse.${DATASET}_marts.mart_brand_quality;

SELECT 'mart_country_negative_products_rows' AS check_name,
       SUM(CASE WHEN products_cnt <= 0 THEN 1 ELSE 0 END) AS value
FROM warehouse.${DATASET}_marts.mart_country_profile;
