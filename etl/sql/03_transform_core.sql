-- 03_transform_core.sql
-- Builds curated dimensions and product-level current fact table.

SET SESSION task_concurrency = 1;
SET SESSION task_scale_writers_enabled = false;
SET SESSION scale_writers = false;

DROP TABLE IF EXISTS warehouse.${DATASET}_marts.dim_nutriscore;
CREATE TABLE warehouse.${DATASET}_marts.dim_nutriscore
WITH (format = 'PARQUET')
AS
SELECT *
FROM (
    VALUES
        ('A', 'Best nutritional quality', 1),
        ('B', 'Good nutritional quality', 2),
        ('C', 'Average nutritional quality', 3),
        ('D', 'Low nutritional quality', 4),
        ('E', 'Worst nutritional quality', 5),
        ('UNKNOWN', 'Grade unavailable in source', 99)
) AS t(nutriscore_grade, nutriscore_label, severity_rank);

DROP TABLE IF EXISTS warehouse.${DATASET}_marts.dim_main_category;
CREATE TABLE warehouse.${DATASET}_marts.dim_main_category
WITH (format = 'PARQUET')
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY main_category_en) AS category_id,
    main_category_en,
    COUNT(*) AS source_rows
FROM warehouse.${DATASET}_stg.products_stg
GROUP BY main_category_en;

DROP TABLE IF EXISTS warehouse.${DATASET}_marts.fact_products_current;
CREATE TABLE warehouse.${DATASET}_marts.fact_products_current (
    product_code VARCHAR,
    product_name VARCHAR,
    brand_name VARCHAR,
    category_id BIGINT,
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
    last_modified_month DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['last_modified_month']
);

INSERT INTO warehouse.${DATASET}_marts.fact_products_current
WITH ranked AS (
    SELECT
        stg.*,
        ROW_NUMBER() OVER (
            PARTITION BY stg.product_code
            ORDER BY stg.last_modified_ts DESC NULLS LAST, stg.source_path DESC
        ) AS row_num
    FROM warehouse.${DATASET}_stg.products_stg stg
)
SELECT
    r.product_code,
    r.product_name,
    r.brand_name,
    c.category_id,
    r.main_category_en,
    r.primary_country_en,
    r.nutriscore_grade,
    r.nutriscore_score,
    r.nova_group,
    r.energy_kcal_100g,
    r.fat_100g,
    r.saturated_fat_100g,
    r.carbohydrates_100g,
    r.sugars_100g,
    r.fiber_100g,
    r.proteins_100g,
    r.salt_100g,
    r.sodium_100g,
    r.additives_n,
    r.ingredients_text,
    r.serving_quantity,
    r.completeness,
    r.unique_scans_n,
    r.environmental_score_grade,
    r.environmental_score_score,
    r.no_nutrition_data,
    r.last_modified_ts,
    r.last_modified_date,
    r.last_modified_month
FROM ranked r
LEFT JOIN warehouse.${DATASET}_marts.dim_main_category c
    ON c.main_category_en = r.main_category_en
WHERE r.row_num = 1
  AND (r.energy_kcal_100g IS NULL OR r.energy_kcal_100g BETWEEN 0 AND 9000)
  AND (r.sugars_100g IS NULL OR r.sugars_100g BETWEEN 0 AND 100)
  AND (r.salt_100g IS NULL OR r.salt_100g BETWEEN 0 AND 100)
  AND (r.nova_group IS NULL OR r.nova_group BETWEEN 1 AND 4);

COMMENT ON TABLE warehouse.${DATASET}_marts.fact_products_current IS 'One latest record per product code from Open Food Facts, normalized for analytics.';
COMMENT ON COLUMN warehouse.${DATASET}_marts.fact_products_current.product_code IS 'Open Food Facts unique product barcode/code.';
COMMENT ON COLUMN warehouse.${DATASET}_marts.fact_products_current.last_modified_date IS 'Date of last product update.';
COMMENT ON COLUMN warehouse.${DATASET}_marts.fact_products_current.last_modified_month IS 'Month bucket for partitioning.';
COMMENT ON COLUMN warehouse.${DATASET}_marts.fact_products_current.nutriscore_grade IS 'Nutri-Score class A-E or UNKNOWN.';
COMMENT ON COLUMN warehouse.${DATASET}_marts.fact_products_current.nova_group IS 'NOVA processing level from 1 to 4 when available.';
COMMENT ON COLUMN warehouse.${DATASET}_marts.fact_products_current.energy_kcal_100g IS 'Energy value per 100g.';
COMMENT ON COLUMN warehouse.${DATASET}_marts.fact_products_current.sugars_100g IS 'Sugar content per 100g.';
COMMENT ON COLUMN warehouse.${DATASET}_marts.fact_products_current.salt_100g IS 'Salt content per 100g.';

COMMENT ON TABLE warehouse.${DATASET}_marts.dim_main_category IS 'Distinct product categories with source-row counts.';
COMMENT ON TABLE warehouse.${DATASET}_marts.dim_nutriscore IS 'Nutri-Score reference dimension for BI and agent prompts.';
