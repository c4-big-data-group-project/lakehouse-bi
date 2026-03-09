-- 04_marts.sql
-- Creates analytical marts for Open Food Facts.

DROP TABLE IF EXISTS warehouse.${DATASET}_marts.mart_category_quality;
CREATE TABLE warehouse.${DATASET}_marts.mart_category_quality
WITH (format = 'PARQUET')
AS
SELECT
    main_category_en,
    COUNT(*) AS products_cnt,
    COUNT(DISTINCT brand_name) AS distinct_brands_cnt,
    AVG(CASE WHEN nutriscore_grade IN ('A', 'B') THEN 1.0 ELSE 0.0 END) AS pct_nutriscore_ab,
    AVG(CAST(nutriscore_score AS DOUBLE)) AS avg_nutriscore_score,
    AVG(energy_kcal_100g) AS avg_energy_kcal_100g,
    AVG(sugars_100g) AS avg_sugars_100g,
    AVG(salt_100g) AS avg_salt_100g,
    AVG(CAST(additives_n AS DOUBLE)) AS avg_additives_n
FROM warehouse.${DATASET}_marts.fact_products_current
GROUP BY main_category_en;

DROP TABLE IF EXISTS warehouse.${DATASET}_marts.mart_brand_quality;
CREATE TABLE warehouse.${DATASET}_marts.mart_brand_quality
WITH (format = 'PARQUET')
AS
SELECT
    brand_name,
    COUNT(*) AS products_cnt,
    COUNT(DISTINCT main_category_en) AS categories_cnt,
    AVG(CASE WHEN nutriscore_grade IN ('A', 'B') THEN 1.0 ELSE 0.0 END) AS pct_nutriscore_ab,
    AVG(CAST(nutriscore_score AS DOUBLE)) AS avg_nutriscore_score,
    AVG(energy_kcal_100g) AS avg_energy_kcal_100g,
    AVG(sugars_100g) AS avg_sugars_100g,
    AVG(CAST(additives_n AS DOUBLE)) AS avg_additives_n,
    AVG(CASE WHEN additives_n > 0 THEN 1.0 ELSE 0.0 END) AS pct_with_additives
FROM warehouse.${DATASET}_marts.fact_products_current
GROUP BY brand_name;

DROP TABLE IF EXISTS warehouse.${DATASET}_marts.mart_country_profile;
CREATE TABLE warehouse.${DATASET}_marts.mart_country_profile
WITH (format = 'PARQUET')
AS
SELECT
    primary_country_en,
    COUNT(*) AS products_cnt,
    COUNT(DISTINCT brand_name) AS distinct_brands_cnt,
    AVG(CASE WHEN nutriscore_grade IN ('A', 'B') THEN 1.0 ELSE 0.0 END) AS pct_nutriscore_ab,
    AVG(CAST(nova_group AS DOUBLE)) AS avg_nova_group,
    AVG(energy_kcal_100g) AS avg_energy_kcal_100g,
    AVG(sugars_100g) AS avg_sugars_100g,
    AVG(salt_100g) AS avg_salt_100g,
    AVG(CASE WHEN no_nutrition_data THEN 1.0 ELSE 0.0 END) AS pct_without_nutrition_data
FROM warehouse.${DATASET}_marts.fact_products_current
GROUP BY primary_country_en;

COMMENT ON TABLE warehouse.${DATASET}_marts.mart_category_quality IS 'Category-level nutrition and quality aggregates.';
COMMENT ON TABLE warehouse.${DATASET}_marts.mart_brand_quality IS 'Brand-level portfolio quality aggregates.';
COMMENT ON TABLE warehouse.${DATASET}_marts.mart_country_profile IS 'Country-level product quality and nutrition profile.';
