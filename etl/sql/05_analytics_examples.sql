-- 05_analytics_examples.sql
-- Demo/business queries for Open Food Facts marts.

-- Q1: Top categories by product volume.
SELECT
    main_category_en,
    products_cnt,
    distinct_brands_cnt,
    pct_nutriscore_ab
FROM warehouse.${DATASET}_marts.mart_category_quality
ORDER BY products_cnt DESC
LIMIT 15;

-- Q2: Categories with best Nutri-Score quality (A/B share), limited to meaningful size.
SELECT
    main_category_en,
    products_cnt,
    pct_nutriscore_ab,
    avg_nutriscore_score
FROM warehouse.${DATASET}_marts.mart_category_quality
WHERE products_cnt >= 100
ORDER BY pct_nutriscore_ab DESC, products_cnt DESC
LIMIT 15;

-- Q3: Categories with highest average sugar content.
SELECT
    main_category_en,
    products_cnt,
    avg_sugars_100g,
    avg_energy_kcal_100g
FROM warehouse.${DATASET}_marts.mart_category_quality
WHERE products_cnt >= 100
ORDER BY avg_sugars_100g DESC NULLS LAST
LIMIT 15;

-- Q4: Largest brands and their nutrition quality profile.
SELECT
    brand_name,
    products_cnt,
    pct_nutriscore_ab,
    avg_sugars_100g,
    avg_additives_n
FROM warehouse.${DATASET}_marts.mart_brand_quality
WHERE products_cnt >= 50
ORDER BY products_cnt DESC
LIMIT 20;

-- Q5: Country profile by A/B Nutri-Score share.
SELECT
    primary_country_en,
    products_cnt,
    pct_nutriscore_ab,
    avg_nova_group,
    pct_without_nutrition_data
FROM warehouse.${DATASET}_marts.mart_country_profile
WHERE products_cnt >= 200
ORDER BY pct_nutriscore_ab DESC, products_cnt DESC
LIMIT 20;

-- Q6: Nutri-Score distribution in the curated fact table.
SELECT
    nutriscore_grade,
    COUNT(*) AS products_cnt,
    100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS pct_of_products
FROM warehouse.${DATASET}_marts.fact_products_current
GROUP BY nutriscore_grade
ORDER BY products_cnt DESC;

-- Q7: NOVA vs Nutri-Score cross matrix.
SELECT
    nova_group,
    nutriscore_grade,
    COUNT(*) AS products_cnt,
    AVG(sugars_100g) AS avg_sugars_100g,
    AVG(salt_100g) AS avg_salt_100g
FROM warehouse.${DATASET}_marts.fact_products_current
GROUP BY nova_group, nutriscore_grade
ORDER BY nova_group, nutriscore_grade;

-- Q8: Data freshness snapshot by modification date.
SELECT
    last_modified_date,
    COUNT(*) AS products_cnt,
    AVG(completeness) AS avg_completeness,
    AVG(unique_scans_n) AS avg_unique_scans
FROM warehouse.${DATASET}_marts.fact_products_current
GROUP BY last_modified_date
ORDER BY last_modified_date DESC
LIMIT 15;
