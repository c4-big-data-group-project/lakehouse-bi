# Data Dictionary for Agent Integration (Person 3 Contract)

Dataset: `open_food_facts`
Catalog: `warehouse`
Schema: `open_food_facts_marts`

## 1) Tables and key columns

### `fact_products_current`
Grain: one latest row per `product_code`.

Core columns:
- `product_code` (VARCHAR): OFF product code/barcode.
- `product_name` (VARCHAR): normalized product name.
- `brand_name` (VARCHAR): normalized brand label.
- `main_category_en` (VARCHAR): main category in English.
- `primary_country_en` (VARCHAR): first country from country list.
- `nutriscore_grade` (VARCHAR): A/B/C/D/E/UNKNOWN.
- `nutriscore_score` (INTEGER): numeric Nutri-Score.
- `nova_group` (INTEGER): NOVA processing level (1-4).
- `energy_kcal_100g` (DOUBLE): kcal per 100g.
- `sugars_100g` (DOUBLE): sugar per 100g.
- `salt_100g` (DOUBLE): salt per 100g.
- `additives_n` (INTEGER): additives count.
- `completeness` (DOUBLE): OFF completeness score.
- `unique_scans_n` (BIGINT): product scans count.
- `last_modified_date` (DATE): partition/date for freshness analytics.
- `last_modified_month` (DATE): month-level partition key.

### `dim_main_category`
- `category_id` (BIGINT): generated category key.
- `main_category_en` (VARCHAR): category name.
- `source_rows` (BIGINT): source rows mapped to category.

### `dim_nutriscore`
- `nutriscore_grade` (VARCHAR): grade.
- `nutriscore_label` (VARCHAR): semantic description.
- `severity_rank` (INTEGER): ordering rank.

### `mart_category_quality`
Grain: `main_category_en`.

Columns:
- `products_cnt`
- `distinct_brands_cnt`
- `pct_nutriscore_ab`
- `avg_nutriscore_score`
- `avg_energy_kcal_100g`
- `avg_sugars_100g`
- `avg_salt_100g`
- `avg_additives_n`

### `mart_brand_quality`
Grain: `brand_name`.

Columns:
- `products_cnt`
- `categories_cnt`
- `pct_nutriscore_ab`
- `avg_nutriscore_score`
- `avg_energy_kcal_100g`
- `avg_sugars_100g`
- `avg_additives_n`
- `pct_with_additives`

### `mart_country_profile`
Grain: `primary_country_en`.

Columns:
- `products_cnt`
- `distinct_brands_cnt`
- `pct_nutriscore_ab`
- `avg_nova_group`
- `avg_energy_kcal_100g`
- `avg_sugars_100g`
- `avg_salt_100g`
- `pct_without_nutrition_data`

## 2) Gold business questions with reference SQL

### Q1. Which categories contain the largest number of products?
Expected output columns: `main_category_en`, `products_cnt`, `distinct_brands_cnt`
```sql
SELECT main_category_en,
       products_cnt,
       distinct_brands_cnt
FROM warehouse.open_food_facts_marts.mart_category_quality
ORDER BY products_cnt DESC
LIMIT 20;
```

### Q2. Which categories have the healthiest profile by Nutri-Score A/B share?
Expected output columns: `main_category_en`, `products_cnt`, `pct_nutriscore_ab`
```sql
SELECT main_category_en,
       products_cnt,
       pct_nutriscore_ab,
       avg_nutriscore_score
FROM warehouse.open_food_facts_marts.mart_category_quality
WHERE products_cnt >= 100
ORDER BY pct_nutriscore_ab DESC, products_cnt DESC
LIMIT 20;
```

### Q3. Which large brands have the highest average sugar content?
Expected output columns: `brand_name`, `products_cnt`, `avg_sugars_100g`
```sql
SELECT brand_name,
       products_cnt,
       avg_sugars_100g,
       avg_energy_kcal_100g
FROM warehouse.open_food_facts_marts.mart_brand_quality
WHERE products_cnt >= 50
ORDER BY avg_sugars_100g DESC NULLS LAST
LIMIT 20;
```

### Q4. Which countries have the best overall product quality?
Expected output columns: `primary_country_en`, `products_cnt`, `pct_nutriscore_ab`, `avg_nova_group`
```sql
SELECT primary_country_en,
       products_cnt,
       pct_nutriscore_ab,
       avg_nova_group
FROM warehouse.open_food_facts_marts.mart_country_profile
WHERE products_cnt >= 200
ORDER BY pct_nutriscore_ab DESC, products_cnt DESC
LIMIT 20;
```

### Q5. What is the global Nutri-Score distribution in the current curated product set?
Expected output columns: `nutriscore_grade`, `products_cnt`, `pct_of_products`
```sql
SELECT nutriscore_grade,
       COUNT(*) AS products_cnt,
       100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS pct_of_products
FROM warehouse.open_food_facts_marts.fact_products_current
GROUP BY nutriscore_grade
ORDER BY products_cnt DESC;
```

### Q6. How do NOVA processing levels relate to Nutri-Score and sugar/salt?
Expected output columns: `nova_group`, `nutriscore_grade`, `products_cnt`, `avg_sugars_100g`, `avg_salt_100g`
```sql
SELECT nova_group,
       nutriscore_grade,
       COUNT(*) AS products_cnt,
       AVG(sugars_100g) AS avg_sugars_100g,
       AVG(salt_100g) AS avg_salt_100g
FROM warehouse.open_food_facts_marts.fact_products_current
GROUP BY nova_group, nutriscore_grade
ORDER BY nova_group, nutriscore_grade;
```
