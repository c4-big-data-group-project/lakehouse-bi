# Data Contract: Open Food Facts (Person 2)

Updated: 2026-03-09
Owner: Person 2 (Data ingestion, transforms, marts)

## 1) Dataset selection and source

Primary dataset: **Open Food Facts product dump**

- Format in source: `CSV.GZ` (tab-separated content inside gzip)
- Projected processing format: `CSV` (comma-separated, selected columns)

Sources:

- Kaggle reference page: <https://www.kaggle.com/datasets/konradb/open-food-facts>
- Direct dump URL used in pipeline:
  - <https://openfoodfacts-ds.s3.eu-west-3.amazonaws.com/en.openfoodfacts.org.products.csv.gz>
- Fallback URL (if primary unavailable):
  - <https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz>

Licensing note:

- Open Food Facts data is published under Open Database License (ODbL).
- This project uses data for educational analytics; follow official license terms for redistribution.

## 2) Requirement 1.1 sizing strategy (>1GB)

`full` mode downloads one official source file:

- `en.openfoodfacts.org.products.csv.gz`
- expected size (HEAD): about **1.17 GB** (~1,169,499,939 bytes)

This intentionally keeps payload only slightly above 1GB (minimum requirement) and avoids downloading the full uncompressed multi-GB dump.

## 3) Storage contract (MinIO / S3)

Bucket: `warehouse`

Prefixes:

- raw: `raw/open_food_facts/...`
- iceberg staging: `iceberg/open_food_facts/stg/...`
- marts: `marts/open_food_facts/...`

Mode layouts:

- sample:
  - `raw/open_food_facts/sample/processing/openfoodfacts_products_sample.csv`
- full:
  - `raw/open_food_facts/full/source/en.openfoodfacts.org.products.csv.gz` (AS IS)
  - `raw/open_food_facts/full/processing/openfoodfacts_products_full.csv` (projected for Trino external table)

## 4) Raw schema preview (projected columns)

Raw external table: `raw.raw_ext.open_food_facts_products`

Selected columns (36):

1. `product_code`
2. `product_url`
3. `last_modified_datetime`
4. `product_name`
5. `abbreviated_product_name`
6. `generic_name`
7. `quantity_text`
8. `brands`
9. `brands_en`
10. `categories_en`
11. `countries_en`
12. `ingredients_text`
13. `serving_size`
14. `serving_quantity`
15. `no_nutrition_data`
16. `additives_n`
17. `nutriscore_score`
18. `nutriscore_grade`
19. `nova_group`
20. `pnns_groups_1`
21. `pnns_groups_2`
22. `food_groups_en`
23. `environmental_score_score`
24. `environmental_score_grade`
25. `main_category_en`
26. `completeness`
27. `unique_scans_n`
28. `energy_kcal_100g`
29. `fat_100g`
30. `saturated_fat_100g`
31. `carbohydrates_100g`
32. `sugars_100g`
33. `fiber_100g`
34. `proteins_100g`
35. `salt_100g`
36. `sodium_100g`

## 5) Curated model contract

Catalogs:

- `raw` (Hive connector for external raw CSV)
- `warehouse` (Iceberg connector)

Core tables:

- staging: `warehouse.open_food_facts_stg.products_stg`
- curated fact: `warehouse.open_food_facts_marts.fact_products_current`
- dimensions:
  - `warehouse.open_food_facts_marts.dim_main_category`
  - `warehouse.open_food_facts_marts.dim_nutriscore`

Marts:

- `warehouse.open_food_facts_marts.mart_category_quality`
- `warehouse.open_food_facts_marts.mart_brand_quality`
- `warehouse.open_food_facts_marts.mart_country_profile`

Partitioning:

- staging `products_stg`: partitioned by `last_modified_month`
- curated `fact_products_current`: partitioned by `last_modified_month`

## 6) Data quality contract

Minimum quality rules:

- `product_code` and `product_name` are mandatory for staging/fact.
- `fact_products_current` keeps the latest row per `product_code`.
- basic numeric sanity filters are applied (e.g., sugars/salt not negative).
- marts must be non-empty after successful sample/full pipeline run.
