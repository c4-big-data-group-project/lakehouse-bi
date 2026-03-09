-- 01_raw_external_tables.sql
-- Registers projected Open Food Facts CSV in Hive catalog (`raw`).

DROP TABLE IF EXISTS raw.raw_ext.open_food_facts_products;

CREATE TABLE raw.raw_ext.open_food_facts_products (
    product_code VARCHAR,
    product_url VARCHAR,
    last_modified_datetime VARCHAR,
    product_name VARCHAR,
    abbreviated_product_name VARCHAR,
    generic_name VARCHAR,
    quantity_text VARCHAR,
    brands VARCHAR,
    brands_en VARCHAR,
    categories_en VARCHAR,
    countries_en VARCHAR,
    ingredients_text VARCHAR,
    serving_size VARCHAR,
    serving_quantity VARCHAR,
    no_nutrition_data VARCHAR,
    additives_n VARCHAR,
    nutriscore_score VARCHAR,
    nutriscore_grade VARCHAR,
    nova_group VARCHAR,
    pnns_groups_1 VARCHAR,
    pnns_groups_2 VARCHAR,
    food_groups_en VARCHAR,
    environmental_score_score VARCHAR,
    environmental_score_grade VARCHAR,
    main_category_en VARCHAR,
    completeness VARCHAR,
    unique_scans_n VARCHAR,
    energy_kcal_100g VARCHAR,
    fat_100g VARCHAR,
    saturated_fat_100g VARCHAR,
    carbohydrates_100g VARCHAR,
    sugars_100g VARCHAR,
    fiber_100g VARCHAR,
    proteins_100g VARCHAR,
    salt_100g VARCHAR,
    sodium_100g VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 's3a://warehouse/raw/${DATASET}/${RAW_LAYOUT}/processing/',
    csv_separator = ',',
    skip_header_line_count = 1
);
