-- 00_setup_trino.sql
-- Creates schemas for raw external table, Iceberg staging, and marts.

CREATE SCHEMA IF NOT EXISTS raw.raw_ext
WITH (location = 's3a://warehouse/raw/${DATASET}/');

CREATE SCHEMA IF NOT EXISTS warehouse.${DATASET}_stg
WITH (location = 's3a://warehouse/iceberg/${DATASET}/stg/');

CREATE SCHEMA IF NOT EXISTS warehouse.${DATASET}_marts
WITH (location = 's3a://warehouse/marts/${DATASET}/');
