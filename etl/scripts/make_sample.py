#!/usr/bin/env python3
"""Build sample projection from locally downloaded Open Food Facts full source."""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import sys
from pathlib import Path

DATASET = "open_food_facts"
SOURCE_FILENAME = "en.openfoodfacts.org.products.csv.gz"
SAMPLE_FILENAME = "openfoodfacts_products_sample.csv"
DEFAULT_SAMPLE_ROWS = 250_000

PROJECTION_COLUMNS: list[tuple[str, str]] = [
    ("code", "product_code"),
    ("url", "product_url"),
    ("last_modified_datetime", "last_modified_datetime"),
    ("product_name", "product_name"),
    ("abbreviated_product_name", "abbreviated_product_name"),
    ("generic_name", "generic_name"),
    ("quantity", "quantity_text"),
    ("brands", "brands"),
    ("brands_en", "brands_en"),
    ("categories_en", "categories_en"),
    ("countries_en", "countries_en"),
    ("ingredients_text", "ingredients_text"),
    ("serving_size", "serving_size"),
    ("serving_quantity", "serving_quantity"),
    ("no_nutrition_data", "no_nutrition_data"),
    ("additives_n", "additives_n"),
    ("nutriscore_score", "nutriscore_score"),
    ("nutriscore_grade", "nutriscore_grade"),
    ("nova_group", "nova_group"),
    ("pnns_groups_1", "pnns_groups_1"),
    ("pnns_groups_2", "pnns_groups_2"),
    ("food_groups_en", "food_groups_en"),
    ("environmental_score_score", "environmental_score_score"),
    ("environmental_score_grade", "environmental_score_grade"),
    ("main_category_en", "main_category_en"),
    ("completeness", "completeness"),
    ("unique_scans_n", "unique_scans_n"),
    ("energy-kcal_100g", "energy_kcal_100g"),
    ("fat_100g", "fat_100g"),
    ("saturated-fat_100g", "saturated_fat_100g"),
    ("carbohydrates_100g", "carbohydrates_100g"),
    ("sugars_100g", "sugars_100g"),
    ("fiber_100g", "fiber_100g"),
    ("proteins_100g", "proteins_100g"),
    ("salt_100g", "salt_100g"),
    ("sodium_100g", "sodium_100g"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build sample CSV from full Open Food Facts gzip source.")
    parser.add_argument("--data-dir", default="etl/data", help="Root data directory")
    parser.add_argument("--rows", type=int, default=DEFAULT_SAMPLE_ROWS, help="How many rows to keep in sample")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing sample output")
    return parser.parse_args()


def configure_csv_field_limit() -> None:
    max_limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(max_limit)
            return
        except OverflowError:
            max_limit //= 10


def build_sample(source_path: Path, destination: Path, row_limit: int) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)

    with source_path.open("rb") as source_raw, gzip.GzipFile(fileobj=source_raw, mode="rb") as gz_file, io.TextIOWrapper(
        gz_file, encoding="utf-8", errors="replace", newline=""
    ) as text_stream:
        reader = csv.reader(text_stream, delimiter="\t", quotechar='"')
        header = next(reader)
        index_map = {name: index for index, name in enumerate(header)}

        missing_columns = [source for source, _ in PROJECTION_COLUMNS if source not in index_map]
        if missing_columns:
            raise RuntimeError(f"Missing required columns in OFF header: {', '.join(missing_columns)}")

        written = 0
        with destination.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.writer(output_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            writer.writerow([target for _, target in PROJECTION_COLUMNS])

            projection_sources = [source for source, _ in PROJECTION_COLUMNS]
            while written < row_limit:
                try:
                    row = next(reader)
                except StopIteration:
                    break

                projected = []
                for source_column in projection_sources:
                    idx = index_map[source_column]
                    projected.append(row[idx] if idx < len(row) else "")
                writer.writerow(projected)
                written += 1

    return written


def main() -> int:
    args = parse_args()
    if args.rows <= 0:
        print("ERROR: --rows must be greater than 0", file=sys.stderr)
        return 2

    configure_csv_field_limit()

    root = Path(args.data_dir) / DATASET
    full_source = root / "full" / "source" / SOURCE_FILENAME
    sample_output = root / "sample" / "processing" / SAMPLE_FILENAME

    if not full_source.exists():
        print(f"ERROR: full source file is missing: {full_source}", file=sys.stderr)
        print("Run: python3 etl/scripts/download_dataset.py --mode full", file=sys.stderr)
        return 2

    if sample_output.exists() and not args.overwrite:
        print(f"Sample file already exists: {sample_output}")
        print("Use --overwrite to rebuild it.")
        return 0

    rows_written = build_sample(full_source, sample_output, args.rows)
    print(f"Sample file built: {sample_output}")
    print(f"Rows written: {rows_written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
