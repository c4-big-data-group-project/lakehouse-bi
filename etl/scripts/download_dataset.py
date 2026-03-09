#!/usr/bin/env python3
"""Download and prepare Open Food Facts dataset for sample/full ETL modes."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

DATASET = "open_food_facts"
PRIMARY_SOURCE_URL = "https://openfoodfacts-ds.s3.eu-west-3.amazonaws.com/en.openfoodfacts.org.products.csv.gz"
FALLBACK_SOURCE_URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
SOURCE_FILENAME = "en.openfoodfacts.org.products.csv.gz"
SAMPLE_PROJECTION_FILENAME = "openfoodfacts_products_sample.csv"
FULL_PROJECTION_FILENAME = "openfoodfacts_products_full.csv"
DEFAULT_SAMPLE_ROWS = 250_000
DEFAULT_FULL_ROWS = 1_500_000
MAX_RETRIES = 5
RETRYABLE_HTTP_CODES = {403, 408, 429, 500, 502, 503, 504}
REQUEST_TIMEOUT_SECONDS = 300

REQUEST_HEADERS = {
    "User-Agent": "lakehouse-bi-etl/2.1 (+https://github.com/c4-big-data-group-project/lakehouse-bi)",
    "Accept": "*/*",
}

# Mapping: (source column in OFF dump, output column in projected CSV)
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


@dataclass
class ModePaths:
    mode_root: Path
    source_path: Path
    projection_path: Path


@dataclass
class FileRecord:
    kind: str
    path: Path
    status: str
    source_url: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Open Food Facts data files.")
    parser.add_argument("--mode", choices=("sample", "full"), default="sample")
    parser.add_argument("--data-dir", default="etl/data", help="Local directory used for data files.")
    parser.add_argument(
        "--manifest-path",
        default=f"etl/manifests/{DATASET}_files.json",
        help="Path to output manifest JSON.",
    )
    parser.add_argument("--source-url", default=PRIMARY_SOURCE_URL, help="Primary OFF source CSV.GZ URL.")
    parser.add_argument("--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS)
    parser.add_argument("--full-rows", type=int, default=DEFAULT_FULL_ROWS)
    parser.add_argument("--force", action="store_true", help="Redownload/rebuild files if they already exist.")
    parser.add_argument("--checksum", action="store_true", help="Add SHA256 checksums to manifest.")
    parser.add_argument("--dry-run", action="store_true", help="Do not download/build files.")
    parser.add_argument(
        "--clean-extra",
        action="store_true",
        help="Delete local files under mode path that are not part of current plan.",
    )
    parser.add_argument(
        "--no-fallback",
        action="store_true",
        help="Disable fallback to static.openfoodfacts.org when primary URL fails.",
    )
    return parser.parse_args()


def resolve_paths(data_dir: Path, mode: str) -> ModePaths:
    mode_root = data_dir / DATASET / mode
    source_path = mode_root / "source" / SOURCE_FILENAME
    projection_filename = SAMPLE_PROJECTION_FILENAME if mode == "sample" else FULL_PROJECTION_FILENAME
    projection_path = mode_root / "processing" / projection_filename
    return ModePaths(mode_root=mode_root, source_path=source_path, projection_path=projection_path)


def configure_csv_field_limit() -> None:
    max_limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(max_limit)
            return
        except OverflowError:
            max_limit //= 10


def candidate_urls(primary_url: str, use_fallback: bool) -> list[str]:
    urls = [primary_url]
    if use_fallback and primary_url != FALLBACK_SOURCE_URL:
        urls.append(FALLBACK_SOURCE_URL)
    return urls


def human_size(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    amount = float(value)
    for unit in units:
        if amount < 1024.0 or unit == units[-1]:
            return f"{amount:.2f} {unit}"
        amount /= 1024.0
    return f"{value} B"


def _retry_sleep(attempt: int) -> None:
    time.sleep(min(2**attempt, 30))


def head_content_length(url: str) -> int | None:
    request = urllib.request.Request(url, method="HEAD", headers=REQUEST_HEADERS)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            raw_length = response.headers.get("Content-Length")
            return int(raw_length) if raw_length and raw_length.isdigit() else None
    except (urllib.error.HTTPError, urllib.error.URLError, ValueError):
        return None


def clean_extra_files(mode_root: Path, planned_files: set[Path]) -> tuple[int, int]:
    if not mode_root.exists():
        return 0, 0

    removed_count = 0
    removed_bytes = 0

    for path in sorted(mode_root.rglob("*")):
        if path.is_file() and path not in planned_files:
            removed_bytes += path.stat().st_size
            path.unlink()
            removed_count += 1

    for path in sorted(mode_root.rglob("*"), reverse=True):
        if path.is_dir():
            try:
                path.rmdir()
            except OSError:
                continue

    return removed_count, removed_bytes


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def download_file(url: str, destination: Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".part")
    if temp_path.exists():
        temp_path.unlink()

    for attempt in range(MAX_RETRIES + 1):
        try:
            request = urllib.request.Request(url, headers=REQUEST_HEADERS)
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response, temp_path.open("wb") as output:
                total = 0
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)
                    total += len(chunk)
            temp_path.replace(destination)
            return total
        except urllib.error.HTTPError as error:
            if attempt < MAX_RETRIES and error.code in RETRYABLE_HTTP_CODES:
                _retry_sleep(attempt)
                continue
            if temp_path.exists():
                temp_path.unlink()
            raise
        except urllib.error.URLError:
            if attempt < MAX_RETRIES:
                _retry_sleep(attempt)
                continue
            if temp_path.exists():
                temp_path.unlink()
            raise

    raise RuntimeError(f"Failed to download after retries: {url}")


def iter_rows(reader: csv.reader, index_map: dict[str, int], row_limit: int) -> list[list[str]]:
    projection_sources = [source for source, _ in PROJECTION_COLUMNS]
    rows: list[list[str]] = []

    while len(rows) < row_limit:
        try:
            row = next(reader)
        except StopIteration:
            break

        projected: list[str] = []
        for source_column in projection_sources:
            idx = index_map[source_column]
            projected.append(row[idx] if idx < len(row) else "")
        rows.append(projected)

    return rows


def build_projection_from_text_stream(text_stream: io.TextIOBase, output_path: Path, row_limit: int) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    reader = csv.reader(text_stream, delimiter="\t", quotechar='"')
    try:
        header = next(reader)
    except StopIteration as error:
        raise RuntimeError("Source dataset is empty.") from error

    index_map = {name: i for i, name in enumerate(header)}
    missing_columns = [source for source, _ in PROJECTION_COLUMNS if source not in index_map]
    if missing_columns:
        joined = ", ".join(missing_columns)
        raise RuntimeError(f"OFF header does not include required columns: {joined}")

    rows = iter_rows(reader, index_map, row_limit)

    with output_path.open("w", encoding="utf-8", newline="") as out_file:
        writer = csv.writer(out_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        writer.writerow([target for _, target in PROJECTION_COLUMNS])
        writer.writerows(rows)

    return len(rows)


def build_projection_from_local_gzip(source_path: Path, output_path: Path, row_limit: int) -> int:
    with source_path.open("rb") as source_raw, gzip.GzipFile(fileobj=source_raw, mode="rb") as gz_file, io.TextIOWrapper(
        gz_file, encoding="utf-8", errors="replace", newline=""
    ) as text_stream:
        return build_projection_from_text_stream(text_stream, output_path, row_limit)


def build_projection_from_remote_gzip(url: str, output_path: Path, row_limit: int) -> int:
    request = urllib.request.Request(url, headers=REQUEST_HEADERS)
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response, gzip.GzipFile(
        fileobj=response, mode="rb"
    ) as gz_file, io.TextIOWrapper(gz_file, encoding="utf-8", errors="replace", newline="") as text_stream:
        return build_projection_from_text_stream(text_stream, output_path, row_limit)


def first_available_source_size(urls: list[str]) -> tuple[str | None, int | None]:
    for url in urls:
        size = head_content_length(url)
        if size is not None:
            return url, size
    return None, None


def build_manifest_file_entry(
    file_record: FileRecord,
    checksum_enabled: bool,
) -> dict[str, object]:
    size_bytes = file_record.path.stat().st_size
    return {
        "kind": file_record.kind,
        "local_path": str(file_record.path),
        "url": file_record.source_url,
        "size_bytes": size_bytes,
        "status": file_record.status,
        "sha256": sha256_file(file_record.path) if checksum_enabled else None,
    }


def download_with_fallback(urls: list[str], destination: Path) -> tuple[str, int]:
    last_error: Exception | None = None
    for url in urls:
        try:
            downloaded = download_file(url, destination)
            return url, downloaded
        except Exception as error:  # noqa: BLE001
            last_error = error
            print(f"WARN: download failed from {url}: {error}", file=sys.stderr)
    if last_error is not None:
        raise last_error
    raise RuntimeError("No source URLs provided")


def build_sample_projection_with_fallback(urls: list[str], projection_path: Path, row_limit: int) -> tuple[str, int]:
    last_error: Exception | None = None
    for url in urls:
        try:
            rows_written = build_projection_from_remote_gzip(url, projection_path, row_limit)
            return url, rows_written
        except Exception as error:  # noqa: BLE001
            last_error = error
            print(f"WARN: projection stream failed from {url}: {error}", file=sys.stderr)
    if last_error is not None:
        raise last_error
    raise RuntimeError("No source URLs provided")


def main() -> int:
    args = parse_args()
    configure_csv_field_limit()

    data_dir = Path(args.data_dir)
    manifest_path = Path(args.manifest_path)
    paths = resolve_paths(data_dir, args.mode)
    row_limit = args.sample_rows if args.mode == "sample" else args.full_rows

    if row_limit <= 0:
        print("ERROR: row limit must be greater than 0", file=sys.stderr)
        return 2

    urls = candidate_urls(args.source_url, use_fallback=not args.no_fallback)
    source_size_url, source_size = first_available_source_size(urls)

    planned_files: set[Path] = {paths.projection_path}
    if args.mode == "full":
        planned_files.add(paths.source_path)

    if args.clean_extra and not args.dry_run:
        removed_count, removed_bytes = clean_extra_files(paths.mode_root, planned_files)
        if removed_count > 0:
            print(f"Removed extra files: {removed_count} ({human_size(removed_bytes)}) from {paths.mode_root}")

    if args.dry_run:
        print(f"Planned mode: {args.mode}")
        print(f"Source candidates: {', '.join(urls)}")
        print(f"Projection row limit: {row_limit}")
        if source_size is not None and source_size_url is not None:
            print(f"Source size from HEAD ({source_size_url}): {human_size(source_size)} ({source_size} bytes)")
        else:
            print("Source size is unavailable (HEAD without Content-Length).")

        manifest = {
            "dataset": DATASET,
            "mode": args.mode,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_candidates": urls,
            "source_size_url": source_size_url,
            "source_size_bytes": source_size,
            "source_size_human": human_size(source_size) if source_size is not None else None,
            "projection_row_limit": row_limit,
            "projection_rows_written": None,
            "files": [
                {
                    "kind": "source_csv_gz" if args.mode == "full" else "projected_csv",
                    "local_path": str(paths.source_path if args.mode == "full" else paths.projection_path),
                    "url": source_size_url,
                    "size_bytes": source_size,
                    "status": "planned",
                    "sha256": None,
                }
            ],
            "total_bytes": source_size,
            "total_human": human_size(source_size) if source_size is not None else None,
        }
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Manifest written: {manifest_path}")
        return 0

    records: list[FileRecord] = []
    used_source_url: str | None = None
    projection_rows_written = 0

    if args.mode == "full":
        if paths.source_path.exists() and not args.force:
            print(f"Skipping source download (already exists): {paths.source_path}")
            records.append(FileRecord("source_csv_gz", paths.source_path, "skipped_exists", None))
        else:
            print(f"Downloading source CSV.GZ to {paths.source_path}")
            used_source_url, downloaded_bytes = download_with_fallback(urls, paths.source_path)
            print(f"Downloaded from {used_source_url}: {human_size(downloaded_bytes)} ({downloaded_bytes} bytes)")
            records.append(FileRecord("source_csv_gz", paths.source_path, "downloaded", used_source_url))

        if paths.projection_path.exists() and not args.force:
            print(f"Skipping projection build (already exists): {paths.projection_path}")
            records.append(FileRecord("projected_csv", paths.projection_path, "skipped_exists", None))
        else:
            print(f"Building projected CSV from local gzip: {paths.projection_path}")
            projection_rows_written = build_projection_from_local_gzip(paths.source_path, paths.projection_path, row_limit)
            print(f"Projection rows written: {projection_rows_written}")
            records.append(FileRecord("projected_csv", paths.projection_path, "built_from_local_source", None))
    else:
        if paths.projection_path.exists() and not args.force:
            print(f"Skipping projection build (already exists): {paths.projection_path}")
            records.append(FileRecord("projected_csv", paths.projection_path, "skipped_exists", None))
        else:
            print(f"Streaming sample projection from remote gzip into {paths.projection_path}")
            used_source_url, projection_rows_written = build_sample_projection_with_fallback(urls, paths.projection_path, row_limit)
            print(f"Projection rows written: {projection_rows_written} from {used_source_url}")
            records.append(FileRecord("projected_csv", paths.projection_path, "built_from_remote_stream", used_source_url))

    file_entries = [build_manifest_file_entry(record, checksum_enabled=args.checksum) for record in records]
    total_bytes = sum(int(entry["size_bytes"]) for entry in file_entries)

    manifest = {
        "dataset": DATASET,
        "mode": args.mode,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_candidates": urls,
        "source_url_used": used_source_url,
        "source_size_url": source_size_url,
        "source_size_bytes": source_size,
        "source_size_human": human_size(source_size) if source_size is not None else None,
        "projection_row_limit": row_limit,
        "projection_rows_written": projection_rows_written,
        "files": file_entries,
        "total_bytes": total_bytes,
        "total_human": human_size(total_bytes),
    }

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Manifest written: {manifest_path}")
    if args.mode == "full" and source_size is not None and source_size <= 1_000_000_000:
        print("WARNING: full source file size is <= 1GB; requirement 1.1 may fail.", file=sys.stderr)
    print(f"Total local size for mode={args.mode}: {human_size(total_bytes)} ({total_bytes} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
