#!/usr/bin/env python3
"""Upload raw dataset files to MinIO as-is."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError
except ModuleNotFoundError as error:
    raise SystemExit(
        "boto3 is required. Install dependencies first: pip install -r etl/requirements.txt"
    ) from error


DEFAULT_DATASET = "open_food_facts"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload local raw files to MinIO.")
    parser.add_argument("--mode", choices=("sample", "full"), default="sample")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--bucket", default="warehouse")
    parser.add_argument("--data-dir", default="etl/data")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--prefix-base", default="raw")
    parser.add_argument(
        "--manifest-path",
        default=None,
        help="Path to manifest JSON (defaults to etl/manifests/<dataset>_files.json).",
    )
    parser.add_argument(
        "--prune-extra",
        action="store_true",
        help="Delete remote objects under the mode prefix that are not present in current upload file set.",
    )
    return parser.parse_args()


def parse_env_file(path: Path) -> dict[str, str]:
    parsed: dict[str, str] = {}
    if not path.exists():
        return parsed

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if "#" in value:
            value = value.split("#", 1)[0].rstrip()
        if value.startswith(('"', "'")) and value.endswith(('"', "'")):
            value = value[1:-1]
        parsed[key.strip()] = value.strip()

    return parsed


def get_config_value(env_file_values: dict[str, str], name: str, default: str | None = None) -> str:
    if name in os.environ:
        return os.environ[name]
    if name in env_file_values:
        return env_file_values[name]
    if default is not None:
        return default
    raise KeyError(f"Missing required configuration value: {name}")


def ensure_bucket(client: Any, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError:
        pass

    client.create_bucket(Bucket=bucket)


def iter_local_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*") if path.is_file())


def load_manifest_file_list(manifest_path: Path, dataset: str, mode: str) -> list[Path]:
    if not manifest_path.exists():
        return []

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []

    if payload.get("dataset") != dataset or payload.get("mode") != mode:
        return []

    files: list[Path] = []
    for item in payload.get("files", []):
        local_path = item.get("local_path")
        if not local_path:
            continue
        path = Path(local_path)
        if path.exists() and path.is_file():
            files.append(path)

    # Keep deterministic order and deduplicate when manifest contains duplicates.
    return sorted(set(files))


def object_exists_with_same_size(client: Any, bucket: str, key: str, expected_size: int) -> bool:
    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise

    return int(response["ContentLength"]) == expected_size


def summarize_prefix(client: Any, bucket: str, prefix: str) -> tuple[int, int]:
    paginator = client.get_paginator("list_objects_v2")
    total_bytes = 0
    total_objects = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page.get("Contents", []):
            total_objects += 1
            total_bytes += int(content.get("Size", 0))

    return total_objects, total_bytes


def list_prefix_objects(client: Any, bucket: str, prefix: str) -> dict[str, int]:
    paginator = client.get_paginator("list_objects_v2")
    objects: dict[str, int] = {}

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page.get("Contents", []):
            key = content.get("Key")
            if not key:
                continue
            objects[key] = int(content.get("Size", 0))

    return objects


def chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def human_size(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    amount = float(value)
    for unit in units:
        if amount < 1024.0 or unit == units[-1]:
            return f"{amount:.2f} {unit}"
        amount /= 1024.0
    return f"{value} B"


def main() -> int:
    args = parse_args()
    env_values = parse_env_file(Path(args.env_file))

    minio_host = get_config_value(env_values, "GP2__MINIO__HOST_NAME", "127.0.0.1")
    minio_local_port = get_config_value(env_values, "GP2__MINIO__API__LOCAL_PORT", "9000")
    endpoint_url = os.environ.get(
        "MINIO_ENDPOINT_URL",
        f"http://127.0.0.1:{minio_local_port}" if minio_host == "minio" else f"http://{minio_host}:{minio_local_port}",
    )

    access_key = get_config_value(env_values, "GP2__MINIO__USER")
    secret_key = get_config_value(env_values, "GP2__MINIO__PASSWORD")

    data_root = Path(args.data_dir) / args.dataset / args.mode
    if not data_root.exists():
        raise SystemExit(f"Local data path does not exist: {data_root}")

    manifest_path = Path(args.manifest_path) if args.manifest_path else Path(f"etl/manifests/{args.dataset}_files.json")
    files = load_manifest_file_list(manifest_path, args.dataset, args.mode)
    if not files:
        files = iter_local_files(data_root)
    if not files:
        raise SystemExit(f"No files found under: {data_root}")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    ensure_bucket(client, args.bucket)

    uploaded = 0
    skipped = 0
    uploaded_bytes = 0
    deleted = 0
    deleted_bytes = 0
    prefix = f"{args.prefix_base}/{args.dataset}/{args.mode}/"
    expected_keys: set[str] = set()

    for file_path in files:
        relative = file_path.relative_to(data_root).as_posix()
        object_key = f"{prefix}{relative}"
        expected_keys.add(object_key)
        size = file_path.stat().st_size

        if object_exists_with_same_size(client, args.bucket, object_key, size):
            skipped += 1
            print(f"SKIP  {object_key} ({size} bytes)")
            continue

        client.upload_file(str(file_path), args.bucket, object_key)
        uploaded += 1
        uploaded_bytes += size
        print(f"UPLOAD {object_key} ({size} bytes)")

    if args.prune_extra:
        remote_objects = list_prefix_objects(client, args.bucket, prefix)
        stale_keys = sorted(set(remote_objects.keys()) - expected_keys)
        if stale_keys:
            for keys_batch in chunked(stale_keys, 1000):
                delete_payload = {"Objects": [{"Key": key} for key in keys_batch], "Quiet": True}
                client.delete_objects(Bucket=args.bucket, Delete=delete_payload)
            deleted = len(stale_keys)
            deleted_bytes = sum(remote_objects[key] for key in stale_keys)
            for key in stale_keys:
                print(f"DELETE {key} ({remote_objects[key]} bytes)")

    total_objects, total_bytes = summarize_prefix(client, args.bucket, prefix)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = Path("etl/reports") / f"raw_upload_{timestamp}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    report_lines = [
        "# Raw Upload Report",
        "",
        f"- Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}",
        f"- Mode: {args.mode}",
        f"- Dataset: {args.dataset}",
        f"- Bucket: {args.bucket}",
        f"- Prefix: {prefix}",
        f"- Endpoint: {endpoint_url}",
        f"- Uploaded objects this run: {uploaded}",
        f"- Skipped existing objects this run: {skipped}",
        f"- Uploaded bytes this run: {uploaded_bytes} ({human_size(uploaded_bytes)})",
        f"- Deleted stale objects this run: {deleted}",
        f"- Deleted stale bytes this run: {deleted_bytes} ({human_size(deleted_bytes)})",
        f"- Total objects under prefix: {total_objects}",
        f"- Total bytes under prefix: {total_bytes} ({human_size(total_bytes)})",
    ]
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print(f"Report written: {report_path}")
    print(f"Prefix total: {total_objects} objects, {human_size(total_bytes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
