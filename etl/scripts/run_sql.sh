#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-sample}"
DATASET="${DATASET:-open_food_facts}"
TRINO_SERVICE="${TRINO_SERVICE:-trino-coordinator-and-worker}"
TRINO_URL="${TRINO_URL:-http://localhost:8080}"
SQL_DIR="${SQL_DIR:-etl/sql}"
TRINO_READY_ATTEMPTS="${TRINO_READY_ATTEMPTS:-60}"
TRINO_READY_SLEEP_SECONDS="${TRINO_READY_SLEEP_SECONDS:-2}"

if [[ "$MODE" != "sample" && "$MODE" != "full" ]]; then
  echo "ERROR: MODE must be 'sample' or 'full' (got: $MODE)" >&2
  exit 2
fi

RAW_LAYOUT="$MODE"

DEFAULT_SQL_FILES=(
  "$SQL_DIR/00_setup_trino.sql"
  "$SQL_DIR/01_raw_external_tables.sql"
  "$SQL_DIR/02_iceberg_ingest.sql"
  "$SQL_DIR/03_transform_core.sql"
  "$SQL_DIR/04_marts.sql"
  "$SQL_DIR/05_analytics_examples.sql"
)

if [[ "$#" -gt 0 ]]; then
  SQL_FILES=("$@")
else
  SQL_FILES=("${DEFAULT_SQL_FILES[@]}")
fi

echo "Running SQL pipeline"
echo "  MODE=$MODE"
echo "  DATASET=$DATASET"
echo "  TRINO_SERVICE=$TRINO_SERVICE"

wait_for_trino_ready() {
  local attempt=1
  while [[ "$attempt" -le "$TRINO_READY_ATTEMPTS" ]]; do
    if docker compose exec -T "$TRINO_SERVICE" trino "$TRINO_URL" --execute "SELECT 1" >/dev/null 2>&1; then
      echo "Trino is ready."
      return 0
    fi
    echo "Waiting for Trino readiness ($attempt/$TRINO_READY_ATTEMPTS)..."
    sleep "$TRINO_READY_SLEEP_SECONDS"
    attempt=$((attempt + 1))
  done

  echo "ERROR: Trino is not ready after $TRINO_READY_ATTEMPTS attempts." >&2
  return 1
}

render_sql() {
  local source_file="$1"
  local rendered_file="$2"

  if command -v envsubst >/dev/null 2>&1; then
    DATASET="$DATASET" MODE="$MODE" RAW_LAYOUT="$RAW_LAYOUT" envsubst '${DATASET} ${MODE} ${RAW_LAYOUT}' < "$source_file" > "$rendered_file"
  else
    sed \
      -e "s|\${DATASET}|$DATASET|g" \
      -e "s|\${MODE}|$MODE|g" \
      -e "s|\${RAW_LAYOUT}|$RAW_LAYOUT|g" \
      "$source_file" > "$rendered_file"
  fi
}

wait_for_trino_ready

for sql_file in "${SQL_FILES[@]}"; do
  if [[ ! -f "$sql_file" ]]; then
    echo "ERROR: SQL file not found: $sql_file" >&2
    exit 1
  fi

  printf '\n=== Executing: %s ===\n' "$sql_file"
  tmp_file="$(mktemp /tmp/trino-sql-XXXXXX)"
  render_sql "$sql_file" "$tmp_file"

  # Feed rendered SQL through stdin to Trino CLI running inside the container.
  cat "$tmp_file" | docker compose exec -T "$TRINO_SERVICE" trino "$TRINO_URL" --output-format ALIGNED
  rm -f "$tmp_file"
done

printf '\nSQL pipeline finished successfully.\n'
