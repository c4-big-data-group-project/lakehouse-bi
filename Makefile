MODE ?= sample
DATASET ?= open_food_facts

up:
	@docker compose up --build -d

down:
	@docker compose down

etl-sample:
	@python3 etl/scripts/download_dataset.py --mode sample --clean-extra
	@python3 etl/scripts/upload_to_minio.py --mode sample --dataset $(DATASET) --prune-extra
	@MODE=sample DATASET=$(DATASET) ./etl/scripts/run_sql.sh

etl-full:
	@python3 etl/scripts/download_dataset.py --mode full --clean-extra
	@python3 etl/scripts/upload_to_minio.py --mode full --dataset $(DATASET) --prune-extra
	@MODE=full DATASET=$(DATASET) ./etl/scripts/run_sql.sh

etl-verify:
	@MODE=$(MODE) DATASET=$(DATASET) ./etl/scripts/run_sql.sh etl/sql/00_setup_trino.sql etl/sql/01_raw_external_tables.sql etl/sql/99_acceptance_checks.sql
