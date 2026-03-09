# lakehouse-bi

Group Project #2: Data Lakehouse + BI Assistant.

## Project layout

- `docs/infra.md` - infrastructure stack and local run instructions.
- `etl/README_data.md` - Person 2 pipeline (Open Food Facts): raw -> Iceberg -> marts.
- `docs/data_contract.md` - dataset contract and storage/model conventions.
- `docs/demo_script_data.md` - short demo flow for Part 3.2.
- `docs/data_dictionary_for_agent.md` - semantic handoff for Person 3 (BI agent).

## Quick start

```bash
cp .example.env .env
make up
python3 -m pip install -r etl/requirements.txt
make etl-sample
make etl-verify MODE=sample
```

Stop services:

```bash
make down
```

## Dataset

Current Person 2 implementation is based on Open Food Facts:

- Kaggle page: https://www.kaggle.com/datasets/konradb/open-food-facts
- Pipeline source URL: https://openfoodfacts-ds.s3.eu-west-3.amazonaws.com/en.openfoodfacts.org.products.csv.gz

`full` mode intentionally uses payload just above 1GB to satisfy course requirement 1.1 with minimal extra download volume.
