.PHONY: setup test lint run offline energy energy-entsoe dbt-build dbt-energy-build dbt-energy-bigquery dashboard cloud-plan k8s-apply clean

setup:
	python -m venv .venv
	. .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

test:
	pytest

lint:
	ruff check src tests streamlit_app

run:
	quant-alpha run

offline:
	quant-alpha run --offline

energy:
	quant-alpha energy-run

energy-entsoe:
	quant-alpha energy-run --source entsoe

dbt-build:
	cd dbt_quant_alpha && dbt build --profiles-dir .

dbt-energy-build:
	cd dbt_energy_alpha && dbt build --profiles-dir .

dbt-energy-bigquery:
	cd dbt_energy_alpha && dbt build --profiles-dir . --target bigquery

dashboard:
	streamlit run streamlit_app/app.py

cloud-plan:
	cd infra/terraform && terraform init && terraform plan

k8s-apply:
	kubectl apply -k infra/k8s/base

clean:
	rm -rf data/raw/*.parquet data/processed/*.parquet data/warehouse/*.duckdb
