"""
DAG: edfi_lakehouse_pipeline
Orchestrates the full Ed-Fi Interoperability Lakehouse pipeline:
1. Generate synthetic data (Ed-Fi XML + OneRoster CSV)
2. PySpark: Parse raw files, hash PII, load to DuckDB Silver
3. dbt: Transform Silver -> Gold (staging -> intermediate -> marts)
4. dbt: Run data quality tests
5. Export DuckDB for Streamlit
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 8, 1),
    "retries": 1,
}

with DAG(
    dag_id="edfi_lakehouse_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Full Bronze -> Silver -> Gold pipeline for multi-district K-12 data",
    tags=["edfi", "oneroster", "lakehouse"],
) as dag:

    generate_edfi = BashOperator(
        task_id="generate_edfi_data",
        bash_command=(
            "cd /opt/airflow && "
            "python -c 'from data_generation.generate_edfi_xml import generate_edfi_district; "
            'generate_edfi_district("data/bronze/edfi", num_students=5000)\''
        ),
    )

    generate_oneroster = BashOperator(
        task_id="generate_oneroster_data",
        bash_command=(
            "cd /opt/airflow && "
            "python -c 'from data_generation.generate_oneroster_csv import generate_oneroster_district; "
            'generate_oneroster_district("data/bronze/oneroster", num_students=3500)\''
        ),
    )

    parse_to_silver = BashOperator(
        task_id="spark_parse_to_silver",
        bash_command=(
            "cd /opt/airflow && "
            "python -m spark_jobs.run_bronze_to_silver"
        ),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt_project && dbt seed",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test",
    )

    export_for_streamlit = BashOperator(
        task_id="export_duckdb_for_streamlit",
        bash_command="cp /opt/airflow/data/lakehouse.duckdb /opt/airflow/streamlit_app/data/lakehouse.duckdb",
    )

    # DAG dependency chain
    [generate_edfi, generate_oneroster] >> parse_to_silver >> dbt_seed >> dbt_run >> dbt_test >> export_for_streamlit
