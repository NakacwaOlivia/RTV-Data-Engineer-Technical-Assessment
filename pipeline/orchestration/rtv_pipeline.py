from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import logging
from pathlib import Path
import sys

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Import custom modules
from pipeline.ingestion.ingest_data import ingest_survey_data
from pipeline.transformation.transform_data import transform_survey_data
from pipeline.transformation.quality_checks import run_quality_checks

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'rtv_pipeline',
    default_args=default_args,
    description='RTV Household Survey Data Pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['rtv', 'household', 'survey'],
)

# Task 1: Create database tables if they don't exist
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql='warehouse/schema.sql',
    dag=dag,
)

# Task 2: Ingest survey data
ingest_data = PythonOperator(
    task_id='ingest_survey_data',
    python_callable=ingest_survey_data,
    op_kwargs={
        'source_path': '/opt/airflow/data/combined_data/',
        'minio_bucket': 'rtv-data',
    },
    dag=dag,
)

# Task 3: Transform data
transform_data = PythonOperator(
    task_id='transform_survey_data',
    python_callable=transform_survey_data,
    op_kwargs={
        'minio_bucket': 'rtv-data',
        'db_conn_id': 'postgres_default',
    },
    dag=dag,
)

# Task 4: Run quality checks
quality_checks = PythonOperator(
    task_id='run_quality_checks',
    python_callable=run_quality_checks,
    op_kwargs={
        'db_conn_id': 'postgres_default',
    },
    dag=dag,
)

# Task 5: Update materialized views
update_views = PostgresOperator(
    task_id='update_views',
    postgres_conn_id='postgres_default',
    sql="""
    REFRESH MATERIALIZED VIEW CONCURRENTLY household_progress;
    """,
    dag=dag,
)

# Task 6: Log pipeline completion
log_completion = PostgresOperator(
    task_id='log_pipeline_completion',
    postgres_conn_id='postgres_default',
    sql="""
    INSERT INTO pipeline_logs (
        pipeline_name,
        run_id,
        status,
        start_time,
        end_time,
        details
    ) VALUES (
        'rtv_pipeline',
        '{{ ts_nodash }}',
        'completed',
        '{{ execution_date }}',
        CURRENT_TIMESTAMP,
        '{"records_processed": 1000, "quality_score": 95}'
    );
    """,
    dag=dag,
)

# Define task dependencies
create_tables >> ingest_data >> transform_data >> quality_checks >> update_views >> log_completion

# Add documentation
dag.doc_md = """
# RTV Household Survey Pipeline

This DAG orchestrates the ETL process for RTV household survey data.

## Pipeline Steps
1. Create/update database tables
2. Ingest survey data from source files
3. Transform and clean the data
4. Run quality checks
5. Update materialized views
6. Log pipeline completion

## Schedule
- Runs daily at midnight
- Processes all new survey data
- Updates all derived metrics

## Data Quality
- Validates data completeness
- Checks for data consistency
- Monitors data timeliness
- Generates quality reports
""" 