from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import psycopg2
from minio import Minio
from lib.data_lake import DataLakeManager
from lib.schema_migration import SchemaMigrationManager
from lib.quality_checks import SurveyDataQualityValidator
from lib.ingestion import SurveyDataIngester
from lib.monitoring import PipelineMonitor

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_db_connection() -> psycopg2.extensions.connection:
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=Variable.get("POSTGRES_HOST"),
        database=Variable.get("POSTGRES_DB"),
        user=Variable.get("POSTGRES_USER"),
        password=Variable.get("POSTGRES_PASSWORD")
    )

def get_minio_client() -> Minio:
    """Get MinIO client."""
    return Minio(
        endpoint=Variable.get("MINIO_ENDPOINT"),
        access_key=Variable.get("MINIO_ACCESS_KEY"),
        secret_key=Variable.get("MINIO_SECRET_KEY"),
        secure=Variable.get("MINIO_SECURE", default_var=True)
    )

def initialize_components(**context) -> Dict:
    """Initialize pipeline components."""
    db_conn = get_db_connection()
    minio_client = get_minio_client()
    
    components = {
        'data_lake': DataLakeManager(db_conn, minio_client),
        'schema_manager': SchemaMigrationManager(db_conn),
        'quality_validator': SurveyDataQualityValidator(db_conn),
        'data_ingester': SurveyDataIngester(db_conn, minio_client),
        'pipeline_monitor': PipelineMonitor(db_conn)
    }
    
    # Start pipeline run
    run_id = components['pipeline_monitor'].start_pipeline_run(
        pipeline_name="survey_pipeline",
        metadata={
            "dag_run_id": context['dag_run'].run_id,
            "execution_date": context['execution_date'].isoformat()
        }
    )
    
    return {
        'components': components,
        'run_id': run_id,
        'db_conn': db_conn
    }

def ingest_survey_data(**context) -> None:
    """Ingest survey data with validation."""
    ti = context['task_instance']
    components = ti.xcom_pull(task_ids='initialize')['components']
    db_conn = ti.xcom_pull(task_ids='initialize')['db_conn']
    run_id = ti.xcom_pull(task_ids='initialize')['run_id']
    
    try:
        # Get survey year from DAG parameters
        survey_year = context['dag_run'].conf.get('survey_year')
        if not survey_year:
            raise ValueError("survey_year parameter is required")
        
        # Get file path from DAG parameters
        file_path = context['dag_run'].conf.get('file_path')
        if not file_path:
            raise ValueError("file_path parameter is required")
        
        # Ingest data
        lineage = components['data_ingester'].ingest_survey_data(
            file_path=file_path,
            survey_year=survey_year,
            metadata={
                "dag_run_id": context['dag_run'].run_id,
                "execution_date": context['execution_date'].isoformat()
            }
        )
        
        # Record metric
        components['pipeline_monitor'].record_metric(
            run_id=run_id,
            metric_name="records_ingested",
            metric_value=lineage.record_count if lineage else 0
        )
        
    except Exception as e:
        logger.error(f"Failed to ingest survey data: {e}")
        components['pipeline_monitor'].record_alert(
            run_id=run_id,
            alert_name="ingestion_failed",
            severity="ERROR",
            message=str(e)
        )
        raise
    finally:
        db_conn.close()

def run_quality_checks(**context) -> None:
    """Run quality checks on ingested data."""
    ti = context['task_instance']
    components = ti.xcom_pull(task_ids='initialize')['components']
    db_conn = ti.xcom_pull(task_ids='initialize')['db_conn']
    run_id = ti.xcom_pull(task_ids='initialize')['run_id']
    
    try:
        survey_year = context['dag_run'].conf.get('survey_year')
        
        # Start quality run
        quality_run_id = components['quality_validator'].start_quality_run(
            survey_year=survey_year,
            metadata={
                "dag_run_id": context['dag_run'].run_id,
                "execution_date": context['execution_date'].isoformat()
            }
        )
        
        # Run checks for each table
        tables = ["fact_survey", "fact_expenditure", "fact_crop_yield"]
        for table in tables:
            results = components['quality_validator'].run_quality_checks(
                run_id=quality_run_id,
                table_name=table
            )
            
            # Record metrics
            summary = components['quality_validator'].get_quality_summary(quality_run_id)
            components['pipeline_monitor'].record_metric(
                run_id=run_id,
                metric_name=f"quality_checks_{table}",
                metric_value=summary['overall']['passed_checks'] / summary['overall']['total_checks']
            )
            
            # Check for failed checks
            failed_checks = components['quality_validator'].get_failed_checks(quality_run_id)
            if failed_checks:
                components['pipeline_monitor'].record_alert(
                    run_id=run_id,
                    alert_name=f"quality_checks_failed_{table}",
                    severity="WARNING",
                    message=f"Found {len(failed_checks)} failed quality checks",
                    metadata={"failed_checks": failed_checks}
                )
        
        # End quality run
        components['quality_validator'].end_quality_run(quality_run_id)
        
    except Exception as e:
        logger.error(f"Failed to run quality checks: {e}")
        components['pipeline_monitor'].record_alert(
            run_id=run_id,
            alert_name="quality_checks_failed",
            severity="ERROR",
            message=str(e)
        )
        raise
    finally:
        db_conn.close()

def finalize_pipeline(**context) -> None:
    """Finalize pipeline run and record metrics."""
    ti = context['task_instance']
    components = ti.xcom_pull(task_ids='initialize')['components']
    db_conn = ti.xcom_pull(task_ids='initialize')['db_conn']
    run_id = ti.xcom_pull(task_ids='initialize')['run_id']
    
    try:
        # Get pipeline status
        status = components['pipeline_monitor'].get_pipeline_status(run_id)
        
        # Record final metrics
        components['pipeline_monitor'].record_metric(
            run_id=run_id,
            metric_name="pipeline_duration",
            metric_value=status['duration']
        )
        
        # End pipeline run
        components['pipeline_monitor'].end_pipeline_run(
            run_id=run_id,
            status="COMPLETED" if not status['failed_runs'] else "FAILED"
        )
        
    except Exception as e:
        logger.error(f"Failed to finalize pipeline: {e}")
        components['pipeline_monitor'].end_pipeline_run(
            run_id=run_id,
            status="FAILED",
            error_message=str(e)
        )
        raise
    finally:
        db_conn.close()

# Create the DAG
with DAG(
    'survey_pipeline',
    default_args=default_args,
    description='Enhanced survey data pipeline with quality checks and monitoring',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['survey', 'data-quality']
) as dag:
    
    # Initialize components
    init_task = PythonOperator(
        task_id='initialize',
        python_callable=initialize_components,
        provide_context=True
    )
    
    # Create task groups
    with TaskGroup(group_id='data_ingestion') as ingestion_group:
        ingest_task = PythonOperator(
            task_id='ingest_survey_data',
            python_callable=ingest_survey_data,
            provide_context=True
        )
    
    with TaskGroup(group_id='quality_validation') as quality_group:
        quality_task = PythonOperator(
            task_id='run_quality_checks',
            python_callable=run_quality_checks,
            provide_context=True
        )
    
    # Finalize pipeline
    finalize_task = PythonOperator(
        task_id='finalize',
        python_callable=finalize_pipeline,
        provide_context=True,
        trigger_rule='all_done'
    )
    
    # Set task dependencies
    init_task >> ingestion_group >> quality_group >> finalize_task 