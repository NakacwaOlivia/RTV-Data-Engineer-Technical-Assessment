import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import pandas as pd
from minio import Minio
from minio.error import S3Error
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from lib.data_lake import DataLakeManager
from lib.schema import SchemaManager
from lib.quality import DataQualityValidator, QualityCheckResult
from lib.monitoring import PipelineMonitor, PipelineMetric, PipelineStatus

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
    """Get a database connection."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        database=os.getenv('POSTGRES_DB', 'survey_data'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
    )

def get_minio_client() -> Minio:
    """Get a MinIO client."""
    return Minio(
        endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    )

def ingest_survey_data(**context) -> None:
    """Ingest survey data from CSV files into the data lake and database.
    
    This task:
    1. Downloads CSV files from the data lake
    2. Validates the data against the schema
    3. Performs quality checks
    4. Loads the data into the database
    5. Records metrics and alerts
    """
    # Get execution date from context
    execution_date = context['execution_date']
    survey_year = execution_date.year
    
    # Initialize components
    db_conn = get_db_connection()
    minio_client = get_minio_client()
    data_lake = DataLakeManager(minio_client)
    schema_manager = SchemaManager(db_conn)
    quality_validator = DataQualityValidator(db_conn)
    pipeline_monitor = PipelineMonitor(db_conn)
    
    try:
        # Start pipeline run
        run_id = pipeline_monitor.start_pipeline_run(
            pipeline_name="survey_data_ingestion",
            metadata={
                "survey_year": survey_year,
                "execution_date": execution_date.isoformat()
            }
        )
        
        # Get latest survey data file
        survey_file = data_lake.get_latest_file(
            bucket_name="survey-data",
            prefix=f"raw/survey_{survey_year}"
        )
        
        if not survey_file:
            raise ValueError(f"No survey data found for year {survey_year}")
        
        # Download and read the file
        file_data = data_lake.get_file_content(survey_file.bucket_name, survey_file.object_name)
        df = pd.read_csv(file_data)
        
        # Record file metrics
        pipeline_monitor.record_metric(
            run_id=run_id,
            metric=PipelineMetric(
                metric_name="file_size_bytes",
                metric_value=len(file_data),
                metric_type="gauge",
                timestamp=datetime.now(),
                metadata={"file_name": survey_file.object_name}
            )
        )
        
        pipeline_monitor.record_metric(
            run_id=run_id,
            metric=PipelineMetric(
                metric_name="row_count",
                metric_value=len(df),
                metric_type="gauge",
                timestamp=datetime.now(),
                metadata={"file_name": survey_file.object_name}
            )
        )
        
        # Detect schema changes
        schema_changes = schema_manager.detect_schema_changes(
            table_name="fact_survey",
            survey_year=survey_year
        )
        
        if schema_changes["added"] or schema_changes["modified"]:
            pipeline_monitor.record_alert(
                run_id=run_id,
                alert_type="schema_change",
                alert_message=f"Schema changes detected: {schema_changes}",
                severity="warning",
                metadata=schema_changes
            )
        
        # Validate data against schema
        validation_results = schema_manager.validate_data_against_schema(
            table_name="fact_survey",
            data=df
        )
        
        if not validation_results["is_valid"]:
            pipeline_monitor.record_alert(
                run_id=run_id,
                alert_type="schema_validation",
                alert_message="Data validation failed against schema",
                severity="error",
                metadata=validation_results
            )
            raise ValueError("Data validation failed against schema")
        
        # Run quality checks
        quality_results = quality_validator.run_quality_checks(
            table_name="fact_survey",
            survey_year=survey_year,
            run_id=run_id
        )
        
        # Check for quality issues
        failed_checks = [
            result for result in quality_results
            if result.status == "fail"
        ]
        
        if failed_checks:
            pipeline_monitor.record_alert(
                run_id=run_id,
                alert_type="quality_check",
                alert_message=f"Quality checks failed: {len(failed_checks)} issues found",
                severity="error",
                metadata={
                    "failed_checks": [
                        {
                            "check_type": check.check_type.value,
                            "metric_name": check.metric_name,
                            "metric_value": check.metric_value,
                            "threshold": check.threshold,
                            "details": check.details
                        }
                        for check in failed_checks
                    ]
                }
            )
            raise ValueError("Quality checks failed")
        
        # Load data into database
        with db_conn.cursor() as cur:
            # Create staging table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS staging_survey_{survey_year} (
                    LIKE fact_survey INCLUDING ALL
                );
                
                TRUNCATE TABLE staging_survey_{survey_year};
            """)
            
            # Insert data
            execute_values(
                cur,
                f"""
                INSERT INTO staging_survey_{survey_year}
                ({', '.join(df.columns)})
                VALUES %s
                """,
                [tuple(x) for x in df.values]
            )
            
            # Merge into fact table
            cur.execute(f"""
                INSERT INTO fact_survey
                SELECT * FROM staging_survey_{survey_year}
                ON CONFLICT (survey_year, hhid_2) 
                DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP,
                    {', '.join(f"{col} = EXCLUDED.{col}" 
                             for col in df.columns 
                             if col not in ['survey_year', 'hhid_2'])}
            """)
            
            db_conn.commit()
        
        # Record success metrics
        pipeline_monitor.record_metric(
            run_id=run_id,
            metric=PipelineMetric(
                metric_name="load_duration_seconds",
                metric_value=(datetime.now() - execution_date).total_seconds(),
                metric_type="gauge",
                timestamp=datetime.now()
            )
        )
        
        # End pipeline run successfully
        pipeline_monitor.end_pipeline_run(
            run_id=run_id,
            status=PipelineStatus.COMPLETED
        )
        
    except Exception as e:
        # Record failure
        if 'run_id' in locals():
            pipeline_monitor.record_alert(
                run_id=run_id,
                alert_type="pipeline_error",
                alert_message=str(e),
                severity="critical"
            )
            pipeline_monitor.end_pipeline_run(
                run_id=run_id,
                status=PipelineStatus.FAILED,
                error_message=str(e)
            )
        raise
    
    finally:
        db_conn.close()

# Create the DAG
dag = DAG(
    'enhanced_survey_pipeline',
    default_args=default_args,
    description='Enhanced pipeline for survey data ingestion with quality checks and monitoring',
    schedule_interval='@yearly',
    start_date=days_ago(1),
    catchup=False,
    tags=['survey', 'data-quality', 'monitoring'],
)

# Define the ingestion task
ingest_task = PythonOperator(
    task_id='ingest_survey_data',
    python_callable=ingest_survey_data,
    provide_context=True,
    dag=dag,
) 