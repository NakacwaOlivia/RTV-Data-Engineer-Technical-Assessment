import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class QualityCheckType(Enum):
    """Types of quality checks available."""
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"

@dataclass
class QualityCheckResult:
    """Results of a quality check."""
    check_type: QualityCheckType
    table_name: str
    column_name: Optional[str]
    metric_name: str
    metric_value: float
    threshold: float
    status: str
    details: Dict
    timestamp: datetime

class DataQualityValidator:
    """Validates data quality across the pipeline."""
    
    def __init__(self, db_conn: psycopg2.extensions.connection):
        """Initialize the data quality validator.
        
        Args:
            db_conn: PostgreSQL connection
        """
        self.conn = db_conn
        self._init_quality_tables()
        logger.info("Initialized DataQualityValidator")
    
    def _init_quality_tables(self) -> None:
        """Initialize tables for storing quality check results."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS quality_check_results (
                    check_id SERIAL PRIMARY KEY,
                    check_type VARCHAR(50),
                    table_name VARCHAR(100),
                    column_name VARCHAR(100),
                    metric_name VARCHAR(100),
                    metric_value FLOAT,
                    threshold FLOAT,
                    status VARCHAR(20),
                    details JSONB,
                    timestamp TIMESTAMP,
                    run_id INT
                );
                
                CREATE TABLE IF NOT EXISTS quality_check_runs (
                    run_id SERIAL PRIMARY KEY,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status VARCHAR(20),
                    metadata JSONB
                );
                
                CREATE INDEX IF NOT EXISTS idx_quality_checks_table 
                ON quality_check_results(table_name, check_type);
                
                CREATE INDEX IF NOT EXISTS idx_quality_checks_run 
                ON quality_check_results(run_id);
            """)
            self.conn.commit()
    
    def start_quality_run(self, metadata: Optional[Dict] = None) -> int:
        """Start a new quality check run.
        
        Args:
            metadata: Additional metadata for the run
            
        Returns:
            int: Run ID
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO quality_check_runs 
                    (start_time, status, metadata)
                    VALUES (CURRENT_TIMESTAMP, 'running', %s)
                    RETURNING run_id
                """, (Json(metadata) if metadata else None,))
                run_id = cur.fetchone()[0]
                self.conn.commit()
                logger.info(f"Started quality check run {run_id}")
                return run_id
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to start quality run: {e}")
            raise
    
    def end_quality_run(self, run_id: int, status: str = 'completed') -> None:
        """End a quality check run.
        
        Args:
            run_id: ID of the run to end
            status: Final status of the run
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE quality_check_runs 
                    SET end_time = CURRENT_TIMESTAMP,
                        status = %s
                    WHERE run_id = %s
                """, (status, run_id))
                self.conn.commit()
                logger.info(f"Ended quality check run {run_id} with status {status}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to end quality run {run_id}: {e}")
            raise
    
    def run_quality_checks(self, 
                          table_name: str, 
                          survey_year: int,
                          run_id: Optional[int] = None) -> List[QualityCheckResult]:
        """Run comprehensive quality checks on a table.
        
        Args:
            table_name: Name of the table to check
            survey_year: Year of the survey
            run_id: Optional run ID to associate checks with
            
        Returns:
            List[QualityCheckResult]: List of quality check results
        """
        results = []
        
        # Run all check types
        for check_type in QualityCheckType:
            check_func = getattr(self, f"_check_{check_type.value}")
            try:
                result = check_func(table_name, survey_year)
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)
            except Exception as e:
                logger.error(f"Failed to run {check_type.value} check: {e}")
        
        # Store results
        if run_id is not None:
            self._store_results(results, run_id)
        
        return results
    
    def _check_completeness(self, 
                           table_name: str, 
                           survey_year: int) -> List[QualityCheckResult]:
        """Check for missing values and required fields.
        
        Args:
            table_name: Name of the table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: List of completeness check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Get column names and types
                cur.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                """, (table_name,))
                columns = cur.fetchall()
                
                for col_name, data_type in columns:
                    # Check for NULL values
                    cur.execute(f"""
                        SELECT 
                            COUNT(*) as total_rows,
                            COUNT(*) FILTER (WHERE {col_name} IS NULL) as null_count,
                            COUNT(*) FILTER (WHERE {col_name} = -98 OR {col_name} = -99) as special_null_count
                        FROM {table_name}
                        WHERE survey_year = %s
                    """, (survey_year,))
                    total, nulls, special_nulls = cur.fetchone()
                    
                    if total > 0:
                        null_rate = (nulls + special_nulls) / total
                        results.append(QualityCheckResult(
                            check_type=QualityCheckType.COMPLETENESS,
                            table_name=table_name,
                            column_name=col_name,
                            metric_name="null_rate",
                            metric_value=null_rate,
                            threshold=0.1,  # 10% threshold
                            status="pass" if null_rate <= 0.1 else "fail",
                            details={
                                "total_rows": total,
                                "null_count": nulls,
                                "special_null_count": special_nulls
                            },
                            timestamp=datetime.now()
                        ))
        
        except Exception as e:
            logger.error(f"Failed to run completeness check: {e}")
        
        return results
    
    def _check_consistency(self, 
                          table_name: str, 
                          survey_year: int) -> List[QualityCheckResult]:
        """Check for data consistency across related tables.
        
        Args:
            table_name: Name of the table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: List of consistency check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Check household ID consistency
                if table_name == "fact_survey":
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_rows,
                            COUNT(*) FILTER (WHERE h.hhid_2 IS NULL) as missing_hh
                        FROM fact_survey f
                        LEFT JOIN dim_household h ON f.hhid_2 = h.hhid_2
                        WHERE f.survey_year = %s
                    """, (survey_year,))
                    total, missing = cur.fetchone()
                    
                    if total > 0:
                        missing_rate = missing / total
                        results.append(QualityCheckResult(
                            check_type=QualityCheckType.CONSISTENCY,
                            table_name=table_name,
                            column_name="hhid_2",
                            metric_name="household_consistency",
                            metric_value=missing_rate,
                            threshold=0.0,  # No missing households allowed
                            status="pass" if missing_rate == 0 else "fail",
                            details={
                                "total_rows": total,
                                "missing_households": missing
                            },
                            timestamp=datetime.now()
                        ))
                
                # Add more consistency checks as needed
        
        except Exception as e:
            logger.error(f"Failed to run consistency check: {e}")
        
        return results
    
    def _check_accuracy(self, 
                       table_name: str, 
                       survey_year: int) -> List[QualityCheckResult]:
        """Check for data accuracy and validity.
        
        Args:
            table_name: Name of the table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: List of accuracy check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Check for negative values in numeric columns
                cur.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                    AND data_type IN ('integer', 'numeric', 'decimal')
                """, (table_name,))
                numeric_cols = cur.fetchall()
                
                for col_name, _ in numeric_cols:
                    cur.execute(f"""
                        SELECT 
                            COUNT(*) as total_rows,
                            COUNT(*) FILTER (WHERE {col_name} < 0) as negative_count
                        FROM {table_name}
                        WHERE survey_year = %s
                    """, (survey_year,))
                    total, negatives = cur.fetchone()
                    
                    if total > 0:
                        negative_rate = negatives / total
                        results.append(QualityCheckResult(
                            check_type=QualityCheckType.ACCURACY,
                            table_name=table_name,
                            column_name=col_name,
                            metric_name="negative_value_rate",
                            metric_value=negative_rate,
                            threshold=0.0,  # No negative values allowed
                            status="pass" if negative_rate == 0 else "fail",
                            details={
                                "total_rows": total,
                                "negative_count": negatives
                            },
                            timestamp=datetime.now()
                        ))
        
        except Exception as e:
            logger.error(f"Failed to run accuracy check: {e}")
        
        return results
    
    def _check_timeliness(self, 
                         table_name: str, 
                         survey_year: int) -> QualityCheckResult:
        """Check data timeliness.
        
        Args:
            table_name: Name of the table to check
            survey_year: Year of the survey
            
        Returns:
            QualityCheckResult: Timeliness check result
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"""
                    SELECT 
                        MAX(submission_date) as latest_submission,
                        COUNT(*) as total_rows
                    FROM {table_name}
                    WHERE survey_year = %s
                """, (survey_year,))
                latest_submission, total = cur.fetchone()
                
                if latest_submission:
                    days_since_update = (datetime.now() - latest_submission).days
                    return QualityCheckResult(
                        check_type=QualityCheckType.TIMELINESS,
                        table_name=table_name,
                        column_name=None,
                        metric_name="days_since_update",
                        metric_value=days_since_update,
                        threshold=30,  # 30 days threshold
                        status="pass" if days_since_update <= 30 else "fail",
                        details={
                            "latest_submission": latest_submission.isoformat(),
                            "total_rows": total
                        },
                        timestamp=datetime.now()
                    )
        
        except Exception as e:
            logger.error(f"Failed to run timeliness check: {e}")
        
        return None
    
    def _check_uniqueness(self, 
                         table_name: str, 
                         survey_year: int) -> List[QualityCheckResult]:
        """Check for unique constraints.
        
        Args:
            table_name: Name of the table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: List of uniqueness check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Get primary key columns
                cur.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid 
                        AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass
                    AND i.indisprimary
                """, (table_name,))
                pk_columns = [row[0] for row in cur.fetchall()]
                
                if pk_columns:
                    # Check for duplicates
                    pk_cols_str = ", ".join(pk_columns)
                    cur.execute(f"""
                        SELECT {pk_cols_str}, COUNT(*) as count
                        FROM {table_name}
                        WHERE survey_year = %s
                        GROUP BY {pk_cols_str}
                        HAVING COUNT(*) > 1
                    """, (survey_year,))
                    duplicates = cur.fetchall()
                    
                    if duplicates:
                        results.append(QualityCheckResult(
                            check_type=QualityCheckType.UNIQUENESS,
                            table_name=table_name,
                            column_name=pk_cols_str,
                            metric_name="duplicate_count",
                            metric_value=len(duplicates),
                            threshold=0,  # No duplicates allowed
                            status="fail",
                            details={
                                "duplicate_keys": [dict(zip(pk_columns, dup[:-1])) 
                                                 for dup in duplicates],
                                "duplicate_counts": [dup[-1] for dup in duplicates]
                            },
                            timestamp=datetime.now()
                        ))
        
        except Exception as e:
            logger.error(f"Failed to run uniqueness check: {e}")
        
        return results
    
    def _store_results(self, 
                      results: List[QualityCheckResult], 
                      run_id: int) -> None:
        """Store quality check results in the database.
        
        Args:
            results: List of quality check results to store
            run_id: Run ID to associate results with
        """
        try:
            with self.conn.cursor() as cur:
                for result in results:
                    cur.execute("""
                        INSERT INTO quality_check_results 
                        (check_type, table_name, column_name, metric_name,
                         metric_value, threshold, status, details, timestamp, run_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        result.check_type.value,
                        result.table_name,
                        result.column_name,
                        result.metric_name,
                        result.metric_value,
                        result.threshold,
                        result.status,
                        Json(result.details),
                        result.timestamp,
                        run_id
                    ))
                self.conn.commit()
                logger.info(f"Stored {len(results)} quality check results for run {run_id}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to store quality check results: {e}")
            raise
    
    def get_quality_summary(self, 
                           table_name: str, 
                           survey_year: int,
                           run_id: Optional[int] = None) -> Dict:
        """Get a summary of quality check results.
        
        Args:
            table_name: Name of the table to summarize
            survey_year: Year of the survey
            run_id: Optional run ID to filter by
            
        Returns:
            Dict: Summary of quality check results
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        check_type,
                        COUNT(*) as total_checks,
                        COUNT(*) FILTER (WHERE status = 'pass') as passed_checks,
                        COUNT(*) FILTER (WHERE status = 'fail') as failed_checks,
                        AVG(CASE WHEN status = 'fail' THEN metric_value ELSE NULL END) as avg_failure_value
                    FROM quality_check_results
                    WHERE table_name = %s
                    AND (%s IS NULL OR run_id = %s)
                    GROUP BY check_type
                """, (table_name, run_id, run_id))
                
                summary = {
                    row[0]: {
                        "total_checks": row[1],
                        "passed_checks": row[2],
                        "failed_checks": row[3],
                        "pass_rate": row[2] / row[1] if row[1] > 0 else 0,
                        "avg_failure_value": row[4]
                    }
                    for row in cur.fetchall()
                }
                
                return summary
                
        except Exception as e:
            logger.error(f"Failed to get quality summary: {e}")
            return {} 