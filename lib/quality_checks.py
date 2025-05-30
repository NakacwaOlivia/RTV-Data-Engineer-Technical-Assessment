import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import psycopg2
from psycopg2.extras import Json
import pandas as pd
import numpy as np
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class QualityCheckType(Enum):
    """Types of quality checks."""
    COMPLETENESS = "COMPLETENESS"
    CONSISTENCY = "CONSISTENCY"
    ACCURACY = "ACCURACY"
    TIMELINESS = "TIMELINESS"
    UNIQUENESS = "UNIQUENESS"
    VALIDATION = "VALIDATION"
    DISTRIBUTION = "DISTRIBUTION"
    RELATIONSHIP = "RELATIONSHIP"

@dataclass
class QualityCheckResult:
    """Represents the result of a quality check."""
    check_type: QualityCheckType
    table_name: str
    check_name: str
    status: str
    details: Dict[str, Any]
    error_count: int
    error_rate: float
    error_details: Optional[List[Dict]] = None

class SurveyDataQualityValidator:
    """Validates quality of survey data with specific checks."""
    
    def __init__(self, db_conn: psycopg2.extensions.connection):
        """Initialize the quality validator.
        
        Args:
            db_conn: PostgreSQL connection
        """
        self.conn = db_conn
        self._init_quality_tables()
        logger.info("Initialized SurveyDataQualityValidator")
    
    def _init_quality_tables(self) -> None:
        """Initialize tables for storing quality check results."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS quality_check_runs (
                    run_id SERIAL PRIMARY KEY,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status VARCHAR(20),
                    survey_year INTEGER,
                    metadata JSONB
                );
                
                CREATE TABLE IF NOT EXISTS quality_check_results (
                    result_id SERIAL PRIMARY KEY,
                    run_id INTEGER REFERENCES quality_check_runs(run_id),
                    check_type VARCHAR(50),
                    table_name VARCHAR(100),
                    check_name VARCHAR(255),
                    status VARCHAR(20),
                    details JSONB,
                    error_count INTEGER,
                    error_rate FLOAT,
                    error_details JSONB,
                    created_at TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS quality_thresholds (
                    threshold_id SERIAL PRIMARY KEY,
                    check_type VARCHAR(50),
                    table_name VARCHAR(100),
                    check_name VARCHAR(255),
                    threshold_value FLOAT,
                    severity VARCHAR(20),
                    description TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_quality_runs_year 
                ON quality_check_runs(survey_year);
                
                CREATE INDEX IF NOT EXISTS idx_quality_results_run 
                ON quality_check_results(run_id);
            """)
            self.conn.commit()
    
    def start_quality_run(self,
                         survey_year: int,
                         metadata: Optional[Dict] = None) -> int:
        """Start a new quality check run.
        
        Args:
            survey_year: Year of the survey
            metadata: Additional metadata
            
        Returns:
            int: Run ID
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO quality_check_runs
                    (start_time, status, survey_year, metadata)
                    VALUES (%s, %s, %s, %s)
                    RETURNING run_id
                """, (
                    datetime.now(),
                    "RUNNING",
                    survey_year,
                    Json(metadata or {})
                ))
                run_id = cur.fetchone()[0]
                self.conn.commit()
                logger.info(f"Started quality run {run_id} for {survey_year}")
                return run_id
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to start quality run: {e}")
            raise
    
    def end_quality_run(self,
                       run_id: int,
                       status: str = "COMPLETED",
                       error_message: Optional[str] = None) -> None:
        """End a quality check run.
        
        Args:
            run_id: ID of the run to end
            status: Final status
            error_message: Optional error message
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE quality_check_runs
                    SET end_time = %s,
                        status = %s,
                        metadata = metadata || %s
                    WHERE run_id = %s
                """, (
                    datetime.now(),
                    status,
                    Json({"error_message": error_message} if error_message else {}),
                    run_id
                ))
                self.conn.commit()
                logger.info(f"Ended quality run {run_id} with status {status}")
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to end quality run: {e}")
            raise
    
    def run_quality_checks(self,
                          run_id: int,
                          table_name: str) -> List[QualityCheckResult]:
        """Run all quality checks for a table.
        
        Args:
            run_id: ID of the quality run
            table_name: Name of the table to check
            
        Returns:
            List[QualityCheckResult]: Results of all checks
        """
        try:
            results = []
            
            # Get survey year
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT survey_year
                    FROM quality_check_runs
                    WHERE run_id = %s
                """, (run_id,))
                survey_year = cur.fetchone()[0]
            
            # Run all checks
            results.extend(self._check_completeness(run_id, table_name, survey_year))
            results.extend(self._check_consistency(run_id, table_name, survey_year))
            results.extend(self._check_accuracy(run_id, table_name, survey_year))
            results.extend(self._check_validation(run_id, table_name, survey_year))
            results.extend(self._check_distribution(run_id, table_name, survey_year))
            results.extend(self._check_relationships(run_id, table_name, survey_year))
            
            # Store results
            self._store_results(results)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to run quality checks: {e}")
            raise
    
    def _check_completeness(self,
                           run_id: int,
                           table_name: str,
                           survey_year: int) -> List[QualityCheckResult]:
        """Check data completeness.
        
        Args:
            run_id: Quality run ID
            table_name: Table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: Completeness check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Check null rates
                cur.execute(f"""
                    SELECT 
                        column_name,
                        COUNT(*) as total,
                        COUNT(*) FILTER (WHERE {table_name}.{column_name} IS NULL) as null_count
                    FROM {table_name}
                    WHERE survey_year = %s
                    GROUP BY column_name
                """, (survey_year,))
                
                for row in cur.fetchall():
                    column, total, null_count = row
                    null_rate = null_count / total if total > 0 else 0
                    
                    results.append(QualityCheckResult(
                        check_type=QualityCheckType.COMPLETENESS,
                        table_name=table_name,
                        check_name=f"null_rate_{column}",
                        status="FAIL" if null_rate > 0.1 else "PASS",
                        details={
                            "column": column,
                            "total": total,
                            "null_count": null_count,
                            "null_rate": null_rate
                        },
                        error_count=null_count,
                        error_rate=null_rate
                    ))
                
                # Check required fields
                required_fields = {
                    "fact_survey": ["hhid_2", "survey_year", "district"],
                    "fact_expenditure": ["hhid_2", "survey_year", "expenditure_type"],
                    "fact_crop_yield": ["hhid_2", "survey_year", "crop_type"]
                }.get(table_name, [])
                
                for field in required_fields:
                    cur.execute(f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE survey_year = %s
                        AND {field} IS NULL
                    """, (survey_year,))
                    null_count = cur.fetchone()[0]
                    
                    cur.execute(f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE survey_year = %s
                    """, (survey_year,))
                    total = cur.fetchone()[0]
                    
                    error_rate = null_count / total if total > 0 else 0
                    results.append(QualityCheckResult(
                        check_type=QualityCheckType.COMPLETENESS,
                        table_name=table_name,
                        check_name=f"required_field_{field}",
                        status="FAIL" if null_count > 0 else "PASS",
                        details={
                            "field": field,
                            "total": total,
                            "null_count": null_count,
                            "error_rate": error_rate
                        },
                        error_count=null_count,
                        error_rate=error_rate
                    ))
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to check completeness: {e}")
            return []
    
    def _check_consistency(self,
                          run_id: int,
                          table_name: str,
                          survey_year: int) -> List[QualityCheckResult]:
        """Check data consistency.
        
        Args:
            run_id: Quality run ID
            table_name: Table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: Consistency check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Check value consistency
                consistency_rules = {
                    "fact_survey": [
                        ("hhh_gender", "IN ('M', 'F')"),
                        ("hhh_educ_level", "IN ('None', 'Primary', 'Secondary', 'Tertiary')"),
                        ("Material_walls", "IN ('Mud', 'Brick', 'Block', 'Other')")
                    ],
                    "fact_expenditure": [
                        ("expenditure_type", "IN ('Food', 'Education', 'Health', 'Other')"),
                        ("amount", ">= 0")
                    ],
                    "fact_crop_yield": [
                        ("crop_type", "IN ('Maize', 'Rice', 'Beans', 'Other')"),
                        ("yield_amount", ">= 0"),
                        ("area_planted", "> 0")
                    ]
                }.get(table_name, [])
                
                for column, rule in consistency_rules:
                    cur.execute(f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE survey_year = %s
                        AND NOT ({column} {rule})
                    """, (survey_year,))
                    invalid_count = cur.fetchone()[0]
                    
                    cur.execute(f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE survey_year = %s
                    """, (survey_year,))
                    total = cur.fetchone()[0]
                    
                    error_rate = invalid_count / total if total > 0 else 0
                    results.append(QualityCheckResult(
                        check_type=QualityCheckType.CONSISTENCY,
                        table_name=table_name,
                        check_name=f"value_consistency_{column}",
                        status="FAIL" if invalid_count > 0 else "PASS",
                        details={
                            "column": column,
                            "rule": rule,
                            "total": total,
                            "invalid_count": invalid_count,
                            "error_rate": error_rate
                        },
                        error_count=invalid_count,
                        error_rate=error_rate
                    ))
                
                # Check referential integrity
                if table_name == "fact_survey":
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_survey s
                        LEFT JOIN dim_household h 
                        ON s.hhid_2 = h.hhid_2
                        WHERE s.survey_year = %s
                        AND h.hhid_2 IS NULL
                    """, (survey_year,))
                    orphan_count = cur.fetchone()[0]
                    
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_survey
                        WHERE survey_year = %s
                    """, (survey_year,))
                    total = cur.fetchone()[0]
                    
                    error_rate = orphan_count / total if total > 0 else 0
                    results.append(QualityCheckResult(
                        check_type=QualityCheckType.CONSISTENCY,
                        table_name=table_name,
                        check_name="referential_integrity_household",
                        status="FAIL" if orphan_count > 0 else "PASS",
                        details={
                            "total": total,
                            "orphan_count": orphan_count,
                            "error_rate": error_rate
                        },
                        error_count=orphan_count,
                        error_rate=error_rate
                    ))
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to check consistency: {e}")
            return []
    
    def _check_accuracy(self,
                       run_id: int,
                       table_name: str,
                       survey_year: int) -> List[QualityCheckResult]:
        """Check data accuracy.
        
        Args:
            run_id: Quality run ID
            table_name: Table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: Accuracy check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Check for outliers
                outlier_checks = {
                    "fact_survey": [
                        ("asp_actual_income", 3.0),  # 3 standard deviations
                        ("hh_size", 3.0)
                    ],
                    "fact_expenditure": [
                        ("amount", 3.0)
                    ],
                    "fact_crop_yield": [
                        ("yield_amount", 3.0),
                        ("area_planted", 3.0)
                    ]
                }.get(table_name, [])
                
                for column, std_dev in outlier_checks:
                    cur.execute(f"""
                        WITH stats AS (
                            SELECT 
                                AVG({column}) as mean,
                                STDDEV({column}) as stddev
                            FROM {table_name}
                            WHERE survey_year = %s
                            AND {column} IS NOT NULL
                        )
                        SELECT COUNT(*)
                        FROM {table_name}, stats
                        WHERE survey_year = %s
                        AND {column} IS NOT NULL
                        AND ABS({column} - mean) > %s * stddev
                    """, (survey_year, survey_year, std_dev))
                    outlier_count = cur.fetchone()[0]
                    
                    cur.execute(f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE survey_year = %s
                        AND {column} IS NOT NULL
                    """, (survey_year,))
                    total = cur.fetchone()[0]
                    
                    error_rate = outlier_count / total if total > 0 else 0
                    results.append(QualityCheckResult(
                        check_type=QualityCheckType.ACCURACY,
                        table_name=table_name,
                        check_name=f"outliers_{column}",
                        status="WARNING" if error_rate > 0.01 else "PASS",
                        details={
                            "column": column,
                            "std_dev": std_dev,
                            "total": total,
                            "outlier_count": outlier_count,
                            "error_rate": error_rate
                        },
                        error_count=outlier_count,
                        error_rate=error_rate
                    ))
                
                # Check for logical inconsistencies
                if table_name == "fact_survey":
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_survey
                        WHERE survey_year = %s
                        AND hh_size > 0
                        AND hh_size < (
                            SELECT COUNT(*)
                            FROM fact_household_member
                            WHERE hhid_2 = fact_survey.hhid_2
                            AND survey_year = fact_survey.survey_year
                        )
                    """, (survey_year,))
                    inconsistent_count = cur.fetchone()[0]
                    
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_survey
                        WHERE survey_year = %s
                    """, (survey_year,))
                    total = cur.fetchone()[0]
                    
                    error_rate = inconsistent_count / total if total > 0 else 0
                    results.append(QualityCheckResult(
                        check_type=QualityCheckType.ACCURACY,
                        table_name=table_name,
                        check_name="logical_consistency_hh_size",
                        status="FAIL" if inconsistent_count > 0 else "PASS",
                        details={
                            "total": total,
                            "inconsistent_count": inconsistent_count,
                            "error_rate": error_rate
                        },
                        error_count=inconsistent_count,
                        error_rate=error_rate
                    ))
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to check accuracy: {e}")
            return []
    
    def _check_validation(self,
                         run_id: int,
                         table_name: str,
                         survey_year: int) -> List[QualityCheckResult]:
        """Check data validation rules.
        
        Args:
            run_id: Quality run ID
            table_name: Table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: Validation check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Check PPI indicators
                if table_name == "fact_survey":
                    ppi_checks = [
                        ("Material_walls", "IN ('Mud', 'Brick', 'Block', 'Other')"),
                        ("Fuel_source_cooking", "IN ('Wood', 'Charcoal', 'Electricity', 'Other')"),
                        ("Water_source", "IN ('Pipe', 'Well', 'River', 'Other')"),
                        ("Toilet_type", "IN ('None', 'Pit', 'Flush', 'Other')")
                    ]
                    
                    for column, rule in ppi_checks:
                        cur.execute(f"""
                            SELECT COUNT(*)
                            FROM {table_name}
                            WHERE survey_year = %s
                            AND {column} IS NOT NULL
                            AND NOT ({column} {rule})
                        """, (survey_year,))
                        invalid_count = cur.fetchone()[0]
                        
                        cur.execute(f"""
                            SELECT COUNT(*)
                            FROM {table_name}
                            WHERE survey_year = %s
                            AND {column} IS NOT NULL
                        """, (survey_year,))
                        total = cur.fetchone()[0]
                        
                        error_rate = invalid_count / total if total > 0 else 0
                        results.append(QualityCheckResult(
                            check_type=QualityCheckType.VALIDATION,
                            table_name=table_name,
                            check_name=f"ppi_validation_{column}",
                            status="FAIL" if invalid_count > 0 else "PASS",
                            details={
                                "column": column,
                                "rule": rule,
                                "total": total,
                                "invalid_count": invalid_count,
                                "error_rate": error_rate
                            },
                            error_count=invalid_count,
                            error_rate=error_rate
                        ))
                
                # Check expenditure validation
                if table_name == "fact_expenditure":
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_expenditure
                        WHERE survey_year = %s
                        AND amount < 0
                    """, (survey_year,))
                    negative_count = cur.fetchone()[0]
                    
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_expenditure
                        WHERE survey_year = %s
                    """, (survey_year,))
                    total = cur.fetchone()[0]
                    
                    error_rate = negative_count / total if total > 0 else 0
                    results.append(QualityCheckResult(
                        check_type=QualityCheckType.VALIDATION,
                        table_name=table_name,
                        check_name="expenditure_validation_negative",
                        status="FAIL" if negative_count > 0 else "PASS",
                        details={
                            "total": total,
                            "negative_count": negative_count,
                            "error_rate": error_rate
                        },
                        error_count=negative_count,
                        error_rate=error_rate
                    ))
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to check validation: {e}")
            return []
    
    def _check_distribution(self,
                           run_id: int,
                           table_name: str,
                           survey_year: int) -> List[QualityCheckResult]:
        """Check data distributions.
        
        Args:
            run_id: Quality run ID
            table_name: Table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: Distribution check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Check gender distribution
                if table_name == "fact_survey":
                    cur.execute("""
                        SELECT 
                            hhh_gender,
                            COUNT(*) as count,
                            COUNT(*)::float / SUM(COUNT(*)) OVER () as ratio
                        FROM fact_survey
                        WHERE survey_year = %s
                        GROUP BY hhh_gender
                    """, (survey_year,))
                    
                    gender_dist = {
                        row[0]: {"count": row[1], "ratio": row[2]}
                        for row in cur.fetchall()
                    }
                    
                    # Check if gender ratio is within expected range
                    expected_ratio = 0.5  # Expected ratio for each gender
                    tolerance = 0.2  # 20% tolerance
                    
                    for gender, stats in gender_dist.items():
                        if abs(stats["ratio"] - expected_ratio) > tolerance:
                            results.append(QualityCheckResult(
                                check_type=QualityCheckType.DISTRIBUTION,
                                table_name=table_name,
                                check_name=f"gender_distribution_{gender}",
                                status="WARNING",
                                details={
                                    "gender": gender,
                                    "count": stats["count"],
                                    "ratio": stats["ratio"],
                                    "expected_ratio": expected_ratio,
                                    "tolerance": tolerance
                                },
                                error_count=stats["count"],
                                error_rate=abs(stats["ratio"] - expected_ratio)
                            ))
                
                # Check education level distribution
                if table_name == "fact_survey":
                    cur.execute("""
                        SELECT 
                            hhh_educ_level,
                            COUNT(*) as count,
                            COUNT(*)::float / SUM(COUNT(*)) OVER () as ratio
                        FROM fact_survey
                        WHERE survey_year = %s
                        GROUP BY hhh_educ_level
                    """, (survey_year,))
                    
                    educ_dist = {
                        row[0]: {"count": row[1], "ratio": row[2]}
                        for row in cur.fetchall()
                    }
                    
                    # Check if education levels follow expected distribution
                    expected_ratios = {
                        "None": 0.2,
                        "Primary": 0.4,
                        "Secondary": 0.3,
                        "Tertiary": 0.1
                    }
                    tolerance = 0.1  # 10% tolerance
                    
                    for level, stats in educ_dist.items():
                        expected = expected_ratios.get(level, 0)
                        if abs(stats["ratio"] - expected) > tolerance:
                            results.append(QualityCheckResult(
                                check_type=QualityCheckType.DISTRIBUTION,
                                table_name=table_name,
                                check_name=f"education_distribution_{level}",
                                status="WARNING",
                                details={
                                    "level": level,
                                    "count": stats["count"],
                                    "ratio": stats["ratio"],
                                    "expected_ratio": expected,
                                    "tolerance": tolerance
                                },
                                error_count=stats["count"],
                                error_rate=abs(stats["ratio"] - expected)
                            ))
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to check distribution: {e}")
            return []
    
    def _check_relationships(self,
                            run_id: int,
                            table_name: str,
                            survey_year: int) -> List[QualityCheckResult]:
        """Check relationships between tables.
        
        Args:
            run_id: Quality run ID
            table_name: Table to check
            survey_year: Year of the survey
            
        Returns:
            List[QualityCheckResult]: Relationship check results
        """
        results = []
        try:
            with self.conn.cursor() as cur:
                # Check household-survey relationship
                if table_name == "fact_survey":
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_survey s
                        LEFT JOIN fact_expenditure e
                        ON s.hhid_2 = e.hhid_2
                        AND s.survey_year = e.survey_year
                        WHERE s.survey_year = %s
                        AND e.hhid_2 IS NULL
                    """, (survey_year,))
                    missing_exp_count = cur.fetchone()[0]
                    
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_survey
                        WHERE survey_year = %s
                    """, (survey_year,))
                    total = cur.fetchone()[0]
                    
                    error_rate = missing_exp_count / total if total > 0 else 0
                    results.append(QualityCheckResult(
                        check_type=QualityCheckType.RELATIONSHIP,
                        table_name=table_name,
                        check_name="relationship_survey_expenditure",
                        status="WARNING" if error_rate > 0.1 else "PASS",
                        details={
                            "total": total,
                            "missing_count": missing_exp_count,
                            "error_rate": error_rate
                        },
                        error_count=missing_exp_count,
                        error_rate=error_rate
                    ))
                
                # Check expenditure-crop relationship
                if table_name == "fact_expenditure":
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_expenditure e
                        LEFT JOIN fact_crop_yield c
                        ON e.hhid_2 = c.hhid_2
                        AND e.survey_year = c.survey_year
                        WHERE e.survey_year = %s
                        AND e.expenditure_type = 'Food'
                        AND c.hhid_2 IS NULL
                    """, (survey_year,))
                    missing_crop_count = cur.fetchone()[0]
                    
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_expenditure
                        WHERE survey_year = %s
                        AND expenditure_type = 'Food'
                    """, (survey_year,))
                    total = cur.fetchone()[0]
                    
                    error_rate = missing_crop_count / total if total > 0 else 0
                    results.append(QualityCheckResult(
                        check_type=QualityCheckType.RELATIONSHIP,
                        table_name=table_name,
                        check_name="relationship_expenditure_crop",
                        status="WARNING" if error_rate > 0.2 else "PASS",
                        details={
                            "total": total,
                            "missing_count": missing_crop_count,
                            "error_rate": error_rate
                        },
                        error_count=missing_crop_count,
                        error_rate=error_rate
                    ))
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to check relationships: {e}")
            return []
    
    def _store_results(self, results: List[QualityCheckResult]) -> None:
        """Store quality check results.
        
        Args:
            results: List of quality check results
        """
        try:
            with self.conn.cursor() as cur:
                for result in results:
                    cur.execute("""
                        INSERT INTO quality_check_results
                        (run_id, check_type, table_name, check_name,
                         status, details, error_count, error_rate,
                         error_details, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        result.run_id,
                        result.check_type.value,
                        result.table_name,
                        result.check_name,
                        result.status,
                        Json(result.details),
                        result.error_count,
                        result.error_rate,
                        Json(result.error_details) if result.error_details else None,
                        datetime.now()
                    ))
                self.conn.commit()
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to store results: {e}")
            raise
    
    def get_quality_summary(self,
                           run_id: int) -> Dict:
        """Get a summary of quality check results.
        
        Args:
            run_id: ID of the quality run
            
        Returns:
            Dict: Quality check summary
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        check_type,
                        COUNT(*) as total_checks,
                        COUNT(*) FILTER (WHERE status = 'PASS') as passed_checks,
                        COUNT(*) FILTER (WHERE status = 'FAIL') as failed_checks,
                        COUNT(*) FILTER (WHERE status = 'WARNING') as warning_checks,
                        AVG(error_rate) as avg_error_rate,
                        MAX(error_rate) as max_error_rate
                    FROM quality_check_results
                    WHERE run_id = %s
                    GROUP BY check_type
                """, (run_id,))
                
                summary = {
                    "by_type": {
                        row[0]: {
                            "total_checks": row[1],
                            "passed_checks": row[2],
                            "failed_checks": row[3],
                            "warning_checks": row[4],
                            "avg_error_rate": row[5],
                            "max_error_rate": row[6]
                        }
                        for row in cur.fetchall()
                    }
                }
                
                # Get overall summary
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_checks,
                        COUNT(*) FILTER (WHERE status = 'PASS') as passed_checks,
                        COUNT(*) FILTER (WHERE status = 'FAIL') as failed_checks,
                        COUNT(*) FILTER (WHERE status = 'WARNING') as warning_checks,
                        AVG(error_rate) as avg_error_rate,
                        MAX(error_rate) as max_error_rate
                    FROM quality_check_results
                    WHERE run_id = %s
                """, (run_id,))
                
                row = cur.fetchone()
                summary["overall"] = {
                    "total_checks": row[0],
                    "passed_checks": row[1],
                    "failed_checks": row[2],
                    "warning_checks": row[3],
                    "avg_error_rate": row[4],
                    "max_error_rate": row[5]
                }
                
                return summary
                
        except Exception as e:
            logger.error(f"Failed to get quality summary: {e}")
            return {}
    
    def get_failed_checks(self,
                         run_id: int,
                         check_type: Optional[QualityCheckType] = None) -> List[Dict]:
        """Get details of failed quality checks.
        
        Args:
            run_id: ID of the quality run
            check_type: Optional check type to filter by
            
        Returns:
            List[Dict]: Failed check details
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        check_type,
                        table_name,
                        check_name,
                        status,
                        details,
                        error_count,
                        error_rate,
                        error_details
                    FROM quality_check_results
                    WHERE run_id = %s
                    AND status IN ('FAIL', 'WARNING')
                    AND (%s IS NULL OR check_type = %s)
                    ORDER BY error_rate DESC
                """, (run_id, check_type.value if check_type else None, check_type.value if check_type else None))
                
                return [
                    {
                        "check_type": row[0],
                        "table_name": row[1],
                        "check_name": row[2],
                        "status": row[3],
                        "details": row[4],
                        "error_count": row[5],
                        "error_rate": row[6],
                        "error_details": row[7]
                    }
                    for row in cur.fetchall()
                ]
                
        except Exception as e:
            logger.error(f"Failed to get failed checks: {e}")
            return [] 