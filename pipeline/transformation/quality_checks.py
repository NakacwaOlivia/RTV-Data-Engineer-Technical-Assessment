import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import os
from sqlalchemy import create_engine, text
import json
from typing import Dict, List, Optional, Tuple
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityChecks:
    def __init__(self, db_conn_id: str):
        """
        Initialize the data quality checks process.
        
        Args:
            db_conn_id: Database connection identifier
        """
        self.db_conn_id = db_conn_id
        self.db_engine = self._setup_database()
        
    def _setup_database(self) -> create_engine:
        """Set up database connection."""
        try:
            engine = create_engine('postgresql://postgres:password@localhost:5432/airflow')
            return engine
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def _check_completeness(self) -> Dict[str, float]:
        """
        Check data completeness for all tables.
        
        Returns:
            Dict[str, float]: Completeness scores for each table
        """
        try:
            tables = ['households', 'household_surveys', 'household_measurements', 'indicators']
            scores = {}
            
            for table in tables:
                # Get total rows and non-null counts for each column
                query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    {', '.join([f'COUNT({col}) as {col}_count' for col in self._get_table_columns(table)])}
                FROM {table}
                """
                
                result = pd.read_sql(query, self.db_engine)
                
                # Calculate completeness score
                total_cells = result['total_rows'].iloc[0] * len(self._get_table_columns(table))
                non_null_cells = sum(result[col].iloc[0] for col in result.columns if col != 'total_rows')
                completeness = (non_null_cells / total_cells) * 100
                
                scores[table] = round(completeness, 2)
                
            return scores
        except Exception as e:
            logger.error(f"Error checking completeness: {e}")
            raise
            
    def _check_consistency(self) -> Dict[str, List[Dict]]:
        """
        Check data consistency across tables.
        
        Returns:
            Dict[str, List[Dict]]: Consistency issues found
        """
        try:
            issues = []
            
            # Check referential integrity
            checks = [
                {
                    'name': 'household_surveys_household_id',
                    'query': """
                    SELECT COUNT(*) as invalid_count
                    FROM household_surveys hs
                    LEFT JOIN households h ON hs.household_id = h.household_id
                    WHERE h.household_id IS NULL
                    """
                },
                {
                    'name': 'household_measurements_survey_id',
                    'query': """
                    SELECT COUNT(*) as invalid_count
                    FROM household_measurements hm
                    LEFT JOIN household_surveys hs ON hm.survey_id = hs.survey_id
                    WHERE hs.survey_id IS NULL
                    """
                },
                {
                    'name': 'household_measurements_indicator_id',
                    'query': """
                    SELECT COUNT(*) as invalid_count
                    FROM household_measurements hm
                    LEFT JOIN indicators i ON hm.indicator_id = i.indicator_id
                    WHERE i.indicator_id IS NULL
                    """
                }
            ]
            
            for check in checks:
                result = pd.read_sql(check['query'], self.db_engine)
                invalid_count = result['invalid_count'].iloc[0]
                
                if invalid_count > 0:
                    issues.append({
                        'check_name': check['name'],
                        'invalid_count': int(invalid_count),
                        'severity': 'High' if invalid_count > 100 else 'Medium'
                    })
                    
            return {'consistency_issues': issues}
        except Exception as e:
            logger.error(f"Error checking consistency: {e}")
            raise
            
    def _check_timeliness(self) -> Dict[str, float]:
        """
        Check data timeliness.
        
        Returns:
            Dict[str, float]: Timeliness scores
        """
        try:
            # Check survey data timeliness
            query = """
            SELECT 
                COUNT(*) as total_surveys,
                COUNT(CASE WHEN survey_date >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_surveys
            FROM household_surveys
            """
            
            result = pd.read_sql(query, self.db_engine)
            timeliness = (result['recent_surveys'].iloc[0] / result['total_surveys'].iloc[0]) * 100
            
            return {'timeliness_score': round(timeliness, 2)}
        except Exception as e:
            logger.error(f"Error checking timeliness: {e}")
            raise
            
    def _check_accuracy(self) -> Dict[str, List[Dict]]:
        """
        Check data accuracy.
        
        Returns:
            Dict[str, List[Dict]]: Accuracy issues found
        """
        try:
            issues = []
            
            # Check for outliers in measurements
            query = """
            WITH measurement_stats AS (
                SELECT 
                    indicator_id,
                    AVG(value) as mean_value,
                    STDDEV(value) as std_value
                FROM household_measurements
                GROUP BY indicator_id
            )
            SELECT 
                hm.indicator_id,
                i.name as indicator_name,
                COUNT(*) as outlier_count
            FROM household_measurements hm
            JOIN measurement_stats ms ON hm.indicator_id = ms.indicator_id
            JOIN indicators i ON hm.indicator_id = i.indicator_id
            WHERE ABS(hm.value - ms.mean_value) > 3 * ms.std_value
            GROUP BY hm.indicator_id, i.name
            HAVING COUNT(*) > 0
            """
            
            outliers = pd.read_sql(query, self.db_engine)
            
            for _, row in outliers.iterrows():
                issues.append({
                    'indicator': row['indicator_name'],
                    'outlier_count': int(row['outlier_count']),
                    'severity': 'High' if row['outlier_count'] > 50 else 'Medium'
                })
                
            return {'accuracy_issues': issues}
        except Exception as e:
            logger.error(f"Error checking accuracy: {e}")
            raise
            
    def _get_table_columns(self, table: str) -> List[str]:
        """
        Get column names for a table.
        
        Args:
            table: Table name
            
        Returns:
            List[str]: List of column names
        """
        try:
            query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = 'public'
            """
            
            result = pd.read_sql(query, self.db_engine)
            return result['column_name'].tolist()
        except Exception as e:
            logger.error(f"Error getting table columns: {e}")
            raise
            
    def _log_quality_results(self, results: Dict) -> None:
        """
        Log quality check results to the database.
        
        Args:
            results: Dictionary containing quality check results
        """
        try:
            # Prepare log entry
            log_entry = {
                'check_type': 'data_quality',
                'check_description': 'Comprehensive data quality check',
                'status': 'completed',
                'details': json.dumps(results)
            }
            
            # Insert into quality logs
            query = """
            INSERT INTO data_quality_logs (
                table_name,
                check_type,
                check_description,
                status,
                details
            ) VALUES (
                'all_tables',
                :check_type,
                :check_description,
                :status,
                :details::jsonb
            )
            """
            
            with self.db_engine.connect() as conn:
                conn.execute(text(query), log_entry)
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error logging quality results: {e}")
            raise
            
    def run_quality_checks(self) -> Dict:
        """
        Run all data quality checks.
        
        Returns:
            Dict: Comprehensive quality check results
        """
        try:
            # Run all checks
            completeness = self._check_completeness()
            consistency = self._check_consistency()
            timeliness = self._check_timeliness()
            accuracy = self._check_accuracy()
            
            # Combine results
            results = {
                'timestamp': datetime.now().isoformat(),
                'completeness_scores': completeness,
                'consistency_issues': consistency['consistency_issues'],
                'timeliness_score': timeliness['timeliness_score'],
                'accuracy_issues': accuracy['accuracy_issues']
            }
            
            # Calculate overall score
            overall_score = (
                np.mean(list(completeness.values())) * 0.4 +
                (100 - len(consistency['consistency_issues']) * 10) * 0.3 +
                timeliness['timeliness_score'] * 0.2 +
                (100 - len(accuracy['accuracy_issues']) * 5) * 0.1
            )
            
            results['overall_quality_score'] = round(overall_score, 2)
            
            # Log results
            self._log_quality_results(results)
            
            return results
        except Exception as e:
            logger.error(f"Error running quality checks: {e}")
            raise

def run_quality_checks(db_conn_id: str) -> Dict:
    """
    Wrapper function for running data quality checks.
    
    Args:
        db_conn_id: Database connection identifier
        
    Returns:
        Dict: Quality check results
    """
    try:
        quality_checks = DataQualityChecks(db_conn_id)
        return quality_checks.run_quality_checks()
    except Exception as e:
        logger.error(f"Failed to run quality checks: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    db_conn_id = "postgres_default"
    
    try:
        results = run_quality_checks(db_conn_id)
        print(f"Quality checks completed. Results: {json.dumps(results, indent=2)}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1) 