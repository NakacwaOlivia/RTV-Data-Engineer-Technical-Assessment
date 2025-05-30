import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import pandas as pd
from minio import Minio
import psycopg2
from psycopg2.extras import Json, execute_values
import hashlib
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class DataLineage:
    """Represents data lineage information."""
    source_file: str
    ingestion_time: datetime
    record_count: int
    hash_value: str
    metadata: Dict
    parent_lineage: Optional[str] = None

class SurveyDataIngester:
    """Handles enhanced data ingestion with CDC and lineage tracking."""
    
    def __init__(self, 
                 db_conn: psycopg2.extensions.connection,
                 minio_client: Minio,
                 bucket_name: str = "survey-data"):
        """Initialize the data ingester.
        
        Args:
            db_conn: PostgreSQL connection
            minio_client: MinIO client
            bucket_name: Name of the MinIO bucket
        """
        self.conn = db_conn
        self.minio = minio_client
        self.bucket = bucket_name
        self._init_lineage_tables()
        logger.info("Initialized SurveyDataIngester")
    
    def _init_lineage_tables(self) -> None:
        """Initialize tables for tracking data lineage."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS data_lineage (
                    lineage_id SERIAL PRIMARY KEY,
                    source_file VARCHAR(255),
                    ingestion_time TIMESTAMP,
                    record_count INTEGER,
                    hash_value VARCHAR(64),
                    metadata JSONB,
                    parent_lineage_id INTEGER REFERENCES data_lineage(lineage_id)
                );
                
                CREATE TABLE IF NOT EXISTS change_log (
                    change_id SERIAL PRIMARY KEY,
                    lineage_id INTEGER REFERENCES data_lineage(lineage_id),
                    table_name VARCHAR(100),
                    operation VARCHAR(20),
                    record_count INTEGER,
                    change_time TIMESTAMP,
                    details JSONB
                );
                
                CREATE INDEX IF NOT EXISTS idx_lineage_hash 
                ON data_lineage(hash_value);
                
                CREATE INDEX IF NOT EXISTS idx_change_log_lineage 
                ON change_log(lineage_id);
            """)
            self.conn.commit()
    
    def _compute_data_hash(self, df: pd.DataFrame) -> str:
        """Compute a hash of the data for change detection."""
        # Sort columns to ensure consistent hashing
        df = df.sort_index(axis=1)
        # Convert to string and hash
        data_str = df.to_string()
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def _detect_changes(self, 
                       table_name: str,
                       df: pd.DataFrame,
                       key_columns: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Detect changes in the data using CDC.
        
        Args:
            table_name: Name of the target table
            df: New data
            key_columns: Columns that uniquely identify a record
            
        Returns:
            Tuple of (inserts, updates, deletes) DataFrames
        """
        try:
            # Get existing data
            with self.conn.cursor() as cur:
                cur.execute(f"""
                    SELECT {', '.join(key_columns + ['updated_at'])}
                    FROM {table_name}
                    WHERE survey_year = %s
                """, (df['survey_year'].iloc[0],))
                existing = pd.DataFrame(cur.fetchall(), columns=key_columns + ['updated_at'])
            
            if existing.empty:
                return df, pd.DataFrame(), pd.DataFrame()
            
            # Find new records
            new_keys = set(map(tuple, df[key_columns].values))
            existing_keys = set(map(tuple, existing[key_columns].values))
            
            inserts = df[df[key_columns].apply(tuple, axis=1).isin(new_keys - existing_keys)]
            deletes = existing[existing[key_columns].apply(tuple, axis=1).isin(existing_keys - new_keys)]
            
            # Find updates
            common_keys = new_keys.intersection(existing_keys)
            if common_keys:
                updates = df[df[key_columns].apply(tuple, axis=1).isin(common_keys)]
                # Filter out unchanged records
                updates = updates[updates.apply(
                    lambda row: any(row[col] != existing.loc[
                        existing[key_columns].apply(tuple, axis=1) == tuple(row[key_columns])
                    ][col].iloc[0] for col in df.columns if col not in key_columns + ['updated_at']),
                    axis=1
                )]
            else:
                updates = pd.DataFrame()
            
            return inserts, updates, deletes
            
        except Exception as e:
            logger.error(f"Failed to detect changes: {e}")
            raise
    
    def ingest_survey_data(self,
                          file_path: str,
                          survey_year: int,
                          metadata: Optional[Dict] = None) -> DataLineage:
        """Ingest survey data with change detection and lineage tracking.
        
        Args:
            file_path: Path to the survey data file
            survey_year: Year of the survey
            metadata: Additional metadata
            
        Returns:
            DataLineage: Lineage information for the ingestion
        """
        try:
            # Read and validate data
            df = pd.read_csv(file_path)
            if 'survey_year' not in df.columns:
                df['survey_year'] = survey_year
            
            # Compute data hash
            hash_value = self._compute_data_hash(df)
            
            # Check if we've seen this data before
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT lineage_id, record_count
                    FROM data_lineage
                    WHERE hash_value = %s
                    ORDER BY ingestion_time DESC
                    LIMIT 1
                """, (hash_value,))
                existing = cur.fetchone()
                
                if existing and existing[1] == len(df):
                    logger.info(f"Data already ingested (lineage_id: {existing[0]})")
                    return None
            
            # Store in MinIO
            parquet_file = f"raw/survey_{survey_year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            temp_path = f"/tmp/{os.path.basename(parquet_file)}"
            df.to_parquet(temp_path)
            self.minio.fput_object(self.bucket, parquet_file, temp_path)
            os.remove(temp_path)
            
            # Record lineage
            lineage = DataLineage(
                source_file=file_path,
                ingestion_time=datetime.now(),
                record_count=len(df),
                hash_value=hash_value,
                metadata={
                    "survey_year": survey_year,
                    "file_type": "survey",
                    **(metadata or {})
                }
            )
            
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO data_lineage 
                    (source_file, ingestion_time, record_count, hash_value, metadata)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING lineage_id
                """, (
                    lineage.source_file,
                    lineage.ingestion_time,
                    lineage.record_count,
                    lineage.hash_value,
                    Json(lineage.metadata)
                ))
                lineage_id = cur.fetchone()[0]
                
                # Detect and record changes
                for table_name, key_cols in [
                    ("fact_survey", ["hhid_2", "survey_year"]),
                    ("fact_expenditure", ["hhid_2", "survey_year"]),
                    ("fact_crop_yield", ["hhid_2", "survey_year", "crop_type"])
                ]:
                    inserts, updates, deletes = self._detect_changes(
                        table_name, df, key_cols
                    )
                    
                    # Record changes
                    for operation, change_df in [
                        ("INSERT", inserts),
                        ("UPDATE", updates),
                        ("DELETE", deletes)
                    ]:
                        if not change_df.empty:
                            cur.execute("""
                                INSERT INTO change_log 
                                (lineage_id, table_name, operation, record_count, 
                                 change_time, details)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                lineage_id,
                                table_name,
                                operation,
                                len(change_df),
                                datetime.now(),
                                Json({
                                    "columns": list(change_df.columns),
                                    "sample": change_df.head().to_dict()
                                })
                            ))
                
                self.conn.commit()
                logger.info(f"Ingested {len(df)} records with lineage_id {lineage_id}")
                return lineage
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to ingest {file_path}: {e}")
            raise
    
    def get_lineage_history(self,
                           survey_year: Optional[int] = None,
                           limit: int = 10) -> List[Dict]:
        """Get lineage history for survey data.
        
        Args:
            survey_year: Optional year to filter by
            limit: Maximum number of records to return
            
        Returns:
            List[Dict]: Lineage history
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        l.lineage_id,
                        l.source_file,
                        l.ingestion_time,
                        l.record_count,
                        l.metadata,
                        COUNT(c.change_id) as change_count,
                        json_agg(json_build_object(
                            'table', c.table_name,
                            'operation', c.operation,
                            'count', c.record_count
                        )) as changes
                    FROM data_lineage l
                    LEFT JOIN change_log c ON l.lineage_id = c.lineage_id
                    WHERE (%s IS NULL OR l.metadata->>'survey_year' = %s::text)
                    GROUP BY l.lineage_id
                    ORDER BY l.ingestion_time DESC
                    LIMIT %s
                """, (survey_year, str(survey_year) if survey_year else None, limit))
                
                return [
                    {
                        "lineage_id": row[0],
                        "source_file": row[1],
                        "ingestion_time": row[2],
                        "record_count": row[3],
                        "metadata": row[4],
                        "change_count": row[5],
                        "changes": row[6]
                    }
                    for row in cur.fetchall()
                ]
                
        except Exception as e:
            logger.error(f"Failed to get lineage history: {e}")
            return []
    
    def get_change_summary(self,
                          lineage_id: int) -> Dict:
        """Get a summary of changes for a lineage record.
        
        Args:
            lineage_id: ID of the lineage record
            
        Returns:
            Dict: Change summary
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        table_name,
                        operation,
                        SUM(record_count) as total_records,
                        COUNT(*) as change_count
                    FROM change_log
                    WHERE lineage_id = %s
                    GROUP BY table_name, operation
                """, (lineage_id,))
                
                changes = {}
                for row in cur.fetchall():
                    table, op = row[0], row[1]
                    if table not in changes:
                        changes[table] = {}
                    changes[table][op] = {
                        "total_records": row[2],
                        "change_count": row[3]
                    }
                
                return changes
                
        except Exception as e:
            logger.error(f"Failed to get change summary: {e}")
            return {} 