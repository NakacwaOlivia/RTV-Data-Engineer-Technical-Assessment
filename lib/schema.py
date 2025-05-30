import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import json
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class DataType(Enum):
    """Supported data types for schema registry."""
    VARCHAR = "VARCHAR"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    JSON = "JSON"

@dataclass
class ColumnDefinition:
    """Represents a column definition in the schema registry."""
    name: str
    data_type: DataType
    description: str
    valid_values: Optional[Dict] = None
    is_required: bool = True
    version: int = 1

class SchemaManager:
    """Manages schema evolution and validation for the data pipeline."""
    
    def __init__(self, db_conn: psycopg2.extensions.connection):
        """Initialize the schema manager.
        
        Args:
            db_conn: PostgreSQL connection
        """
        self.conn = db_conn
        self._init_schema_registry()
        logger.info("Initialized SchemaManager")
    
    def _init_schema_registry(self) -> None:
        """Initialize the schema registry tables."""
        with self.conn.cursor() as cur:
            # Main schema registry table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_registry (
                    survey_year INT,
                    column_name VARCHAR(100),
                    data_type VARCHAR(50),
                    description TEXT,
                    valid_values JSONB,
                    is_required BOOLEAN,
                    version INT,
                    effective_date DATE,
                    deprecated_date DATE,
                    PRIMARY KEY (survey_year, column_name, version)
                );
                
                CREATE TABLE IF NOT EXISTS schema_changes (
                    change_id SERIAL PRIMARY KEY,
                    survey_year INT,
                    change_type VARCHAR(50),
                    column_name VARCHAR(100),
                    old_version INT,
                    new_version INT,
                    change_date TIMESTAMP,
                    change_reason TEXT,
                    metadata JSONB
                );
                
                CREATE INDEX IF NOT EXISTS idx_schema_registry_year 
                ON schema_registry(survey_year);
                
                CREATE INDEX IF NOT EXISTS idx_schema_changes_year 
                ON schema_changes(survey_year);
            """)
            self.conn.commit()
    
    def register_column(self, 
                       survey_year: int, 
                       column: ColumnDefinition,
                       change_reason: Optional[str] = None) -> int:
        """Register a new column or new version of existing column.
        
        Args:
            survey_year: Year of the survey
            column: Column definition
            change_reason: Reason for the change (if updating)
            
        Returns:
            int: The new version number
        """
        try:
            with self.conn.cursor() as cur:
                # Get current version if exists
                cur.execute("""
                    SELECT MAX(version) 
                    FROM schema_registry 
                    WHERE survey_year = %s AND column_name = %s
                """, (survey_year, column.name))
                current_version = cur.fetchone()[0] or 0
                new_version = current_version + 1
                
                # If updating existing column, mark old version as deprecated
                if current_version > 0:
                    cur.execute("""
                        UPDATE schema_registry 
                        SET deprecated_date = CURRENT_DATE
                        WHERE survey_year = %s 
                        AND column_name = %s 
                        AND version = %s
                    """, (survey_year, column.name, current_version))
                    
                    # Log the change
                    cur.execute("""
                        INSERT INTO schema_changes 
                        (survey_year, change_type, column_name, old_version, 
                         new_version, change_date, change_reason)
                        VALUES (%s, 'UPDATE', %s, %s, %s, CURRENT_TIMESTAMP, %s)
                    """, (survey_year, column.name, current_version, new_version, change_reason))
                
                # Register new version
                cur.execute("""
                    INSERT INTO schema_registry 
                    (survey_year, column_name, data_type, description, 
                     valid_values, is_required, version, effective_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
                """, (
                    survey_year, 
                    column.name, 
                    column.data_type.value,
                    column.description,
                    Json(column.valid_values) if column.valid_values else None,
                    column.is_required,
                    new_version
                ))
                
                self.conn.commit()
                logger.info(f"Registered column {column.name} version {new_version} for year {survey_year}")
                return new_version
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to register column {column.name}: {e}")
            raise
    
    def get_column_definition(self, 
                            survey_year: int, 
                            column_name: str, 
                            version: Optional[int] = None) -> Optional[ColumnDefinition]:
        """Get column definition for a specific version or latest version.
        
        Args:
            survey_year: Year of the survey
            column_name: Name of the column
            version: Specific version to get (None for latest)
            
        Returns:
            Optional[ColumnDefinition]: Column definition if found
        """
        try:
            with self.conn.cursor() as cur:
                if version is None:
                    cur.execute("""
                        SELECT data_type, description, valid_values, is_required, version
                        FROM schema_registry
                        WHERE survey_year = %s 
                        AND column_name = %s
                        AND deprecated_date IS NULL
                        ORDER BY version DESC
                        LIMIT 1
                    """, (survey_year, column_name))
                else:
                    cur.execute("""
                        SELECT data_type, description, valid_values, is_required, version
                        FROM schema_registry
                        WHERE survey_year = %s 
                        AND column_name = %s
                        AND version = %s
                    """, (survey_year, column_name, version))
                
                row = cur.fetchone()
                if row:
                    return ColumnDefinition(
                        name=column_name,
                        data_type=DataType(row[0]),
                        description=row[1],
                        valid_values=row[2],
                        is_required=row[3],
                        version=row[4]
                    )
                return None
                
        except Exception as e:
            logger.error(f"Failed to get column definition for {column_name}: {e}")
            return None
    
    def detect_schema_changes(self, 
                            current_year: int, 
                            previous_year: int) -> Dict[str, List[str]]:
        """Detect schema changes between two survey years.
        
        Args:
            current_year: Current survey year
            previous_year: Previous survey year to compare against
            
        Returns:
            Dict[str, List[str]]: Dictionary of changes by type
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    WITH current_cols AS (
                        SELECT column_name, data_type, valid_values
                        FROM schema_registry
                        WHERE survey_year = %s
                        AND deprecated_date IS NULL
                    ),
                    previous_cols AS (
                        SELECT column_name, data_type, valid_values
                        FROM schema_registry
                        WHERE survey_year = %s
                        AND deprecated_date IS NULL
                    )
                    SELECT 
                        CASE 
                            WHEN pc.column_name IS NULL THEN 'added'
                            WHEN cc.column_name IS NULL THEN 'removed'
                            WHEN cc.data_type != pc.data_type 
                                 OR cc.valid_values != pc.valid_values THEN 'modified'
                            ELSE 'unchanged'
                        END as change_type,
                        COALESCE(cc.column_name, pc.column_name) as column_name
                    FROM current_cols cc
                    FULL OUTER JOIN previous_cols pc 
                    ON cc.column_name = pc.column_name
                """, (current_year, previous_year))
                
                changes = {
                    'added': [],
                    'removed': [],
                    'modified': [],
                    'unchanged': []
                }
                
                for row in cur.fetchall():
                    changes[row[0]].append(row[1])
                
                return changes
                
        except Exception as e:
            logger.error(f"Failed to detect schema changes: {e}")
            return {'added': [], 'removed': [], 'modified': [], 'unchanged': []}
    
    def validate_data_against_schema(self, 
                                   survey_year: int, 
                                   data: Dict) -> Tuple[bool, List[str]]:
        """Validate data against the current schema.
        
        Args:
            survey_year: Year of the survey
            data: Dictionary of column names and values
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, list of validation errors)
        """
        errors = []
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name, data_type, valid_values, is_required
                    FROM schema_registry
                    WHERE survey_year = %s
                    AND deprecated_date IS NULL
                """, (survey_year,))
                
                schema = {row[0]: (DataType(row[1]), row[2], row[3]) 
                         for row in cur.fetchall()}
                
                # Check required columns
                for col_name, (_, _, is_required) in schema.items():
                    if is_required and col_name not in data:
                        errors.append(f"Required column {col_name} is missing")
                
                # Validate data types and values
                for col_name, value in data.items():
                    if col_name in schema:
                        data_type, valid_values, _ = schema[col_name]
                        # Type validation
                        if not self._validate_type(value, data_type):
                            errors.append(
                                f"Column {col_name} has invalid type. "
                                f"Expected {data_type.value}, got {type(value).__name__}"
                            )
                        # Value validation
                        if valid_values and value not in valid_values:
                            errors.append(
                                f"Column {col_name} has invalid value. "
                                f"Expected one of {valid_values}, got {value}"
                            )
                
                return len(errors) == 0, errors
                
        except Exception as e:
            logger.error(f"Failed to validate data: {e}")
            return False, [str(e)]
    
    def _validate_type(self, value: any, data_type: DataType) -> bool:
        """Validate a value against a data type."""
        if value is None:
            return True
        
        try:
            if data_type == DataType.VARCHAR:
                return isinstance(value, str)
            elif data_type == DataType.INTEGER:
                return isinstance(value, int)
            elif data_type == DataType.DECIMAL:
                return isinstance(value, (int, float))
            elif data_type == DataType.BOOLEAN:
                return isinstance(value, bool)
            elif data_type == DataType.TIMESTAMP:
                return isinstance(value, datetime)
            elif data_type == DataType.JSON:
                return isinstance(value, (dict, list))
            return False
        except Exception:
            return False 