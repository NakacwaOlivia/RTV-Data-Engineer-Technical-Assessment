import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import psycopg2
from psycopg2.extras import Json
import json
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class MigrationType(Enum):
    """Types of schema migrations."""
    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    RENAME_COLUMN = "RENAME_COLUMN"
    MODIFY_TYPE = "MODIFY_TYPE"
    ADD_CONSTRAINT = "ADD_CONSTRAINT"
    DROP_CONSTRAINT = "DROP_CONSTRAINT"
    ADD_INDEX = "ADD_INDEX"
    DROP_INDEX = "DROP_INDEX"

@dataclass
class MigrationStep:
    """Represents a single migration step."""
    type: MigrationType
    table_name: str
    details: Dict[str, Any]
    rollback_sql: str
    forward_sql: str
    description: str

class SchemaMigrationManager:
    """Manages schema migrations and backward compatibility."""
    
    def __init__(self, db_conn: psycopg2.extensions.connection):
        """Initialize the schema migration manager.
        
        Args:
            db_conn: PostgreSQL connection
        """
        self.conn = db_conn
        self._init_migration_tables()
        logger.info("Initialized SchemaMigrationManager")
    
    def _init_migration_tables(self) -> None:
        """Initialize tables for tracking schema migrations."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    migration_id SERIAL PRIMARY KEY,
                    migration_name VARCHAR(255) UNIQUE,
                    applied_at TIMESTAMP,
                    status VARCHAR(20),
                    steps JSONB,
                    metadata JSONB
                );
                
                CREATE TABLE IF NOT EXISTS schema_versions (
                    table_name VARCHAR(100),
                    version INTEGER,
                    columns JSONB,
                    constraints JSONB,
                    indexes JSONB,
                    valid_from TIMESTAMP,
                    valid_to TIMESTAMP,
                    PRIMARY KEY (table_name, version)
                );
                
                CREATE TABLE IF NOT EXISTS column_mappings (
                    table_name VARCHAR(100),
                    old_column VARCHAR(100),
                    new_column VARCHAR(100),
                    version INTEGER,
                    mapping_type VARCHAR(20),
                    mapping_rule JSONB,
                    PRIMARY KEY (table_name, old_column, new_column, version)
                );
                
                CREATE INDEX IF NOT EXISTS idx_schema_versions_table 
                ON schema_versions(table_name, valid_from, valid_to);
            """)
            self.conn.commit()
    
    def create_migration(self,
                        migration_name: str,
                        steps: List[MigrationStep],
                        metadata: Optional[Dict] = None) -> int:
        """Create a new schema migration.
        
        Args:
            migration_name: Unique name for the migration
            steps: List of migration steps
            metadata: Additional metadata
            
        Returns:
            int: Migration ID
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO schema_migrations 
                    (migration_name, applied_at, status, steps, metadata)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING migration_id
                """, (
                    migration_name,
                    datetime.now(),
                    "PENDING",
                    Json([{
                        "type": step.type.value,
                        "table_name": step.table_name,
                        "details": step.details,
                        "rollback_sql": step.rollback_sql,
                        "forward_sql": step.forward_sql,
                        "description": step.description
                    } for step in steps]),
                    Json(metadata or {})
                ))
                migration_id = cur.fetchone()[0]
                self.conn.commit()
                logger.info(f"Created migration {migration_name} (ID: {migration_id})")
                return migration_id
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to create migration: {e}")
            raise
    
    def apply_migration(self, migration_id: int) -> None:
        """Apply a pending migration.
        
        Args:
            migration_id: ID of the migration to apply
        """
        try:
            with self.conn.cursor() as cur:
                # Get migration details
                cur.execute("""
                    SELECT migration_name, steps
                    FROM schema_migrations
                    WHERE migration_id = %s AND status = 'PENDING'
                    FOR UPDATE
                """, (migration_id,))
                migration = cur.fetchone()
                
                if not migration:
                    raise ValueError(f"Migration {migration_id} not found or not pending")
                
                migration_name, steps = migration
                
                # Start transaction
                cur.execute("BEGIN")
                
                try:
                    # Apply each step
                    for step in steps:
                        logger.info(f"Applying step: {step['description']}")
                        cur.execute(step['forward_sql'])
                        
                        # Update schema version if needed
                        if step['type'] in [
                            MigrationType.ADD_COLUMN.value,
                            MigrationType.DROP_COLUMN.value,
                            MigrationType.RENAME_COLUMN.value,
                            MigrationType.MODIFY_TYPE.value
                        ]:
                            self._update_schema_version(
                                cur, step['table_name'], step['details']
                            )
                    
                    # Mark migration as applied
                    cur.execute("""
                        UPDATE schema_migrations
                        SET status = 'APPLIED', applied_at = %s
                        WHERE migration_id = %s
                    """, (datetime.now(), migration_id))
                    
                    self.conn.commit()
                    logger.info(f"Applied migration {migration_name}")
                    
                except Exception as e:
                    self.conn.rollback()
                    logger.error(f"Failed to apply migration: {e}")
                    raise
                
        except Exception as e:
            logger.error(f"Failed to apply migration {migration_id}: {e}")
            raise
    
    def rollback_migration(self, migration_id: int) -> None:
        """Rollback an applied migration.
        
        Args:
            migration_id: ID of the migration to rollback
        """
        try:
            with self.conn.cursor() as cur:
                # Get migration details
                cur.execute("""
                    SELECT migration_name, steps
                    FROM schema_migrations
                    WHERE migration_id = %s AND status = 'APPLIED'
                    FOR UPDATE
                """, (migration_id,))
                migration = cur.fetchone()
                
                if not migration:
                    raise ValueError(f"Migration {migration_id} not found or not applied")
                
                migration_name, steps = migration
                
                # Start transaction
                cur.execute("BEGIN")
                
                try:
                    # Rollback each step in reverse order
                    for step in reversed(steps):
                        logger.info(f"Rolling back step: {step['description']}")
                        cur.execute(step['rollback_sql'])
                        
                        # Update schema version if needed
                        if step['type'] in [
                            MigrationType.ADD_COLUMN.value,
                            MigrationType.DROP_COLUMN.value,
                            MigrationType.RENAME_COLUMN.value,
                            MigrationType.MODIFY_TYPE.value
                        ]:
                            self._update_schema_version(
                                cur, step['table_name'], step['details'],
                                is_rollback=True
                            )
                    
                    # Mark migration as rolled back
                    cur.execute("""
                        UPDATE schema_migrations
                        SET status = 'ROLLED_BACK', applied_at = %s
                        WHERE migration_id = %s
                    """, (datetime.now(), migration_id))
                    
                    self.conn.commit()
                    logger.info(f"Rolled back migration {migration_name}")
                    
                except Exception as e:
                    self.conn.rollback()
                    logger.error(f"Failed to rollback migration: {e}")
                    raise
                
        except Exception as e:
            logger.error(f"Failed to rollback migration {migration_id}: {e}")
            raise
    
    def _update_schema_version(self,
                              cur: psycopg2.extensions.cursor,
                              table_name: str,
                              details: Dict,
                              is_rollback: bool = False) -> None:
        """Update schema version information.
        
        Args:
            cur: Database cursor
            table_name: Name of the table
            details: Migration details
            is_rollback: Whether this is a rollback operation
        """
        # Get current schema
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table_name,))
        columns = {
            row[0]: {
                "type": row[1],
                "nullable": row[2] == "YES"
            }
            for row in cur.fetchall()
        }
        
        # Get current version
        cur.execute("""
            SELECT MAX(version)
            FROM schema_versions
            WHERE table_name = %s
        """, (table_name,))
        current_version = cur.fetchone()[0] or 0
        
        # Update version
        if not is_rollback:
            new_version = current_version + 1
            valid_from = datetime.now()
            valid_to = None
        else:
            new_version = current_version
            valid_from = None
            valid_to = datetime.now()
        
        # Store version
        cur.execute("""
            INSERT INTO schema_versions
            (table_name, version, columns, valid_from, valid_to)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            table_name,
            new_version,
            Json(columns),
            valid_from,
            valid_to
        ))
    
    def get_table_schema(self,
                        table_name: str,
                        version: Optional[int] = None) -> Dict:
        """Get schema information for a table.
        
        Args:
            table_name: Name of the table
            version: Optional schema version
            
        Returns:
            Dict: Schema information
        """
        try:
            with self.conn.cursor() as cur:
                if version is not None:
                    cur.execute("""
                        SELECT columns, constraints, indexes
                        FROM schema_versions
                        WHERE table_name = %s AND version = %s
                    """, (table_name, version))
                else:
                    cur.execute("""
                        SELECT columns, constraints, indexes
                        FROM schema_versions
                        WHERE table_name = %s
                        AND valid_to IS NULL
                        ORDER BY version DESC
                        LIMIT 1
                    """, (table_name,))
                
                row = cur.fetchone()
                if not row:
                    return {}
                
                return {
                    "columns": row[0],
                    "constraints": row[1],
                    "indexes": row[2]
                }
                
        except Exception as e:
            logger.error(f"Failed to get schema for {table_name}: {e}")
            return {}
    
    def get_column_mapping(self,
                          table_name: str,
                          old_column: str,
                          version: Optional[int] = None) -> Optional[str]:
        """Get the new name for a renamed column.
        
        Args:
            table_name: Name of the table
            old_column: Old column name
            version: Optional schema version
            
        Returns:
            Optional[str]: New column name if mapped
        """
        try:
            with self.conn.cursor() as cur:
                if version is not None:
                    cur.execute("""
                        SELECT new_column, mapping_rule
                        FROM column_mappings
                        WHERE table_name = %s
                        AND old_column = %s
                        AND version = %s
                    """, (table_name, old_column, version))
                else:
                    cur.execute("""
                        SELECT new_column, mapping_rule
                        FROM column_mappings
                        WHERE table_name = %s
                        AND old_column = %s
                        ORDER BY version DESC
                        LIMIT 1
                    """, (table_name, old_column))
                
                row = cur.fetchone()
                if not row:
                    return None
                
                return row[0]
                
        except Exception as e:
            logger.error(f"Failed to get column mapping: {e}")
            return None
    
    def get_migration_history(self,
                            table_name: Optional[str] = None,
                            limit: int = 10) -> List[Dict]:
        """Get migration history.
        
        Args:
            table_name: Optional table to filter by
            limit: Maximum number of records to return
            
        Returns:
            List[Dict]: Migration history
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        m.migration_id,
                        m.migration_name,
                        m.applied_at,
                        m.status,
                        m.steps,
                        m.metadata
                    FROM schema_migrations m
                    WHERE (%s IS NULL OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(m.steps) s
                        WHERE s->>'table_name' = %s
                    ))
                    ORDER BY m.applied_at DESC
                    LIMIT %s
                """, (table_name, table_name, limit))
                
                return [
                    {
                        "migration_id": row[0],
                        "migration_name": row[1],
                        "applied_at": row[2],
                        "status": row[3],
                        "steps": row[4],
                        "metadata": row[5]
                    }
                    for row in cur.fetchall()
                ]
                
        except Exception as e:
            logger.error(f"Failed to get migration history: {e}")
            return [] 