import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class PipelineStatus(Enum):
    """Status of pipeline runs."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class PipelineMetric:
    """Pipeline execution metric."""
    metric_name: str
    metric_value: float
    metric_type: str
    timestamp: datetime
    metadata: Optional[Dict] = None

class PipelineMonitor:
    """Monitors pipeline execution and metrics."""
    
    def __init__(self, db_conn: psycopg2.extensions.connection):
        """Initialize the pipeline monitor.
        
        Args:
            db_conn: PostgreSQL connection
        """
        self.conn = db_conn
        self._init_monitoring_tables()
        logger.info("Initialized PipelineMonitor")
    
    def _init_monitoring_tables(self) -> None:
        """Initialize tables for monitoring pipeline execution."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id SERIAL PRIMARY KEY,
                    pipeline_name VARCHAR(100),
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status VARCHAR(20),
                    error_message TEXT,
                    metadata JSONB
                );
                
                CREATE TABLE IF NOT EXISTS pipeline_metrics (
                    metric_id SERIAL PRIMARY KEY,
                    run_id INT REFERENCES pipeline_runs(run_id),
                    metric_name VARCHAR(100),
                    metric_value FLOAT,
                    metric_type VARCHAR(50),
                    timestamp TIMESTAMP,
                    metadata JSONB
                );
                
                CREATE TABLE IF NOT EXISTS pipeline_alerts (
                    alert_id SERIAL PRIMARY KEY,
                    run_id INT REFERENCES pipeline_runs(run_id),
                    alert_type VARCHAR(50),
                    alert_message TEXT,
                    severity VARCHAR(20),
                    timestamp TIMESTAMP,
                    status VARCHAR(20),
                    metadata JSONB
                );
                
                CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name 
                ON pipeline_runs(pipeline_name, start_time);
                
                CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_run 
                ON pipeline_metrics(run_id, metric_name);
                
                CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_run 
                ON pipeline_alerts(run_id, alert_type);
            """)
            self.conn.commit()
    
    def start_pipeline_run(self, 
                          pipeline_name: str,
                          metadata: Optional[Dict] = None) -> int:
        """Start a new pipeline run.
        
        Args:
            pipeline_name: Name of the pipeline
            metadata: Additional metadata for the run
            
        Returns:
            int: Run ID
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_runs 
                    (pipeline_name, start_time, status, metadata)
                    VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
                    RETURNING run_id
                """, (
                    pipeline_name,
                    PipelineStatus.RUNNING.value,
                    Json(metadata) if metadata else None
                ))
                run_id = cur.fetchone()[0]
                self.conn.commit()
                logger.info(f"Started pipeline run {run_id} for {pipeline_name}")
                return run_id
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to start pipeline run: {e}")
            raise
    
    def end_pipeline_run(self, 
                        run_id: int,
                        status: PipelineStatus = PipelineStatus.COMPLETED,
                        error_message: Optional[str] = None) -> None:
        """End a pipeline run.
        
        Args:
            run_id: ID of the run to end
            status: Final status of the run
            error_message: Error message if the run failed
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_runs 
                    SET end_time = CURRENT_TIMESTAMP,
                        status = %s,
                        error_message = %s
                    WHERE run_id = %s
                """, (status.value, error_message, run_id))
                self.conn.commit()
                logger.info(f"Ended pipeline run {run_id} with status {status.value}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to end pipeline run {run_id}: {e}")
            raise
    
    def record_metric(self, 
                     run_id: int,
                     metric: PipelineMetric) -> None:
        """Record a pipeline metric.
        
        Args:
            run_id: ID of the pipeline run
            metric: Metric to record
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_metrics 
                    (run_id, metric_name, metric_value, metric_type,
                     timestamp, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    run_id,
                    metric.metric_name,
                    metric.metric_value,
                    metric.metric_type,
                    metric.timestamp,
                    Json(metric.metadata) if metric.metadata else None
                ))
                self.conn.commit()
                logger.debug(f"Recorded metric {metric.metric_name} for run {run_id}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to record metric for run {run_id}: {e}")
            raise
    
    def record_alert(self,
                    run_id: int,
                    alert_type: str,
                    alert_message: str,
                    severity: str = "warning",
                    metadata: Optional[Dict] = None) -> None:
        """Record a pipeline alert.
        
        Args:
            run_id: ID of the pipeline run
            alert_type: Type of alert
            alert_message: Alert message
            severity: Alert severity (info, warning, error, critical)
            metadata: Additional alert metadata
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_alerts 
                    (run_id, alert_type, alert_message, severity,
                     timestamp, status, metadata)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, 'active', %s)
                """, (
                    run_id,
                    alert_type,
                    alert_message,
                    severity,
                    Json(metadata) if metadata else None
                ))
                self.conn.commit()
                logger.warning(f"Recorded {severity} alert: {alert_message}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to record alert for run {run_id}: {e}")
            raise
    
    def get_pipeline_status(self, 
                           pipeline_name: str,
                           lookback_days: int = 7) -> Dict[str, Any]:
        """Get the status of recent pipeline runs.
        
        Args:
            pipeline_name: Name of the pipeline
            lookback_days: Number of days to look back
            
        Returns:
            Dict: Pipeline status summary
        """
        try:
            with self.conn.cursor() as cur:
                # Get run statistics
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_runs,
                        COUNT(*) FILTER (WHERE status = 'completed') as successful_runs,
                        COUNT(*) FILTER (WHERE status = 'failed') as failed_runs,
                        AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration,
                        MAX(start_time) as last_run_time
                    FROM pipeline_runs
                    WHERE pipeline_name = %s
                    AND start_time >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                """, (pipeline_name, lookback_days))
                stats = cur.fetchone()
                
                # Get recent alerts
                cur.execute("""
                    SELECT 
                        alert_type,
                        COUNT(*) as alert_count,
                        MAX(severity) as max_severity
                    FROM pipeline_alerts a
                    JOIN pipeline_runs r ON a.run_id = r.run_id
                    WHERE r.pipeline_name = %s
                    AND a.timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                    GROUP BY alert_type
                """, (pipeline_name, lookback_days))
                alerts = {
                    row[0]: {
                        "count": row[1],
                        "max_severity": row[2]
                    }
                    for row in cur.fetchall()
                }
                
                return {
                    "pipeline_name": pipeline_name,
                    "total_runs": stats[0],
                    "successful_runs": stats[1],
                    "failed_runs": stats[2],
                    "success_rate": stats[1] / stats[0] if stats[0] > 0 else 0,
                    "avg_duration_seconds": stats[3],
                    "last_run_time": stats[4],
                    "alerts": alerts
                }
                
        except Exception as e:
            logger.error(f"Failed to get pipeline status: {e}")
            return {}
    
    def get_pipeline_metrics(self,
                           pipeline_name: str,
                           metric_name: Optional[str] = None,
                           lookback_days: int = 7) -> List[Dict[str, Any]]:
        """Get metrics for recent pipeline runs.
        
        Args:
            pipeline_name: Name of the pipeline
            metric_name: Optional metric name to filter by
            lookback_days: Number of days to look back
            
        Returns:
            List[Dict]: List of metric values
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        m.metric_name,
                        m.metric_value,
                        m.metric_type,
                        m.timestamp,
                        r.run_id,
                        r.status as run_status
                    FROM pipeline_metrics m
                    JOIN pipeline_runs r ON m.run_id = r.run_id
                    WHERE r.pipeline_name = %s
                    AND m.timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                    AND (%s IS NULL OR m.metric_name = %s)
                    ORDER BY m.timestamp DESC
                """, (pipeline_name, lookback_days, metric_name, metric_name))
                
                return [
                    {
                        "metric_name": row[0],
                        "metric_value": row[1],
                        "metric_type": row[2],
                        "timestamp": row[3],
                        "run_id": row[4],
                        "run_status": row[5]
                    }
                    for row in cur.fetchall()
                ]
                
        except Exception as e:
            logger.error(f"Failed to get pipeline metrics: {e}")
            return []
    
    def get_active_alerts(self,
                         pipeline_name: Optional[str] = None,
                         severity: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get active pipeline alerts.
        
        Args:
            pipeline_name: Optional pipeline name to filter by
            severity: Optional severity level to filter by
            
        Returns:
            List[Dict]: List of active alerts
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        a.alert_id,
                        a.alert_type,
                        a.alert_message,
                        a.severity,
                        a.timestamp,
                        r.pipeline_name,
                        r.run_id
                    FROM pipeline_alerts a
                    JOIN pipeline_runs r ON a.run_id = r.run_id
                    WHERE a.status = 'active'
                    AND (%s IS NULL OR r.pipeline_name = %s)
                    AND (%s IS NULL OR a.severity = %s)
                    ORDER BY a.timestamp DESC
                """, (pipeline_name, pipeline_name, severity, severity))
                
                return [
                    {
                        "alert_id": row[0],
                        "alert_type": row[1],
                        "alert_message": row[2],
                        "severity": row[3],
                        "timestamp": row[4],
                        "pipeline_name": row[5],
                        "run_id": row[6]
                    }
                    for row in cur.fetchall()
                ]
                
        except Exception as e:
            logger.error(f"Failed to get active alerts: {e}")
            return []
    
    def resolve_alert(self, alert_id: int) -> None:
        """Mark an alert as resolved.
        
        Args:
            alert_id: ID of the alert to resolve
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_alerts 
                    SET status = 'resolved'
                    WHERE alert_id = %s
                """, (alert_id,))
                self.conn.commit()
                logger.info(f"Resolved alert {alert_id}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to resolve alert {alert_id}: {e}")
            raise 