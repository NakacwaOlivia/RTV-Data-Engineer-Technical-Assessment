import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import os
from minio import Minio
from minio.error import S3Error
import json
from typing import Dict, List, Optional
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngestion:
    def __init__(self, source_path: str, minio_bucket: str):
        """
        Initialize the data ingestion process.
        
        Args:
            source_path: Path to the source data files
            minio_bucket: Name of the MinIO bucket to store processed data
        """
        self.source_path = Path(source_path)
        self.minio_bucket = minio_bucket
        self.minio_client = self._setup_minio()
        
    def _setup_minio(self) -> Minio:
        """Set up MinIO client connection."""
        try:
            client = Minio(
                "localhost:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False
            )
            
            # Create bucket if it doesn't exist
            if not client.bucket_exists(self.minio_bucket):
                client.make_bucket(self.minio_bucket)
                logger.info(f"Created bucket: {self.minio_bucket}")
                
            return client
        except S3Error as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            raise
            
    def _validate_file(self, file_path: Path) -> bool:
        """
        Validate the input file format and structure.
        
        Args:
            file_path: Path to the file to validate
            
        Returns:
            bool: True if file is valid, False otherwise
        """
        try:
            # Check file extension
            if file_path.suffix not in ['.csv', '.xlsx']:
                logger.warning(f"Unsupported file format: {file_path.suffix}")
                return False
                
            # Check if file exists
            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                return False
                
            # Check file size
            if file_path.stat().st_size == 0:
                logger.warning(f"Empty file: {file_path}")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error validating file {file_path}: {e}")
            return False
            
    def _read_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Read the input file into a pandas DataFrame.
        
        Args:
            file_path: Path to the file to read
            
        Returns:
            Optional[pd.DataFrame]: DataFrame containing the file data
        """
        try:
            if file_path.suffix == '.csv':
                df = pd.read_csv(file_path)
            else:  # .xlsx
                df = pd.read_excel(file_path)
                
            # Basic data validation
            if df.empty:
                logger.warning(f"Empty DataFrame from file: {file_path}")
                return None
                
            # Check required columns
            required_columns = ['household_id', 'survey_date', 'village_id']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing required columns in {file_path}: {missing_columns}")
                return None
                
            return df
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return None
            
    def _process_data(self, df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
        """
        Process the DataFrame with basic cleaning and validation.
        
        Args:
            df: Input DataFrame
            file_path: Path to the source file
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        try:
            # Create a copy to avoid modifying the original
            processed_df = df.copy()
            
            # Extract survey round from filename
            survey_round = file_path.stem.split('_')[0]
            processed_df['survey_round'] = survey_round
            
            # Convert date columns
            date_columns = [col for col in processed_df.columns if 'date' in col.lower()]
            for col in date_columns:
                processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
                
            # Handle missing values
            numeric_columns = processed_df.select_dtypes(include=[np.number]).columns
            processed_df[numeric_columns] = processed_df[numeric_columns].fillna(0)
            
            categorical_columns = processed_df.select_dtypes(include=['object']).columns
            processed_df[categorical_columns] = processed_df[categorical_columns].fillna('Unknown')
            
            # Add metadata
            processed_df['ingestion_date'] = datetime.now()
            processed_df['source_file'] = file_path.name
            
            return processed_df
        except Exception as e:
            logger.error(f"Error processing data from {file_path}: {e}")
            raise
            
    def _upload_to_minio(self, df: pd.DataFrame, file_path: Path) -> bool:
        """
        Upload processed data to MinIO.
        
        Args:
            df: Processed DataFrame
            file_path: Original source file path
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            # Convert DataFrame to parquet format
            parquet_data = df.to_parquet(index=False)
            
            # Generate object name
            object_name = f"processed/{file_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            
            # Upload to MinIO
            self.minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=object_name,
                data=parquet_data,
                length=len(parquet_data),
                content_type='application/octet-stream'
            )
            
            logger.info(f"Successfully uploaded {object_name} to MinIO")
            return True
        except Exception as e:
            logger.error(f"Error uploading to MinIO: {e}")
            return False
            
    def ingest_survey_data(self) -> Dict[str, int]:
        """
        Main ingestion function to process all survey data files.
        
        Returns:
            Dict[str, int]: Statistics about the ingestion process
        """
        stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'total_records': 0
        }
        
        try:
            # Process all files in the source directory
            for file_path in self.source_path.glob('*'):
                stats['total_files'] += 1
                
                if not self._validate_file(file_path):
                    stats['failed_files'] += 1
                    continue
                    
                # Read and process the file
                df = self._read_file(file_path)
                if df is None:
                    stats['failed_files'] += 1
                    continue
                    
                # Process the data
                processed_df = self._process_data(df, file_path)
                
                # Upload to MinIO
                if self._upload_to_minio(processed_df, file_path):
                    stats['processed_files'] += 1
                    stats['total_records'] += len(processed_df)
                else:
                    stats['failed_files'] += 1
                    
            # Log final statistics
            logger.info(f"Ingestion completed. Statistics: {json.dumps(stats, indent=2)}")
            return stats
            
        except Exception as e:
            logger.error(f"Error during ingestion process: {e}")
            raise

def ingest_survey_data(source_path: str, minio_bucket: str) -> Dict[str, int]:
    """
    Wrapper function for the data ingestion process.
    
    Args:
        source_path: Path to the source data files
        minio_bucket: Name of the MinIO bucket to store processed data
        
    Returns:
        Dict[str, int]: Statistics about the ingestion process
    """
    try:
        ingestion = DataIngestion(source_path, minio_bucket)
        return ingestion.ingest_survey_data()
    except Exception as e:
        logger.error(f"Failed to ingest survey data: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    source_path = "/path/to/survey/data"
    minio_bucket = "rtv-data"
    
    try:
        stats = ingest_survey_data(source_path, minio_bucket)
        print(f"Ingestion completed with stats: {json.dumps(stats, indent=2)}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1) 