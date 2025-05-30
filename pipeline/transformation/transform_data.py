import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import os
from minio import Minio
from minio.error import S3Error
import json
from typing import Dict, List, Optional, Tuple
import sys
from sqlalchemy import create_engine, text
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformation:
    def __init__(self, minio_bucket: str, db_conn_id: str):
        """
        Initialize the data transformation process.
        
        Args:
            minio_bucket: Name of the MinIO bucket containing processed data
            db_conn_id: Database connection identifier
        """
        self.minio_bucket = minio_bucket
        self.db_conn_id = db_conn_id
        self.minio_client = self._setup_minio()
        self.db_engine = self._setup_database()
        
    def _setup_minio(self) -> Minio:
        """Set up MinIO client connection."""
        try:
            client = Minio(
                "localhost:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False
            )
            
            if not client.bucket_exists(self.minio_bucket):
                raise S3Error(f"Bucket {self.minio_bucket} does not exist")
                
            return client
        except S3Error as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            raise
            
    def _setup_database(self) -> create_engine:
        """Set up database connection."""
        try:
            engine = create_engine('postgresql://postgres:password@localhost:5432/airflow')
            return engine
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def _load_from_minio(self) -> List[pd.DataFrame]:
        """
        Load all processed parquet files from MinIO.
        
        Returns:
            List[pd.DataFrame]: List of DataFrames containing the processed data
        """
        dfs = []
        try:
            # List all objects in the processed directory
            objects = self.minio_client.list_objects(
                self.minio_bucket,
                prefix="processed/",
                recursive=True
            )
            
            for obj in objects:
                if obj.object_name.endswith('.parquet'):
                    # Get the object data
                    data = self.minio_client.get_object(
                        self.minio_bucket,
                        obj.object_name
                    )
                    
                    # Read parquet data into DataFrame
                    df = pd.read_parquet(io.BytesIO(data.read()))
                    dfs.append(df)
                    
            return dfs
        except Exception as e:
            logger.error(f"Error loading data from MinIO: {e}")
            raise
            
    def _transform_households(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform household data.
        
        Args:
            df: Input DataFrame containing household data
            
        Returns:
            pd.DataFrame: Transformed household data
        """
        try:
            # Select and rename columns
            household_df = df[[
                'household_id',
                'village_id',
                'household_code',
                'head_of_household',
                'household_size'
            ]].copy()
            
            # Clean and standardize data
            household_df['household_code'] = household_df['household_code'].str.upper()
            household_df['head_of_household'] = household_df['head_of_household'].str.title()
            household_df['household_size'] = pd.to_numeric(household_df['household_size'], errors='coerce')
            
            # Remove duplicates
            household_df = household_df.drop_duplicates(subset=['household_id'])
            
            return household_df
        except Exception as e:
            logger.error(f"Error transforming household data: {e}")
            raise
            
    def _transform_surveys(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform survey data.
        
        Args:
            df: Input DataFrame containing survey data
            
        Returns:
            pd.DataFrame: Transformed survey data
        """
        try:
            # Select and rename columns
            survey_df = df[[
                'household_id',
                'survey_round',
                'survey_date',
                'surveyor_id',
                'status'
            ]].copy()
            
            # Clean and standardize data
            survey_df['survey_date'] = pd.to_datetime(survey_df['survey_date'])
            survey_df['status'] = survey_df['status'].str.lower()
            
            # Add validation
            survey_df = survey_df[survey_df['survey_date'].notna()]
            
            return survey_df
        except Exception as e:
            logger.error(f"Error transforming survey data: {e}")
            raise
            
    def _transform_measurements(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Transform measurement data and indicators.
        
        Args:
            df: Input DataFrame containing measurement data
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Transformed measurement and indicator data
        """
        try:
            # Extract indicator information
            indicator_columns = [col for col in df.columns if col.startswith('indicator_')]
            indicator_df = pd.DataFrame({
                'indicator_id': [col.replace('indicator_', '') for col in indicator_columns],
                'name': [col.replace('indicator_', '').replace('_', ' ').title() for col in indicator_columns],
                'category': [col.split('_')[1] if len(col.split('_')) > 2 else 'General' for col in indicator_columns],
                'unit': 'Numeric',  # Default unit, should be customized based on actual data
                'is_positive': True  # Default value, should be customized based on actual data
            })
            
            # Transform measurements
            measurement_df = pd.melt(
                df,
                id_vars=['household_id', 'survey_round'],
                value_vars=indicator_columns,
                var_name='indicator_id',
                value_name='value'
            )
            
            # Clean indicator IDs
            measurement_df['indicator_id'] = measurement_df['indicator_id'].str.replace('indicator_', '')
            
            # Add survey_id (this should be joined with the survey table)
            measurement_df['survey_id'] = measurement_df.groupby(['household_id', 'survey_round']).ngroup()
            
            return measurement_df, indicator_df
        except Exception as e:
            logger.error(f"Error transforming measurement data: {e}")
            raise
            
    def _load_to_database(self, tables: Dict[str, pd.DataFrame]) -> None:
        """
        Load transformed data into the database.
        
        Args:
            tables: Dictionary of table names and their corresponding DataFrames
        """
        try:
            for table_name, df in tables.items():
                # Load data to database
                df.to_sql(
                    table_name,
                    self.db_engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                logger.info(f"Successfully loaded {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Error loading data to database: {e}")
            raise
            
    def transform_survey_data(self) -> Dict[str, int]:
        """
        Main transformation function to process all survey data.
        
        Returns:
            Dict[str, int]: Statistics about the transformation process
        """
        stats = {
            'households_processed': 0,
            'surveys_processed': 0,
            'measurements_processed': 0,
            'indicators_processed': 0
        }
        
        try:
            # Load data from MinIO
            dfs = self._load_from_minio()
            if not dfs:
                raise ValueError("No data found in MinIO")
                
            # Combine all DataFrames
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Transform data
            household_df = self._transform_households(combined_df)
            survey_df = self._transform_surveys(combined_df)
            measurement_df, indicator_df = self._transform_measurements(combined_df)
            
            # Update statistics
            stats['households_processed'] = len(household_df)
            stats['surveys_processed'] = len(survey_df)
            stats['measurements_processed'] = len(measurement_df)
            stats['indicators_processed'] = len(indicator_df)
            
            # Load to database
            tables = {
                'households': household_df,
                'household_surveys': survey_df,
                'household_measurements': measurement_df,
                'indicators': indicator_df
            }
            
            self._load_to_database(tables)
            
            # Log final statistics
            logger.info(f"Transformation completed. Statistics: {json.dumps(stats, indent=2)}")
            return stats
            
        except Exception as e:
            logger.error(f"Error during transformation process: {e}")
            raise

def transform_survey_data(minio_bucket: str, db_conn_id: str) -> Dict[str, int]:
    """
    Wrapper function for the data transformation process.
    
    Args:
        minio_bucket: Name of the MinIO bucket containing processed data
        db_conn_id: Database connection identifier
        
    Returns:
        Dict[str, int]: Statistics about the transformation process
    """
    try:
        transformation = DataTransformation(minio_bucket, db_conn_id)
        return transformation.transform_survey_data()
    except Exception as e:
        logger.error(f"Failed to transform survey data: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    minio_bucket = "rtv-data"
    db_conn_id = "postgres_default"
    
    try:
        stats = transform_survey_data(minio_bucket, db_conn_id)
        print(f"Transformation completed with stats: {json.dumps(stats, indent=2)}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1) 