from minio import Minio
from datetime import datetime
import os
import json
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class DataLakeManager:
    """Manages interactions with the data lake (MinIO) including versioning and metadata."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: str, secure: bool = True):
        """Initialize the data lake manager.
        
        Args:
            endpoint: MinIO server endpoint
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket_name: Name of the bucket to use
            secure: Whether to use HTTPS
        """
        self.client = Minio(endpoint, access_key, secret_key, secure=secure)
        self.bucket = bucket_name
        self._ensure_bucket()
        logger.info(f"Initialized DataLakeManager for bucket: {bucket_name}")
    
    def _ensure_bucket(self) -> None:
        """Ensure the bucket exists, create if it doesn't."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket, location="us-east-1")
                logger.info(f"Created bucket: {self.bucket}")
        except Exception as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
            raise
    
    def store_raw_data(self, file_path: str, metadata: Dict, prefix: str = "raw") -> str:
        """Store raw data with metadata and versioning.
        
        Args:
            file_path: Path to the file to store
            metadata: Dictionary of metadata to store with the file
            prefix: Prefix for the object name in the bucket
            
        Returns:
            str: The object name in the bucket
        """
        try:
            # Generate object name with date-based path
            date_path = datetime.now().strftime('%Y/%m/%d')
            file_name = os.path.basename(file_path)
            object_name = f"{prefix}/{date_path}/{file_name}"
            
            # Add standard metadata
            metadata.update({
                'upload_date': datetime.now().isoformat(),
                'original_filename': file_name
            })
            
            # Store the file
            self.client.fput_object(
                self.bucket,
                object_name,
                file_path,
                metadata=metadata
            )
            logger.info(f"Stored {file_name} as {object_name}")
            return object_name
            
        except Exception as e:
            logger.error(f"Failed to store {file_path}: {e}")
            raise
    
    def get_latest_version(self, prefix: str) -> Optional[str]:
        """Get the latest version of a file by prefix.
        
        Args:
            prefix: The prefix to search for
            
        Returns:
            Optional[str]: The object name of the latest version, or None if not found
        """
        try:
            objects = list(self.client.list_objects(self.bucket, prefix=prefix, recursive=True))
            if not objects:
                return None
            latest = max(objects, key=lambda x: x.last_modified)
            return latest.object_name
        except Exception as e:
            logger.error(f"Failed to get latest version for {prefix}: {e}")
            return None
    
    def get_file_metadata(self, object_name: str) -> Dict:
        """Get metadata for a stored file.
        
        Args:
            object_name: Name of the object in the bucket
            
        Returns:
            Dict: The metadata dictionary
        """
        try:
            stat = self.client.stat_object(self.bucket, object_name)
            return stat.metadata
        except Exception as e:
            logger.error(f"Failed to get metadata for {object_name}: {e}")
            return {}
    
    def list_files(self, prefix: str = "raw", recursive: bool = True) -> list:
        """List files in the bucket with a given prefix.
        
        Args:
            prefix: The prefix to filter by
            recursive: Whether to list recursively
            
        Returns:
            list: List of object names
        """
        try:
            objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=recursive)
            return [obj.object_name for obj in objects]
        except Exception as e:
            logger.error(f"Failed to list files with prefix {prefix}: {e}")
            return []
    
    def delete_file(self, object_name: str) -> bool:
        """Delete a file from the bucket.
        
        Args:
            object_name: Name of the object to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.client.remove_object(self.bucket, object_name)
            logger.info(f"Deleted {object_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete {object_name}: {e}")
            return False 