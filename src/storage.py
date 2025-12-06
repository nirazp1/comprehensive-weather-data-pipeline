"""Storage utilities for saving data to various backends (S3, local, etc.)."""
import json
import boto3
from pathlib import Path
from typing import Optional, Dict, Any
from .config import settings
import logging

logger = logging.getLogger(__name__)


class S3Storage:
    """S3 storage backend for weather data."""
    
    def __init__(
        self,
        bucket: Optional[str] = None,
        region: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None
    ):
        """
        Initialize S3 storage client.
        
        Args:
            bucket: S3 bucket name
            region: AWS region
            access_key: AWS access key ID
            secret_key: AWS secret access key
        """
        self.bucket = bucket or settings.s3_bucket
        self.region = region or settings.aws_region
        
        if not self.bucket:
            raise ValueError("S3 bucket name is required")
        
        # Initialize boto3 client
        client_kwargs = {"region_name": self.region}
        if access_key and secret_key:
            client_kwargs.update({
                "aws_access_key_id": access_key,
                "aws_secret_access_key": secret_key
            })
        
        self.client = boto3.client("s3", **client_kwargs)
        logger.info(f"Initialized S3 storage: bucket={self.bucket}, region={self.region}")
    
    def upload_json(self, key: str, data: Dict[str, Any], prefix: str = "raw/") -> str:
        """
        Upload JSON data to S3.
        
        Args:
            key: S3 object key (filename)
            data: Dictionary to upload as JSON
            prefix: Key prefix (e.g., "raw/", "processed/")
            
        Returns:
            S3 object key
        """
        full_key = f"{prefix.rstrip('/')}/{key}" if prefix else key
        body = json.dumps(data).encode("utf-8")
        
        self.client.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=body,
            ContentType="application/json"
        )
        
        logger.info(f"✓ Uploaded to s3://{self.bucket}/{full_key}")
        return full_key
    
    def upload_file(self, local_path: Path, key: str, prefix: str = "raw/") -> str:
        """
        Upload a local file to S3.
        
        Args:
            local_path: Path to local file
            key: S3 object key (filename)
            prefix: Key prefix
            
        Returns:
            S3 object key
        """
        full_key = f"{prefix.rstrip('/')}/{key}" if prefix else key
        
        self.client.upload_file(
            str(local_path),
            self.bucket,
            full_key
        )
        
        logger.info(f"✓ Uploaded file to s3://{self.bucket}/{full_key}")
        return full_key


class LocalStorage:
    """Local filesystem storage backend."""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """
        Initialize local storage.
        
        Args:
            base_dir: Base directory for storage
        """
        self.base_dir = base_dir or settings.raw_data_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized local storage: {self.base_dir}")
    
    def save_json(self, filename: str, data: Dict[str, Any], subdir: str = "") -> Path:
        """
        Save JSON data to local file.
        
        Args:
            filename: Filename
            data: Dictionary to save as JSON
            subdir: Subdirectory within base_dir
            
        Returns:
            Path to saved file
        """
        target_dir = self.base_dir / subdir if subdir else self.base_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        
        filepath = target_dir / filename
        filepath.write_text(json.dumps(data, indent=2))
        
        logger.info(f"✓ Saved to {filepath}")
        return filepath


def get_storage(backend: str = "local") -> LocalStorage | S3Storage:
    """
    Factory function to get storage backend.
    
    Args:
        backend: Storage backend type ("local" or "s3")
        
    Returns:
        Storage instance
    """
    if backend == "s3":
        try:
            return S3Storage()
        except Exception as e:
            logger.warning(f"Failed to initialize S3 storage: {e}. Falling back to local.")
            return LocalStorage()
    else:
        return LocalStorage()

