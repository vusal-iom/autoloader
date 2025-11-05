"""
File Discovery Service - AWS S3 Implementation

Lists files from S3 bucket with pagination and metadata collection.
"""

from typing import List, Optional, Dict
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)


@dataclass
class FileMetadata:
    """Metadata for a discovered file"""
    path: str  # Full S3 path: s3://bucket/path/to/file.json
    size: int  # Size in bytes
    modified_at: datetime  # Last modified timestamp
    etag: str  # S3 ETag (hash) for change detection

    def to_dict(self) -> Dict:
        """Convert to dict for JSON serialization"""
        return {
            "path": self.path,
            "size": self.size,
            "modified_at": self.modified_at.isoformat(),
            "etag": self.etag
        }


class FileDiscoveryService:
    """
    Discovers files from AWS S3 bucket.

    Phase 1: S3 only
    Future phases: Azure Blob, GCS
    """

    def __init__(self, source_type: str, credentials: Dict, region: Optional[str] = None):
        """
        Initialize file discovery service.

        Args:
            source_type: Must be "S3" in Phase 1
            credentials: Dict with aws_access_key_id, aws_secret_access_key
            region: AWS region (default: us-east-1)
        """
        if source_type.upper() != "S3":
            raise ValueError(f"Unsupported source type for Phase 1: {source_type}. Only S3 is supported.")

        self.source_type = source_type
        self.credentials = credentials
        self.region = region or "us-east-1"
        self._client = None

    def list_files(
        self,
        bucket: str,
        prefix: str,
        pattern: Optional[str] = None,
        since: Optional[datetime] = None,
        max_files: Optional[int] = None
    ) -> List[FileMetadata]:
        """
        List files from S3 bucket.

        Args:
            bucket: S3 bucket name (e.g., "my-data-bucket")
            prefix: Path prefix (e.g., "data/events/")
            pattern: File pattern for filtering (e.g., "*.json") - NOT IMPLEMENTED IN PHASE 1
            since: Only return files modified after this date (optimization)
            max_files: Maximum number of files to return

        Returns:
            List of FileMetadata objects

        Raises:
            ClientError: If S3 API call fails
            NoCredentialsError: If credentials are invalid
        """
        logger.info(f"Listing files from S3: bucket={bucket}, prefix={prefix}")

        # Initialize S3 client lazily
        if not self._client:
            self._client = self._init_s3_client()

        files = []
        continuation_token = None

        try:
            while True:
                # Build request parameters
                kwargs = {
                    'Bucket': bucket,
                    'Prefix': prefix,
                    'MaxKeys': min(1000, max_files - len(files)) if max_files else 1000
                }

                if continuation_token:
                    kwargs['ContinuationToken'] = continuation_token

                # Call S3 list_objects_v2
                response = self._client.list_objects_v2(**kwargs)

                # Check if any objects returned
                if 'Contents' not in response:
                    logger.info(f"No objects found in s3://{bucket}/{prefix}")
                    break

                # Process each object
                for obj in response['Contents']:
                    # Apply date filter
                    if since and obj['LastModified'] < since:
                        continue

                    # Apply pattern filter (Phase 1: simple suffix check)
                    # TODO Phase 2: Support glob patterns
                    if pattern and not obj['Key'].endswith(pattern.replace("*", "")):
                        continue

                    # Build full S3 path (use s3a:// for Spark compatibility)
                    file_path = f"s3a://{bucket}/{obj['Key']}"

                    # Create FileMetadata
                    file_metadata = FileMetadata(
                        path=file_path,
                        size=obj['Size'],
                        modified_at=obj['LastModified'],
                        etag=obj['ETag'].strip('"')  # Remove quotes from ETag
                    )

                    files.append(file_metadata)

                    # Stop if we've reached max_files
                    if max_files and len(files) >= max_files:
                        logger.info(f"Reached max_files limit: {max_files}")
                        return files

                # Check if there are more results
                if not response.get('IsTruncated'):
                    break

                continuation_token = response.get('NextContinuationToken')

            logger.info(f"Discovered {len(files)} files from s3://{bucket}/{prefix}")
            return files

        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"S3 ClientError: {error_code} - {error_message}")
            raise

        except NoCredentialsError:
            logger.error("AWS credentials not found or invalid")
            raise

        except Exception as e:
            logger.error(f"Unexpected error listing S3 files: {e}", exc_info=True)
            raise

    def _init_s3_client(self):
        """Initialize boto3 S3 client with credentials"""
        try:
            # Extract credentials
            aws_access_key_id = self.credentials.get('aws_access_key_id')
            aws_secret_access_key = self.credentials.get('aws_secret_access_key')

            if not aws_access_key_id or not aws_secret_access_key:
                raise ValueError("Missing AWS credentials: aws_access_key_id and aws_secret_access_key required")

            # Create S3 client
            # Check if custom endpoint is provided (for MinIO, LocalStack, etc.)
            # Support both "endpoint" and "endpoint_url" for flexibility
            endpoint = self.credentials.get("endpoint_url") or self.credentials.get("endpoint")

            client_kwargs = {
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key,
                'region_name': self.region
            }

            if endpoint:
                client_kwargs['endpoint_url'] = endpoint
                logger.info(f"Initialized S3 client with custom endpoint: {endpoint}")
            else:
                logger.info(f"Initialized S3 client for region: {self.region}")

            client = boto3.client('s3', **client_kwargs)
            return client

        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise

    def discover_files_from_path(
        self,
        source_path: str,
        pattern: Optional[str] = None,
        since: Optional[datetime] = None,
        max_files: Optional[int] = None
    ) -> List[FileMetadata]:
        """
        Discover files from S3 path (convenience method that parses the path).

        Args:
            source_path: Full S3 path (e.g., "s3://bucket/prefix" or "s3a://bucket/prefix")
            pattern: File pattern for filtering (e.g., "*.json")
            since: Only return files modified after this date
            max_files: Maximum number of files to return

        Returns:
            List of FileMetadata objects

        Raises:
            ValueError: If source_path format is invalid
            ClientError: If S3 API call fails
        """
        # Parse S3 path (format: s3://bucket/prefix or s3a://bucket/prefix)
        if source_path.startswith("s3://"):
            source_path = source_path[5:]
        elif source_path.startswith("s3a://"):
            source_path = source_path[6:]
        else:
            raise ValueError(
                f"Invalid S3 path format: {source_path}. "
                f"Expected format: s3://bucket/prefix or s3a://bucket/prefix"
            )

        # Split into bucket and prefix
        parts = source_path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        # Use existing list_files method
        return self.list_files(
            bucket=bucket,
            prefix=prefix,
            pattern=pattern,
            since=since,
            max_files=max_files
        )

    def test_connection(self, bucket: str) -> bool:
        """
        Test S3 connection by checking if bucket is accessible.

        Args:
            bucket: S3 bucket name

        Returns:
            True if connection successful

        Raises:
            ClientError if connection fails
        """
        try:
            if not self._client:
                self._client = self._init_s3_client()

            # Simple head_bucket call to test connectivity
            self._client.head_bucket(Bucket=bucket)
            logger.info(f"Successfully connected to S3 bucket: {bucket}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket not found: {bucket}")
            elif error_code == '403':
                logger.error(f"Access denied to bucket: {bucket}")
            else:
                logger.error(f"S3 connection test failed: {error_code}")
            raise
