"""Test data generation utilities"""
import json
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List
from uuid import uuid4


def generate_sample_record(batch_id: int, record_index: int) -> Dict[str, Any]:
    """Generate a single sample JSON record for testing"""
    base_time = datetime.utcnow() - timedelta(days=1)
    record_time = base_time + timedelta(seconds=record_index * 10)

    event_types = ["page_view", "click", "purchase", "signup", "logout"]

    return {
        "id": f"evt_{batch_id}_{record_index}_{uuid4().hex[:8]}",
        "timestamp": record_time.isoformat() + "Z",
        "user_id": f"user_{random.randint(1000, 9999)}",
        "event_type": random.choice(event_types),
        "properties": {
            "page": f"/page/{random.randint(1, 100)}",
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
            "device": random.choice(["desktop", "mobile", "tablet"]),
            "session_id": f"sess_{uuid4().hex[:16]}",
            "duration_ms": random.randint(100, 10000),
            "batch_id": batch_id
        },
        "metadata": {
            "ip_address": f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}",
            "user_agent": "Mozilla/5.0 (Test Agent)",
            "referrer": f"https://example.com/ref/{random.randint(1, 50)}"
        }
    }


def generate_sample_data(num_records: int, batch_id: int) -> List[Dict[str, Any]]:
    """Generate multiple sample records for a batch"""
    return [generate_sample_record(batch_id, i) for i in range(num_records)]


def generate_json_file_content(num_records: int, batch_id: int, multiline: bool = False) -> str:
    """Generate JSON file content (newline-delimited or array format)"""
    records = generate_sample_data(num_records, batch_id)

    if multiline:
        # JSON array format
        return json.dumps(records, indent=2)
    else:
        # Newline-delimited JSON (default)
        return '\n'.join([json.dumps(record) for record in records])


def generate_csv_file_content(num_records: int, batch_id: int) -> str:
    """Generate CSV file content for testing"""
    records = generate_sample_data(num_records, batch_id)

    # Simple CSV with flattened structure
    lines = ["id,timestamp,user_id,event_type,page,browser,device"]

    for record in records:
        line = f"{record['id']},{record['timestamp']},{record['user_id']},{record['event_type']},"
        line += f"{record['properties']['page']},{record['properties']['browser']},{record['properties']['device']}"
        lines.append(line)

    return '\n'.join(lines)


def upload_test_files_to_s3(
    s3_client,
    bucket: str,
    prefix: str,
    num_files: int = 3,
    records_per_file: int = 1000,
    file_format: str = "json"
) -> List[str]:
    """
    Upload test files to S3 (or MinIO)

    Returns:
        List of uploaded file keys
    """
    uploaded_keys = []

    for i in range(num_files):
        if file_format == "json":
            content = generate_json_file_content(records_per_file, i)
            file_key = f'{prefix}batch_{i}.json'
        elif file_format == "csv":
            content = generate_csv_file_content(records_per_file, i)
            file_key = f'{prefix}batch_{i}.csv'
        else:
            raise ValueError(f"Unsupported format: {file_format}")

        s3_client.put_object(
            Bucket=bucket,
            Key=file_key,
            Body=content.encode('utf-8')
        )

        uploaded_keys.append(file_key)

    return uploaded_keys


def verify_data_integrity(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Verify data integrity and return statistics

    Returns:
        Dict with statistics: total_records, unique_ids, duplicate_count, etc.
    """
    total_records = len(records)

    # Check for duplicates
    ids = [r.get('id') for r in records]
    unique_ids = len(set(ids))
    duplicate_count = total_records - unique_ids

    # Check for null/missing required fields
    required_fields = ['id', 'timestamp', 'user_id', 'event_type']
    missing_fields = {}

    for field in required_fields:
        missing_count = sum(1 for r in records if not r.get(field))
        if missing_count > 0:
            missing_fields[field] = missing_count

    # Event type distribution
    event_types = {}
    for record in records:
        event_type = record.get('event_type', 'unknown')
        event_types[event_type] = event_types.get(event_type, 0) + 1

    return {
        'total_records': total_records,
        'unique_ids': unique_ids,
        'duplicate_count': duplicate_count,
        'missing_fields': missing_fields,
        'event_type_distribution': event_types,
        'has_issues': duplicate_count > 0 or len(missing_fields) > 0
    }
