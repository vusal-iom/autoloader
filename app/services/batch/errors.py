from enum import Enum
from dataclasses import dataclass
from typing import Dict

class FileErrorCategory(str, Enum):
    """Predefined error categories for file processing."""
    DATA_MALFORMED = "data_malformed"
    SCHEMA_MISMATCH = "schema_mismatch"
    AUTH = "auth"
    CONNECTIVITY = "connectivity"
    WRITE_FAILURE = "write_failure"
    PATH_NOT_FOUND = "path_not_found"
    BUCKET_NOT_FOUND = "bucket_not_found"
    FORMAT_OPTIONS_INVALID = "format_options_invalid"
    SCHEMA_INFERENCE_FAILURE = "schema_inference_failure"
    UNKNOWN = "unknown"


@dataclass
class FileProcessingError(Exception):
    """Predictable, user-facing file processing error."""
    category: FileErrorCategory
    retryable: bool
    user_message: str
    raw_error: str
    file_path: str

    def __str__(self):
        return f"[{self.category.value}] {self.user_message}: {self.raw_error}"
