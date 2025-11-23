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


def classify_error(error: Exception) -> Dict[str, object]:
    """
    Classify exceptions into coarse categories with retry guidance.

    Returns dict with keys: category (FileErrorCategory), retryable, user_message
    """
    message = str(error) if error else "Unknown error"
    lower = message.lower()

    category = FileErrorCategory.UNKNOWN
    retryable = True
    user_message = message

    if "malformed" in lower or "bad record" in lower or "parse" in lower:
        category = FileErrorCategory.DATA_MALFORMED
        retryable = False
        user_message = "Malformed data encountered. Fix the source file or switch to PERMISSIVE mode."
    elif "schema" in lower or "cannot resolve" in lower or "analysisexception" in lower:
        category = FileErrorCategory.SCHEMA_MISMATCH
        retryable = False
        user_message = "Schema mismatch detected. Align schema or enable evolution before retrying."
    elif "no such bucket" in lower or "nosuchbucket" in lower:
        category = FileErrorCategory.BUCKET_NOT_FOUND
        retryable = False
        user_message = "Source bucket not found. Verify bucket name and permissions."
    elif (
        "nosuchkey" in lower
        or "not found" in lower
        or "does not exist" in lower
    ):
        category = FileErrorCategory.PATH_NOT_FOUND
        retryable = False
        user_message = "Source path not found. Verify bucket/key/prefix and retry."
    elif (
        "unrecognized option" in lower
        or "unsupported option" in lower
        or "invalid" in lower
        or "not a valid" in lower
        or "for input string" in lower
        or "numberformatexception" in lower
    ):
        category = FileErrorCategory.FORMAT_OPTIONS_INVALID
        retryable = False
        user_message = "Invalid format options. Check mode/options for the reader."
    elif "inference" in lower or "unable to infer" in lower or "requires that the schema" in lower:
        category = FileErrorCategory.SCHEMA_INFERENCE_FAILURE
        retryable = False
        user_message = "Schema inference failed. Provide an explicit schema or fix the input."
    elif "access denied" in lower or "permission" in lower or "unauthorized" in lower or "forbidden" in lower:
        category = FileErrorCategory.AUTH
        retryable = False
        user_message = "Authentication/authorization failed when accessing the source. Check credentials."
    elif "connection" in lower or "timeout" in lower or "timed out" in lower:
        category = FileErrorCategory.CONNECTIVITY
        retryable = True
        user_message = "Temporary connectivity issue. Safe to retry."
    elif "write" in lower or "iceberg" in lower:
        category = FileErrorCategory.WRITE_FAILURE
        retryable = True
        user_message = "Failed to write to the destination table. Retry or check destination health."

    return {
        "category": category,
        "retryable": retryable,
        "user_message": user_message
    }


def wrap_error(file_path: str, error: Exception) -> FileProcessingError:
    """Wrap arbitrary exceptions into a predictable FileProcessingError."""
    classification = classify_error(error)
    return FileProcessingError(
        category=classification["category"],
        retryable=classification["retryable"],
        user_message=classification["user_message"],
        raw_error=str(error),
        file_path=file_path
    )
