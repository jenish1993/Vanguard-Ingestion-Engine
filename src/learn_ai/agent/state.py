from typing import TypedDict, List, Optional, Any
from pyspark.sql import DataFrame

class PipelineState(TypedDict):
    file_path: str                  # Path to the source CSV file
    df: Optional[DataFrame]         # The active PySpark DataFrame being processed
    error: Optional[str]            # Details of the last raised exception
    last_failed_step: Optional[str] # Which step failed: "extract", "transform", or "load"
    heal_attempts: int              # Counter to prevent infinite loops
    max_attempts: int               # Limit on healing retries
    heal_history: List[dict]        # Log of AI-generated healing actions applied
    status: str                     # Pipeline status: "pending", "success", "failed"