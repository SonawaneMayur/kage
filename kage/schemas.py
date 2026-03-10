"""
KAGE Event Schemas - Universal JSON contract for ALL languages
KAGE-PROPRIETARY-2026-v1.1
"""
from datetime import datetime
from typing import Optional, Literal, Dict, Any, List
from pydantic import BaseModel, Field

class BaseEvent(BaseModel):
    """Universal base event - ANY language emits this JSON"""
    event_timestamp: datetime = Field(..., description="UTC timestamp")
    event_type: Literal["job_run", "task_run", "dataset_event"]
    log_level: Literal["INFO", "WARN", "ERROR"] = "INFO"
    platform: str = Field(..., description="pyspark|databricks|scala|dbt|airflow")
    language: str = Field(..., description="python|scala|java|sql")
    environment: str = Field(..., description="dev|qa|prod")
    pipeline_name: str
    pipeline_id: str
    run_id: str
    custom_fields: Dict[str, Any] = Field(default_factory=dict)
    kage_signature: str = Field(default="KAGE-PROPRIETARY-2026-v1.1")
    
    class Config:
        frozen = True  # Audit integrity
        json_encoders = {datetime: lambda v: v.isoformat()}

class JobRunEvent(BaseEvent):
    event_type: Literal["job_run"] = "job_run"
    job_run_id: str
    status: Literal["RUNNING", "SUCCESS", "FAILED", "SKIPPED", "CANCELLED"]
    start_time: datetime
    end_time: Optional[datetime] = None

class TaskRunEvent(BaseEvent):
    event_type: Literal["task_run"] = "task_run"
    job_run_id: str
    task_run_id: str
    layer: Literal["landing", "bronze", "silver", "gold"]
    task_name: str
    status: Literal["RUNNING", "SUCCESS", "FAILED"]
    input_record_count: Optional[int] = None
    output_record_count: Optional[int] = None

class DatasetEvent(BaseEvent):
    event_type: Literal["dataset_event"] = "dataset_event"
    job_run_id: str
    task_run_id: Optional[str] = None
    dataset_event_id: str
    layer: Literal["landing", "bronze", "silver", "gold"]
    event_action: Literal["READ", "WRITE", "DELETE", "MERGE"]
    dataset_name: str
    dataset_type: Literal["table", "file", "stream"] = "table"
    record_count: Optional[int] = None
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    path: Optional[str] = None
    upstream_datasets: List[str] = Field(default_factory=list)
