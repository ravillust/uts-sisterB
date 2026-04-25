from pydantic import BaseModel, Field, field_validator
from typing import Any, Optional
from datetime import datetime, timezone
import re

class EventPayload(BaseModel):
    model_config = {"extra": "allow"}

class Event(BaseModel):
    topic: str = Field(..., min_length=1, max_length=255)
    event_id: str = Field(..., min_length=1, max_length=255)
    timestamp: str = Field(...)
    source: str = Field(..., min_length=1, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("topic")
    @classmethod
    def topic_no_spaces(cls, v: str) -> str:
        if not re.match(r'^[\w\-\.]+$', v):
            raise ValueError("Topic hanya boleh mengandung huruf, angka, underscore, dash, dan titik")
        return v
    
    @field_validator("timestamp")
    @classmethod
    def validate_iso8601(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("Timestamp harus dalam format ISO 8601, misalnya: 2024-06-01T12:00:00Z")
        return v
    
    @field_validator("event_id")
    @classmethod
    def event_id_no_spaces(cls, v: str) -> str:
        if " " in v:
            raise ValueError("Event ID tidak boleh mengandung spasi")
        return v
    
class PublishRequest(BaseModel):
    events: list[Event] 
   

class StatsResponse(BaseModel):
    received : int
    unique_processed : int
    duplicate_dropped : int
    topics: list[str]
    uptime_seconds : float

class EvenListResponse(BaseModel):
    topic: str
    count: int
    events: list[Event]