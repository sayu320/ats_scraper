# app/schemas.py
from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel, ConfigDict

class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    external_id: str
    ats_type: str | None = None
    company_name: str | None = None
    title: str | None = None
    department: str | None = None
    location_text: str | None = None
    remote_type: str | None = None
    employment_type: str | None = None
    posted_at: str | None = None
    updated_at_source: str | None = None
    apply_url: str
    source_url: str
    description_html: str | None = None

    is_active: bool
    first_seen_at: datetime
    last_seen_at: datetime

class RunLogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    ats_type: str
    company_name: str
    started_at: datetime
    ended_at: datetime | None = None
    endpoint: str | None = None
    fetched: int
    new: int
    updated: int
    closed: int
    status: str
    error: str | None = None
