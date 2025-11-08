# app/models.py
from __future__ import annotations

from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Boolean,
    DateTime,
    func,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("ats_type", "external_id", name="uq_jobs_ats_external"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)

    # normalized catalog
    external_id = Column(String, nullable=False)                   # tenant/job key
    ats_type     = Column(String, nullable=True)
    company_name = Column(String, nullable=True)
    title        = Column(String, nullable=True)
    department   = Column(String, nullable=True)
    location_text = Column(String, nullable=True)
    remote_type  = Column(String, nullable=True)
    employment_type = Column(String, nullable=True)
    posted_at    = Column(String, nullable=True)
    updated_at_source = Column(String, nullable=True)
    apply_url    = Column(Text, nullable=False)
    source_url   = Column(Text, nullable=False)
    description_html = Column(Text, nullable=True)

    # lifecycle flags — two complementary ways to track removal/reappearance
    is_active     = Column(Boolean, nullable=False, server_default="true", default=True)
    closed        = Column(Boolean, nullable=False, server_default="false", default=False)
    closed_at     = Column(DateTime(timezone=True), nullable=True)

    # history timestamps
    first_seen_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_seen_at  = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def mark_seen(self):
        """Helper: call when a job is present in a run."""
        self.is_active = True
        self.closed = False
        self.closed_at = None
        self.last_seen_at = datetime.utcnow()

    def mark_closed(self):
        """Helper: call when a job is deemed closed/removed."""
        if not self.closed:
            self.closed = True
            self.closed_at = datetime.utcnow()
        self.is_active = False
        self.last_seen_at = datetime.utcnow()

    def __repr__(self):
        return f"<Job({self.ats_type}:{self.external_id} '{(self.title or '')[:40]}')>"

class RunLog(Base):
    __tablename__ = "run_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ats_type = Column(String, nullable=False)
    company_name = Column(String, nullable=False)

    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    ended_at   = Column(DateTime(timezone=True), nullable=True)

    endpoint = Column(Text, nullable=True)
    fetched  = Column(Integer, nullable=False, default=0)
    new      = Column(Integer, nullable=False, default=0)
    updated  = Column(Integer, nullable=False, default=0)
    closed   = Column(Integer, nullable=False, default=0)

    status   = Column(String, nullable=False, default="running")  # running|success|error
    error    = Column(Text, nullable=True)

    def finish(self, fetched: int, new: int, updated: int, closed: int, status: str = "success", error: str | None = None):
        self.fetched = fetched
        self.new = new
        self.updated = updated
        self.closed = closed
        self.ended_at = datetime.utcnow()
        self.status = status
        self.error = error

    def __repr__(self):
        return f"<RunLog({self.ats_type}:{self.company_name} fetched={self.fetched} new={self.new} closed={self.closed})>"
