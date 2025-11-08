# app/main.py
from __future__ import annotations

import os
from datetime import datetime
import logging
from typing import Dict, Any, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import engine, test_connection, SessionLocal
from app.models import Base, Job, RunLog

# Adapters used by the scheduler
from adapters.kekahr import (
    fetch_kekahr_jobs,
    map_kekahr_job,
    _base_url_from_careers as keka_base,
)
from adapters.darwinbox import (
    fetch_darwinbox_jobs,
    map_darwinbox_job,
    base_url_from_careers as darwin_base,
)
from adapters.oracle_orc import (
    fetch_oracle_orc_jobs,
    map_oracle_orc_job,
    build_rest_base,
)
from adapters.join_com import fetch_join_dom_jobs, map_join_dom_job

# Scheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# -------------------------------------------------------------------
# Logging: console + run log (INFO only) + error log (ERROR+)
# -------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
RUN_LOG = os.path.join(DATA_DIR, "run.log")
ERROR_LOG = os.path.join(DATA_DIR, "error.log")

logger = logging.getLogger("ats-scheduler")
logger.setLevel(logging.DEBUG)  # capture everything, handlers will filter
logger.propagate = False

# Remove any previous handlers (important for --reload)
for h in list(logger.handlers):
    logger.removeHandler(h)

# Console handler (dev-friendly)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(console)

# Run file handler: only INFO and WARNING, exclude ERROR/EXCEPTION
class InfoOnlyFilter(logging.Filter):
    def filter(self, record):
        # Allow levels < ERROR (so INFO and WARNING pass). Exclude ERROR & CRITICAL.
        return record.levelno < logging.ERROR

run_file = logging.FileHandler(RUN_LOG, encoding="utf-8", mode="a")
run_file.setLevel(logging.INFO)
run_file.addFilter(InfoOnlyFilter())
run_file.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(run_file)

# Error file handler: ERROR and above (tracebacks here)
error_file = logging.FileHandler(ERROR_LOG, encoding="utf-8", mode="a")
error_file.setLevel(logging.ERROR)
error_file.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(error_file)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="ATS Scraper API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Normalized fields used across code
# ---------------------------------------------------------------------------
NORM_FIELDS = [
    "external_id", "ats_type", "company_name", "title", "department",
    "location_text", "remote_type", "employment_type", "posted_at",
    "updated_at_source", "apply_url", "source_url", "description_html",
]

# ---------------------------------------------------------------------------
# Bulk upsert + close detection (scheduler helper)
# ---------------------------------------------------------------------------
def _bulk_upsert_and_close(db, normalized: List[Dict[str, Any]]) -> Dict[str, int]:
    new_count = updated_count = closed_count = 0
    if not normalized:
        return {"new": 0, "updated": 0, "closed": 0}

    ats = normalized[0].get("ats_type")
    company = normalized[0].get("company_name")
    now = datetime.utcnow()

    incoming_extids = [j.get("external_id") for j in normalized if j.get("external_id")]
    incoming_set = set(incoming_extids)

    existing = db.query(Job).filter(Job.ats_type == ats, Job.external_id.in_(incoming_extids)).all() if incoming_extids else []
    existing_map = {f"{r.ats_type}|{r.external_id}": r for r in existing}

    for j in normalized:
        ext = j.get("external_id")
        key = f"{j.get('ats_type')}|{ext}"
        row = existing_map.get(key)
        if row is None:
            vals = {f: j.get(f) for f in NORM_FIELDS}
            vals.setdefault("apply_url", j.get("apply_url") or "")
            vals.setdefault("source_url", j.get("source_url") or "")
            vals["is_active"] = True
            vals["first_seen_at"] = now
            vals["last_seen_at"] = now
            row = Job(**vals)
            db.add(row)
            new_count += 1
        else:
            changed = False
            for f in NORM_FIELDS:
                new_val = j.get(f)
                if getattr(row, f) != new_val:
                    setattr(row, f, new_val)
                    changed = True
            if not getattr(row, "is_active", True):
                row.is_active = True
                row.last_seen_at = now
                changed = True
            else:
                row.last_seen_at = now
            if changed:
                updated_count += 1

    # mark missing jobs (same company+ats) as closed
    if company and ats:
        q = db.query(Job).filter(
            Job.ats_type == ats,
            Job.company_name == company,
            Job.external_id.isnot(None),
            Job.is_active == True
        )
        if incoming_set:
            q = q.filter(~Job.external_id.in_(list(incoming_set)))
        to_close = q.all()
        for r in to_close:
            r.is_active = False
            r.last_seen_at = now
            closed_count += 1

    db.commit()
    return {"new": new_count, "updated": updated_count, "closed": closed_count}

# ---------------------------------------------------------------------------
# RunLog helpers (persist scheduler run rows)
# ---------------------------------------------------------------------------
def _create_runlog(db, ats_type: str, company: str, endpoint: Optional[str] = None) -> RunLog:
    started = datetime.utcnow()
    rl = RunLog(
        ats_type=ats_type,
        company_name=company,
        started_at=started,
        endpoint=endpoint,
        status="running",
        fetched=0,
        new=0,
        updated=0,
        closed=0,
    )
    db.add(rl)
    db.commit()
    db.refresh(rl)
    return rl

def _finalize_runlog(db, runlog: RunLog, fetched: int, counts: Dict[str, int], status: str = "success", err: Optional[str] = None):
    runlog.fetched = fetched
    runlog.new = counts.get("new", 0)
    runlog.updated = counts.get("updated", 0)
    runlog.closed = counts.get("closed", 0)
    runlog.ended_at = datetime.utcnow()
    runlog.status = status
    if err:
        runlog.error = err
    db.commit()

# ---------------------------------------------------------------------------
# Scheduler job
# ---------------------------------------------------------------------------
def run_daily_job() -> None:
    logger.info("Scheduler triggered")
    db = SessionLocal()
    try:
        # ---------------- KEKAHR ----------------
        try:
            company = "10Decoders"
            careers_url = "https://10decoders.keka.com/careers/"
            endpoint = "https://10decoders.keka.com/careers/api/embedjobs/default/active/8a08095f-29fa-4856-ac19-f693dcf00ad1"

            runlog = _create_runlog(db, "kekahr", company, endpoint)
            res = fetch_kekahr_jobs(careers_url, endpoint_override=endpoint)
            base = keka_base(careers_url)
            raw = res.get("jobs", [])
            normalized = [map_kekahr_job(j, company, careers_url, base) for j in raw]
            counts = _bulk_upsert_and_close(db, normalized)
            _finalize_runlog(db, runlog, len(raw), counts, status="success")
            # This INFO line is written to run.log (only summary)
            logger.info(f"[kekahr] {company}: fetched={len(raw)} new={counts['new']} updated={counts['updated']} closed={counts['closed']}")
        except Exception as e:
            logger.exception("[kekahr] ERROR")
            try:
                _finalize_runlog(db, runlog, 0, {"new": 0, "updated": 0, "closed": 0}, status="error", err=str(e))
            except Exception:
                pass

        # ---------------- DARWINBOX ----------------
        try:
            company = "ADA"
            careers_url = "https://adaglobal.darwinbox.com/ms/candidate/careers"
            runlog = _create_runlog(db, "darwinbox", company, None)
            res = fetch_darwinbox_jobs(careers_url, page_size=50)
            base = darwin_base(careers_url)
            raw = res.get("jobs", [])
            normalized = [map_darwinbox_job(j, company, careers_url, base) for j in raw]
            counts = _bulk_upsert_and_close(db, normalized)
            _finalize_runlog(db, runlog, len(raw), counts, status="success")
            logger.info(f"[darwinbox] {company}: fetched={len(raw)} new={counts['new']} updated={counts['updated']} closed={counts['closed']}")
        except Exception as e:
            logger.exception("[darwinbox] ERROR")
            try:
                _finalize_runlog(db, runlog, 0, {"new": 0, "updated": 0, "closed": 0}, status="error", err=str(e))
            except Exception:
                pass

        # ---------------- ORACLE ORC ----------------
        try:
            company = "Euroclear"
            careers_url = "https://don.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1003/jobs"
            host = "don.fa.em2.oraclecloud.com"
            site = "CX_1003"
            rest_base = build_rest_base(host, site)
            runlog = _create_runlog(db, "oracle_orc", company, None)

            res = fetch_oracle_orc_jobs(
                careers_url=careers_url,
                rest_base=rest_base,
                site_number=site,
                limit=50,
                max_pages=3,
            )
            raw = res.get("jobs", [])
            normalized = [map_oracle_orc_job(j, company, careers_url, site) for j in raw]
            counts = _bulk_upsert_and_close(db, normalized)
            if res.get("last_endpoint"):
                runlog.endpoint = res.get("last_endpoint")
            _finalize_runlog(db, runlog, len(raw), counts, status="success")
            logger.info(f"[oracle_orc] {company}: fetched={len(raw)} new={counts['new']} updated={counts['updated']} closed={counts['closed']}")
        except Exception as e:
            logger.exception("[oracle_orc] ERROR")
            try:
                _finalize_runlog(db, runlog, 0, {"new": 0, "updated": 0, "closed": 0}, status="error", err=str(e))
            except Exception:
                pass

        # ---------------- JOIN ----------------
        try:
            company = "Qdrant"
            careers_url = "https://join.com/companies/qdrant"
            runlog = _create_runlog(db, "join", company, None)

            raw = fetch_join_dom_jobs(careers_url)
            normalized = [map_join_dom_job(j, company, careers_url) for j in raw]
            counts = _bulk_upsert_and_close(db, normalized)
            _finalize_runlog(db, runlog, len(raw), counts, status="success")
            logger.info(f"[join] {company}: fetched={len(raw)} new={counts['new']} updated={counts['updated']} closed={counts['closed']}")
        except Exception as e:
            logger.exception("[join] ERROR")
            try:
                _finalize_runlog(db, runlog, 0, {"new": 0, "updated": 0, "closed": 0}, status="error", err=str(e))
            except Exception:
                pass

        logger.info("Daily run finished successfully")
        logger.info("-" * 55)

    except Exception:
        logger.exception("[scheduler] FATAL ERROR")
    finally:
        db.close()

# ---------------------------------------------------------------------------
# Scheduler bootstrap
# ---------------------------------------------------------------------------
def _setup_scheduler() -> None:
    if getattr(app.state, "scheduler_started", False):
        return

    enabled = os.getenv("SCHEDULER_ENABLED", "true").lower() == "true"
    if not enabled:
        logger.info("[scheduler] DISABLED via SCHEDULER_ENABLED=false")
        return

    tz = os.getenv("SCHEDULER_TZ", "Asia/Kolkata")
    when = os.getenv("SCHEDULER_TIME", "09:00")
    try:
        hour, minute = [int(x) for x in when.split(":")]
    except Exception:
        hour, minute = 9, 0

    sched = BackgroundScheduler(timezone=tz)
    sched.add_job(
        run_daily_job,
        trigger=CronTrigger(hour=hour, minute=minute, timezone=tz),
        id="daily_ats_run",
        replace_existing=True,
    )
    sched.start()
    app.state.scheduler_started = True
    logger.info(f"[scheduler] ENABLED: set to run daily at {hour:02d}:{minute:02d} {tz}")

# ---------------------------------------------------------------------------
# Startup & Routes registration (import routes late to avoid circular)
# ---------------------------------------------------------------------------
from app.routes.jobs import router as jobs_router  # noqa: E402

@app.on_event("startup")
def on_startup():
    # keep these as debug so they don't pollute run.log
    logger.debug("Starting application - testing DB connection...")
    test_connection()
    logger.debug("DB connection OK")
    logger.debug(f"Using DB: {engine.url}")
    Base.metadata.create_all(bind=engine)
    logger.debug("DB tables are up-to-date")
    _setup_scheduler()

app.include_router(jobs_router, prefix="/api/jobs", tags=["jobs"])

@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok"}

@app.get("/", tags=["meta"])
def root():
    return {
        "service": "ATS Scraper API",
        "version": "1.0.0",
        "endpoints": [
            "/health",
            "/api/jobs",
            "/api/jobs/run/{ats}",
            "/api/jobs/__debug/db_url",
            "/api/jobs/summary",
        ],
        "now": datetime.utcnow().isoformat(timespec="seconds"),
    }
