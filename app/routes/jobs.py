# app/routes/jobs.py
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import not_
import datetime

from app.database import get_db, engine
from app.models import Job, RunLog
from app.schemas import JobOut

# Adapters
from adapters.join_com import fetch_join_dom_jobs, map_join_dom_job
from adapters.kekahr import fetch_kekahr_jobs, map_kekahr_job, _base_url_from_careers as keka_base
from adapters.darwinbox import fetch_darwinbox_jobs, map_darwinbox_job, base_url_from_careers as darwin_base
from adapters.oracle_orc import fetch_oracle_orc_jobs, map_oracle_orc_job, build_rest_base

router = APIRouter()

class RunResult(BaseModel):
    adapter: str
    company: str
    endpoint: Optional[str] = None
    fetched: int
    new: int
    updated: int
    closed: int = 0

# Normalized fields we store
NORM_FIELDS = [
    "external_id","ats_type","company_name","title","department","location_text",
    "remote_type","employment_type","posted_at","updated_at_source","apply_url",
    "source_url","description_html"
]

# ------------------ Upsert + close detection ------------------
def _upsert_jobs(db: Session, normalized: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Bulk upsert keyed by (ats_type, external_id), and mark missing previous jobs as closed.
    Returns counts: new, updated, closed.
    Uses Job.is_active and Job.last_seen_at to represent lifecycle state.
    """
    new_count = updated_count = closed_count = 0
    if not normalized:
        return {"new": 0, "updated": 0, "closed": 0}

    # Determine ats+company for this run (assumes normalized items share them)
    ats = normalized[0].get("ats_type")
    company = normalized[0].get("company_name")

    # collect incoming external_ids (non-empty)
    incoming_extids = [j.get("external_id") for j in normalized if j.get("external_id")]
    incoming_set = set(incoming_extids)

    # preload existing rows for these external_ids (only same ats)
    existing = db.query(Job).filter(Job.ats_type == ats, Job.external_id.in_(incoming_extids)).all()
    existing_map = {f"{row.ats_type}|{row.external_id}": row for row in existing}

    now = datetime.datetime.utcnow()

    for j in normalized:
        ext = j.get("external_id")
        key = f"{j.get('ats_type')}|{ext}"
        row = existing_map.get(key)
        if row is None:
            # insert new
            vals = {f: j.get(f) for f in NORM_FIELDS}
            vals["is_active"] = True
            vals["first_seen_at"] = now
            vals["last_seen_at"] = now
            # ensure required text fields exist to avoid None issues
            vals.setdefault("apply_url", j.get("apply_url") or "")
            vals.setdefault("source_url", j.get("source_url") or "")
            row = Job(**vals)
            db.add(row)
            new_count += 1
        else:
            # update if changed
            changed = False
            for f in NORM_FIELDS:
                new_val = j.get(f)
                if getattr(row, f) != new_val:
                    setattr(row, f, new_val)
                    changed = True
            # If previously inactive, reopen it
            if not getattr(row, "is_active", True):
                row.is_active = True
                row.last_seen_at = now
                changed = True
            else:
                # update last_seen_at on any successful sighting
                row.last_seen_at = now
            if changed:
                updated_count += 1

    # Now detect jobs that belong to this company+ats but are NOT in incoming_set → mark closed
    # Only mark those currently active (is_active == True)
    if company and ats:
        # select rows for same company+ats and external_id NOT in incoming_set and currently active
        to_close_q = db.query(Job).filter(
            Job.ats_type == ats,
            Job.company_name == company,
            Job.external_id.isnot(None),
            not_(Job.external_id.in_(list(incoming_set))),
            Job.is_active == True
        )
        to_close = to_close_q.all()
        for r in to_close:
            r.is_active = False
            r.last_seen_at = now
            closed_count += 1

    db.commit()
    return {"new": new_count, "updated": updated_count, "closed": closed_count}


# ------------------ Read endpoints ------------------
@router.get("/", response_model=List[JobOut])
def list_jobs(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    company: str | None = Query(None, description="Filter by company_name (ILIKE)"),
    ats: str | None = Query(None, description="Filter by ats_type (kekahr, darwinbox, oracle_orc, join)"),
    title: str | None = Query(None, description="Search in title (ILIKE)"),
    db: Session = Depends(get_db),
):
    q = db.query(Job)
    if company:
        q = q.filter(Job.company_name.ilike(f"%{company}%"))
    if ats:
        q = q.filter(Job.ats_type == ats)
    if title:
        q = q.filter(Job.title.ilike(f"%{title}%"))
    rows = q.order_by(Job.id.desc()).limit(limit).offset(offset).all()
    return rows

@router.get("/summary")
def jobs_summary(db: Session = Depends(get_db)):
    from sqlalchemy import func
    total = db.query(func.count(Job.id)).scalar()
    by_ats = (
        db.query(Job.ats_type, func.count(Job.id))
        .group_by(Job.ats_type)
        .all()
    )
    by_company = (
        db.query(Job.company_name, func.count(Job.id))
        .group_by(Job.company_name)
        .order_by(func.count(Job.id).desc())
        .limit(20)
        .all()
    )
    return {
        "total": total,
        "by_ats": {k or "unknown": v for k, v in by_ats},
        "top_companies": [{ "company": c, "count": n } for c, n in by_company],
    }

@router.get("/{job_id}", response_model=JobOut)
def get_job(job_id: int, db: Session = Depends(get_db)):
    row = db.query(Job).get(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return row


# ------------------ Runner endpoints ------------------
@router.get("/run/join", response_model=RunResult)
def run_join(
    company: str = Query(..., description="Company name"),
    careers_url: str = Query(..., description="Join.com companies URL (e.g. https://join.com/companies/qdrant)"),
    db: Session = Depends(get_db),
):
    started = datetime.datetime.utcnow()
    run_log = RunLog(ats_type="join", company_name=company, started_at=started, status="running", endpoint=None)
    db.add(run_log)
    db.commit()
    db.refresh(run_log)

    try:
        raw = fetch_join_dom_jobs(careers_url)
        normalized = [map_join_dom_job(j, company, careers_url) for j in raw]
        counts = _upsert_jobs(db, normalized)

        run_log.fetched = len(raw)
        run_log.new = counts.get("new", 0)
        run_log.updated = counts.get("updated", 0)
        run_log.closed = counts.get("closed", 0)
        run_log.ended_at = datetime.datetime.utcnow()
        run_log.status = "success"
        db.commit()

        return RunResult(adapter="join", company=company, endpoint=None,
                         fetched=len(raw), new=counts["new"], updated=counts["updated"], closed=counts.get("closed", 0))
    except Exception as e:
        run_log.ended_at = datetime.datetime.utcnow()
        run_log.status = "error"
        run_log.error = str(e)
        db.commit()
        raise HTTPException(status_code=500, detail=f"JOIN run failed: {e}")


@router.get("/run/kekahr", response_model=RunResult)
def run_kekahr(
    company: str = Query(..., description="Company name"),
    careers_url: str = Query(..., description="https://<tenant>.keka.com/careers/"),
    endpoint: Optional[str] = Query(None, description="Full embedjobs endpoint (optional)"),
    db: Session = Depends(get_db),
):
    started = datetime.datetime.utcnow()
    run_log = RunLog(ats_type="kekahr", company_name=company, started_at=started, status="running", endpoint=endpoint)
    db.add(run_log)
    db.commit()
    db.refresh(run_log)

    try:
        res = fetch_kekahr_jobs(careers_url, endpoint_override=endpoint)
        base = keka_base(careers_url)
        raw = res.get("jobs", [])
        normalized = [map_kekahr_job(j, company, careers_url, base) for j in raw]
        counts = _upsert_jobs(db, normalized)

        run_log.fetched = len(raw)
        run_log.new = counts.get("new", 0)
        run_log.updated = counts.get("updated", 0)
        run_log.closed = counts.get("closed", 0)
        run_log.ended_at = datetime.datetime.utcnow()
        run_log.status = "success"
        db.commit()

        return RunResult(
            adapter="kekahr",
            company=company,
            endpoint=res.get("endpoint"),
            fetched=len(raw),
            new=counts["new"],
            updated=counts["updated"],
            closed=counts.get("closed", 0),
        )
    except Exception as e:
        run_log.ended_at = datetime.datetime.utcnow()
        run_log.status = "error"
        run_log.error = str(e)
        db.commit()
        raise HTTPException(status_code=500, detail=f"KEKAHR run failed: {e}")


@router.get("/run/darwinbox", response_model=RunResult)
def run_darwinbox(
    company: str = Query(..., description="Company name"),
    careers_url: str = Query(..., description="https://<tenant>.darwinbox.com/ms/candidate/careers"),
    page_size: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
):
    started = datetime.datetime.utcnow()
    run_log = RunLog(ats_type="darwinbox", company_name=company, started_at=started, status="running", endpoint=None)
    db.add(run_log)
    db.commit()
    db.refresh(run_log)

    try:
        res = fetch_darwinbox_jobs(careers_url, page_size=page_size)
        base = darwin_base(careers_url)
        raw = res.get("jobs", [])
        normalized = [map_darwinbox_job(j, company, careers_url, base) for j in raw]
        counts = _upsert_jobs(db, normalized)

        run_log.fetched = len(raw)
        run_log.new = counts.get("new", 0)
        run_log.updated = counts.get("updated", 0)
        run_log.closed = counts.get("closed", 0)
        run_log.ended_at = datetime.datetime.utcnow()
        run_log.status = "success"
        db.commit()

        return RunResult(adapter="darwinbox", company=company, endpoint=res.get("endpoint"),
                         fetched=len(raw), new=counts["new"], updated=counts["updated"], closed=counts.get("closed", 0))
    except Exception as e:
        run_log.ended_at = datetime.datetime.utcnow()
        run_log.status = "error"
        run_log.error = str(e)
        db.commit()
        raise HTTPException(status_code=500, detail=f"DarwinBox run failed: {e}")


@router.get("/run/oracle_orc", response_model=RunResult)
def run_oracle_orc(
    company: str = Query(..., description="Company name"),
    careers_url: str = Query(..., description="https://<host>/hcmUI/CandidateExperience/en/sites/<SITE>/jobs"),
    host: Optional[str] = Query(None, description="e.g. don.fa.em2.oraclecloud.com (optional)"),
    site: Optional[str] = Query(None, description="e.g. CX_1003 (optional)"),
    limit: int = Query(50, ge=1, le=100),
    pages: int = Query(3, ge=1, le=20),
    db: Session = Depends(get_db),
):
    started = datetime.datetime.utcnow()
    run_log = RunLog(ats_type="oracle_orc", company_name=company, started_at=started, status="running", endpoint=None)
    db.add(run_log)
    db.commit()
    db.refresh(run_log)

    try:
        if not host or not site:
            from urllib.parse import urlparse
            parts = urlparse(careers_url)
            host = host or parts.netloc
            try:
                frag = parts.path.split("/sites/")[-1]
                site = site or frag.split("/")[0]
            except Exception:
                run_log.ended_at = datetime.datetime.utcnow()
                run_log.status = "error"
                run_log.error = "Invalid careers_url; missing /sites/<SITE>/jobs"
                db.commit()
                raise HTTPException(
                    status_code=400,
                    detail="Provide valid host & site or a standard careers_url like /sites/<SITE>/jobs.",
                )

        rest_base = build_rest_base(host, site)
        res = fetch_oracle_orc_jobs(
            careers_url=careers_url,
            rest_base=rest_base,
            site_number=site,
            limit=limit,
            max_pages=pages,
        )
        raw = res.get("jobs", [])
        if not isinstance(raw, list):
            run_log.ended_at = datetime.datetime.utcnow()
            run_log.status = "error"
            run_log.error = "ORC: unexpected response format (no 'jobs' list')"
            db.commit()
            raise HTTPException(status_code=502, detail="ORC: unexpected response format (no 'jobs' list').")

        normalized = [map_oracle_orc_job(j, company, careers_url, site) for j in raw]
        counts = _upsert_jobs(db, normalized)

        run_log.endpoint = res.get("last_endpoint")
        run_log.fetched = len(raw)
        run_log.new = counts.get("new", 0)
        run_log.updated = counts.get("updated", 0)
        run_log.closed = counts.get("closed", 0)
        run_log.ended_at = datetime.datetime.utcnow()
        run_log.status = "success"
        db.commit()

        return RunResult(
            adapter="oracle_orc",
            company=company,
            endpoint=res.get("last_endpoint"),
            fetched=len(raw),
            new=counts["new"],
            updated=counts["updated"],
            closed=counts.get("closed", 0),
        )
    except HTTPException:
        raise
    except Exception as e:
        run_log.ended_at = datetime.datetime.utcnow()
        run_log.status = "error"
        run_log.error = str(e)
        db.commit()
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"ORC run failed: {e}")


@router.get("/__debug/db_url")
def debug_db_url():
    url = str(engine.url)
    if engine.url.password:
        url = url.replace(engine.url.password, "***")
    return {"url": url}
