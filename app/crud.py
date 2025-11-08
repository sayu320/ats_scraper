# app/crud.py
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from app.models import Job

# minimal set of normalized fields we store
NORM_FIELDS = [
    "external_id","ats_type","company_name","title","department","location_text",
    "remote_type","employment_type","posted_at","updated_at_source","apply_url",
    "source_url","description_html"
]

def get_jobs(db: Session, limit: int = 100, offset: int = 0):
    return (
        db.query(Job)
        .order_by(Job.id.desc())
        .limit(limit)
        .offset(offset)
        .all()
    )

def get_job(db: Session, job_id: int):
    return db.get(Job, job_id)

def upsert_jobs(db: Session, normalized: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Bulk upsert keyed by (ats_type, external_id).
    Returns counts: {"new": X, "updated": Y}
    """
    new_count = updated_count = 0
    if not normalized:
        return {"new": 0, "updated": 0}

    # Preload existing by external_id (and we also check ats_type in the map)
    ext_ids = [j.get("external_id") for j in normalized if j.get("external_id")]
    existing = db.query(Job).filter(Job.external_id.in_(ext_ids)).all()
    existing_map = {f"{row.ats_type}|{row.external_id}": row for row in existing}

    for j in normalized:
        key = f"{j.get('ats_type')}|{j.get('external_id')}"
        row = existing_map.get(key)
        if row is None:
            row = Job(**{f: j.get(f) for f in NORM_FIELDS})
            db.add(row)
            new_count += 1
        else:
            changed = False
            for f in NORM_FIELDS:
                new_val = j.get(f)
                if getattr(row, f) != new_val:
                    setattr(row, f, new_val)
                    changed = True
            if changed:
                updated_count += 1

    db.commit()
    return {"new": new_count, "updated": updated_count}
