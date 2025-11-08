# main.py
import os, csv, json
from utils.delta import detect_changes

# JOIN.com
from adapters.join_com import fetch_join_dom_jobs, map_join_dom_job

# KekaHR
from adapters.kekahr import fetch_kekahr_jobs, map_kekahr_job, _base_url_from_careers as keka_base

# DarwinBox (NEW)
from adapters.darwinbox import (
    fetch_darwinbox_jobs,
    map_darwinbox_job,
    base_url_from_careers as darwin_base,
)

# Oracle ORC (NEW)
from adapters.oracle_orc import (
    fetch_oracle_orc_jobs,
    map_oracle_orc_job,
)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

CSV_PATH = os.path.join(DATA_DIR, "company_data.csv")  # optional CSV input

def _load_prev(path):
    return json.load(open(path, encoding="utf-8")) if os.path.exists(path) else []

def _save_json(path, data):
    json.dump(data, open(path, "w", encoding="utf-8"), indent=2, ensure_ascii=False)

# -------------------- JOIN.COM --------------------
def run_join(company_name, careers_url):
    print(f"\n[JOIN] Fetching jobs for {company_name} ...")
    raw_jobs = fetch_join_dom_jobs(careers_url)
    print(f"Fetched {len(raw_jobs)} jobs.")
    normalized = [map_join_dom_job(j, company_name, careers_url) for j in raw_jobs]

    prev_path = os.path.join(DATA_DIR, "state_join.json")
    prev = _load_prev(prev_path)
    new, updated, closed = detect_changes(prev, normalized)

    _save_json(prev_path, normalized)
    _save_json(os.path.join(DATA_DIR, "output_join.json"), normalized)

    print(f"New: {len(new)} | Updated: {len(updated)} | Closed: {len(closed)}")
    return {"adapter": "join", "company": company_name, "fetched": len(raw_jobs),
            "new": len(new), "updated": len(updated), "closed": len(closed)}

# -------------------- KEKAHR --------------------
def run_kekahr(company_name, careers_url, endpoint_override=None):
    print(f"\n[KEKAHR] Fetching jobs for {company_name} ...")
    res = fetch_kekahr_jobs(careers_url, endpoint_override=endpoint_override)
    base = keka_base(careers_url)
    raw_jobs = res["jobs"]
    print(f"Fetched {len(raw_jobs)} jobs. Endpoint: {res['endpoint'] or '-'}")

    prev_path = os.path.join(DATA_DIR, "state_kekahr.json")
    prev = _load_prev(prev_path)

    if len(raw_jobs) == 0:
        print("WARN: Got 0 jobs — keeping previous state, not overwriting files.")
        _save_json(os.path.join(DATA_DIR, "debug_kekahr_raw.json"), res)
        print(f"New: 0 | Updated: 0 | Closed: 0")
        return {"adapter": "kekahr", "company": company_name, "fetched": 0,
                "new": 0, "updated": 0, "closed": 0}

    normalized = [map_kekahr_job(j, company_name, careers_url, base) for j in raw_jobs]

    new, updated, closed = detect_changes(prev, normalized)

    _save_json(prev_path, normalized)
    _save_json(os.path.join(DATA_DIR, "output_kekahr.json"), normalized)

    print(f"New: {len(new)} | Updated: {len(updated)} | Closed: {len(closed)}")
    return {"adapter": "kekahr", "company": company_name, "fetched": len(raw_jobs),
            "new": len(new), "updated": len(updated), "closed": len(closed)}

# -------------------- DARWINBOX (NEW) --------------------
def run_darwinbox(company_name, careers_url, page_size: int = 50):
    print(f"\n[DARWINBOX] Fetching jobs for {company_name} ...")
    res = fetch_darwinbox_jobs(careers_url, page_size=page_size)
    base = darwin_base(careers_url)
    raw_jobs = res["jobs"]
    print(f"Fetched {len(raw_jobs)} jobs. Endpoint: {res.get('endpoint') or '-'}")

    prev_path = os.path.join(DATA_DIR, "state_darwinbox.json")
    prev = _load_prev(prev_path)

    if len(raw_jobs) == 0:
        print("WARN: Got 0 jobs — keeping previous state, not overwriting files.")
        _save_json(os.path.join(DATA_DIR, "debug_darwinbox_raw.json"), res)
        print(f"New: 0 | Updated: 0 | Closed: 0")
        return {"adapter": "darwinbox", "company": company_name, "fetched": 0,
                "new": 0, "updated": 0, "closed": 0}

    normalized = [map_darwinbox_job(j, company_name, careers_url, base) for j in raw_jobs]

    new, updated, closed = detect_changes(prev, normalized)

    _save_json(prev_path, normalized)
    _save_json(os.path.join(DATA_DIR, "output_darwinbox.json"), normalized)

    print(f"New: {len(new)} | Updated: {len(updated)} | Closed: {len(closed)}")
    return {"adapter": "darwinbox", "company": company_name, "fetched": len(raw_jobs),
            "new": len(new), "updated": len(updated), "closed": len(closed)}
# -------------------- ORACLE ORC (NEW) --------------------
def run_oracle_orc(company_name, careers_url, rest_base, site_number, limit=50, max_pages=5):
    """
    careers_url: public CX site, e.g.:
      https://don.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1003/jobs
    rest_base: REST API base, e.g.:
      https://don.fa.em2.oraclecloud.com
    site_number: site code from the CX URL, e.g. "CX_1003"
    """
    print(f"\n[ORACLE-ORC] Fetching jobs for {company_name} ...")
    res = fetch_oracle_orc_jobs(
        careers_url=careers_url,
        rest_base=rest_base,
        site_number=site_number,
        limit=limit,
        max_pages=max_pages,
    )
    raw_jobs = res["jobs"]
    endpoint_display = res.get("last_endpoint") or "-"
    print(f"Fetched {len(raw_jobs)} jobs. Endpoint: {endpoint_display}")

    prev_path = os.path.join(DATA_DIR, "state_oracle_orc.json")
    prev = _load_prev(prev_path)

    if len(raw_jobs) == 0:
        print("WARN: Got 0 jobs — keeping previous state, not overwriting files.")
        _save_json(os.path.join(DATA_DIR, "debug_oracle_orc_raw.json"), res)
        print(f"New: 0 | Updated: 0 | Closed: 0")
        return {"adapter": "oracle_orc", "company": company_name, "fetched": 0,
                "new": 0, "updated": 0, "closed": 0}

    normalized = [map_oracle_orc_job(j, company_name, careers_url, site_number) for j in raw_jobs]
    new, updated, closed = detect_changes(prev, normalized)

    _save_json(prev_path, normalized)
    _save_json(os.path.join(DATA_DIR, "output_oracle_orc.json"), normalized)

    print(f"New: {len(new)} | Updated: {len(updated)} | Closed: {len(closed)}")
    return {"adapter": "oracle_orc", "company": company_name, "fetched": len(raw_jobs),
            "new": len(new), "updated": len(updated), "closed": len(closed)}

# -------------------- MAIN ENTRY --------------------
if __name__ == "__main__":
    # Quick smoke tests
    run_join("Qdrant", "https://join.com/companies/qdrant")

    # KekaHR — use the exact JSON endpoint you captured in DevTools.
    run_kekahr(
        "10Decoders",
        "https://10decoders.keka.com/careers/",
        endpoint_override="https://10decoders.keka.com/careers/api/embedjobs/default/active/8a08095f-29fa-4856-ac19-f693dcf00ad1"
    )

    # DarwinBox — ADA example (auto-pages /ms/candidateapi/job?page=1&limit=50)
    run_darwinbox("ADA", "https://adaglobal.darwinbox.com/ms/candidate/careers", page_size=50)

    # Oracle ORC — Euroclear (example)
    run_oracle_orc(
        "Euroclear (example)",
        "https://don.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1003/jobs",
        rest_base="https://don.fa.em2.oraclecloud.com",
        site_number="CX_1003",
        limit=50,
        max_pages=5,
    )

    # --- CSV-driven mode (optional) ---
    # Columns: company_name, careers_url, ats_type, [api_endpoint]
    # if os.path.exists(CSV_PATH):
    #     with open(CSV_PATH, newline="", encoding="utf-8") as f:
    #         rows = list(csv.DictReader(f))
    #     for r in rows:
    #         name = (r.get("company_name") or r.get("company") or "").strip() or "Unknown"
    #         url  = (r.get("careers_url")  or r.get("job source urls") or r.get("url") or "").strip()
    #         ats  = (r.get("ats_type")     or r.get("ats") or "").strip().lower()
    #         api  = (r.get("api_endpoint") or "").strip()
    #         if not url:
    #             continue
    #         if "keka" in url or ats == "kekahr":
    #             run_kekahr(name, url, endpoint_override=api or None)
    #         elif "join.com" in url or ats == "join":
    #             run_join(name, url)
    #         elif "darwinbox.com" in url or ats == "darwinbox":
    #             run_darwinbox(name, url, page_size=50)
