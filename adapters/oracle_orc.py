# adapters/oracle_orc.py
# Oracle Recruiting Cloud (ORC) adapter
# Exports:
#   - fetch_oracle_orc_jobs(careers_url, rest_base, site_number, limit=50, max_pages=10)
#   - map_oracle_orc_job(raw, company_name, source_url, site_number)
#   - base_url_from_careers(careers_url)

# from __future__ import annotations

# import time
# from typing import Dict, Any, List, Tuple
# from urllib.parse import urlparse

# import requests

# DEFAULT_HEADERS = {
#     "User-Agent": (
#         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) "
#         "Chrome/120.0 Safari/537.36"
#     ),
#     "Accept": "application/json, text/plain, */*",
# }

# def base_url_from_careers(careers_url: str) -> str:
#     """
#     Extract the origin (scheme://host) from a CX careers URL.
#     e.g. https://don.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1003/jobs
#       -> https://don.fa.em2.oraclecloud.com
#     """
#     p = urlparse(careers_url)
#     return f"{p.scheme}://{p.hostname}"

# def _endpoint_template(rest_base: str, site_number: str, limit: int) -> str:
#     """
#     Build the ORC REST endpoint template with an {offset} placeholder.
#     """
#     # expand includes nested pieces we often need; finder=findReqs drives the search
#     return (
#         f"{rest_base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions?"
#         f"onlyData=true&expand="
#         f"requisitionList.workLocation,"
#         f"requisitionList.otherWorkLocations,"
#         f"requisitionList.secondaryLocations,"
#         f"flexFieldsFacet.values,"
#         f"requisitionList.requisitionFlexFields&"
#         f"finder=findReqs;siteNumber={site_number},"
#         f"facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3B"
#         f"CATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,"
#         f"limit={limit},sortBy=POSTING_DATES_DESC,offset={{offset}}"
#     )

# def _get_json(url: str, headers: Dict[str, str], retries: int = 2, backoff: float = 0.8) -> Tuple[int, Any]:
#     """
#     GET JSON with tiny retry/backoff for 429/5xx.
#     Returns (status_code, json_or_None)
#     """
#     attempt = 0
#     while True:
#         resp = requests.get(url, headers=headers, timeout=30)
#         sc = resp.status_code
#         if sc == 200:
#             try:
#                 return sc, resp.json()
#             except Exception:
#                 return sc, None
#         if sc in (429, 500, 502, 503, 504) and attempt < retries:
#             time.sleep(backoff * (attempt + 1))
#             attempt += 1
#             continue
#         return sc, None

# def fetch_oracle_orc_jobs(
#     careers_url: str,
#     rest_base: str,
#     site_number: str,
#     limit: int = 50,
#     max_pages: int = 10,
# ) -> Dict[str, Any]:
#     """
#     Fetch job listings from Oracle Recruiting Cloud (ORC) public Candidate Experience REST API.

#     Parameters
#     ----------
#     careers_url : str
#         e.g. "https://don.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1003/jobs"
#     rest_base : str
#         e.g. "https://don.fa.em2.oraclecloud.com"
#     site_number : str
#         e.g. "CX_1003"
#     limit : int
#         Results per page (typical 25/50)
#     max_pages : int
#         Hard page cap to avoid runaway loops

#     Returns
#     -------
#     dict : { "jobs": [raw_job, ...], "last_endpoint": <str> }
#     """
#     jobs: List[Dict[str, Any]] = []
#     last_endpoint = None
#     template = _endpoint_template(rest_base, site_number, limit)

#     for page in range(max_pages):
#         offset = page * limit
#         endpoint = template.format(offset=offset)
#         last_endpoint = endpoint

#         status, data = _get_json(endpoint, DEFAULT_HEADERS)
#         if status != 200:
#             print(f"[WARN] Oracle ORC non-200 ({status}) at page {page + 1}")
#             break
#         if not isinstance(data, dict):
#             break

#         items = data.get("items", [])
#         if not items:
#             # No more result blocks
#             break

#         # Each entry in "items" typically contains a "requisitionList" array (the actual jobs)
#         got_any = False
#         for entry in items:
#             reqs = entry.get("requisitionList") or []
#             if reqs:
#                 jobs.extend(reqs)
#                 got_any = True

#         # If the server returned no requisitions in this block, we stop
#         if not got_any:
#             break

#         # If we got fewer than a full page of items (rare here), we can break early
#         # Note: ORC sometimes keeps "items" length constant and packs actual jobs in requisitionList.
#         # We rely on "got_any" above to decide continuation.
#         if len(items) < 1:  # defensive; usually not hit
#             break

#     return {"jobs": jobs, "last_endpoint": last_endpoint}

# def _join_locations(primary: str | None, secondary: List[Dict[str, Any]] | None) -> str | None:
#     """
#     Compose a human-friendly location string from primary & secondary location blocks.
#     """
#     pieces: List[str] = []
#     if primary:
#         pieces.append(primary)
#     if secondary:
#         for loc in secondary:
#             name = (loc or {}).get("Name")
#             if name:
#                 pieces.append(name)
#     # De-duplicate while preserving order
#     seen = set()
#     out: List[str] = []
#     for p in pieces:
#         if p not in seen:
#             out.append(p)
#             seen.add(p)
#     return " | ".join(out) if out else None

# def map_oracle_orc_job(raw: Dict[str, Any], company_name: str, source_url: str, site_number: str) -> Dict[str, Any]:
#     """
#     Normalize a single ORC job object into the common output schema used by main.py.
#     """
#     job_id = str(raw.get("Id") or raw.get("id") or "").strip() or None
#     title = raw.get("Title") or raw.get("title")
#     department = raw.get("Department") or raw.get("Organization") or raw.get("department")
#     primary_loc = raw.get("PrimaryLocation") or raw.get("primaryLocation")
#     secondary_locs = raw.get("secondaryLocations") or []
#     location_text = _join_locations(primary_loc, secondary_locs)

#     posted_at = raw.get("PostedDate") or raw.get("postingStartDate") or raw.get("postedDate")
#     employment_type = (
#         raw.get("JobType")
#         or raw.get("WorkerType")
#         or raw.get("ContractType")
#         or raw.get("JobSchedule")
#         or raw.get("WorkplaceType")
#         or ""
#     )
#     description_html = raw.get("ShortDescriptionStr") or raw.get("ExternalResponsibilitiesStr") or None

#     # Apply URL in CX:
#     #   https://<host>/hcmUI/CandidateExperience/en/sites/<SITE>/job/<Id>
#     apply_url = source_url
#     if job_id:
#         base = source_url.split("/jobs", 1)[0]
#         apply_url = f"{base}/job/{job_id}"

#     return {
#         "external_id": job_id,
#         "ats_type": "oracle_orc",
#         "company_name": company_name,
#         "title": title,
#         "department": department,
#         "location_text": location_text,
#         "remote_type": None,
#         "employment_type": employment_type,
#         "posted_at": posted_at,
#         "updated_at_source": None,
#         "apply_url": apply_url,
#         "source_url": source_url,
#         "description_html": description_html,
#         "raw_payload": raw,
#     }

# __all__ = [
#     "fetch_oracle_orc_jobs",
#     "map_oracle_orc_job",
#     "base_url_from_careers",
# ]
from __future__ import annotations

import time
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import urlparse

import requests

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

# ------------------------ small URL helpers ------------------------

def base_url_from_careers(careers_url: str) -> str:
    """
    Extract the origin (scheme://host) from a CX careers URL.
    e.g. https://don.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1003/jobs
      -> https://don.fa.em2.oraclecloud.com
    """
    p = urlparse(careers_url)
    return f"{p.scheme}://{p.hostname}"

def extract_site_number(careers_url: str) -> Optional[str]:
    """
    From a CX careers URL, pick out the siteNumber segment, e.g. 'CX_1003'.
    Returns None if not found.
    """
    try:
        # .../en/sites/<SITE>/jobs
        parts = urlparse(careers_url).path.split("/")
        idx = parts.index("sites")
        return parts[idx + 1] if idx >= 0 and idx + 1 < len(parts) else None
    except Exception:
        return None

def build_rest_base(host_or_url: str, site_number: str) -> str:
    """
    Normalize the host for ORC REST calls.
    NOTE: We intentionally return ONLY the origin (scheme://host). The finder + query
    string is composed by _endpoint_template(), which appends /hcmRestApi/... etc.

    Accepts either:
      - 'don.fa.em2.oraclecloud.com'
      - 'https://don.fa.em2.oraclecloud.com/anything...'
    """
    host_or_url = (host_or_url or "").strip().rstrip("/")
    if not host_or_url:
        raise ValueError("host_or_url is required")
    if not site_number:
        raise ValueError("site_number is required")

    if host_or_url.startswith("http"):
        parsed = urlparse(host_or_url)
        origin = f"{parsed.scheme}://{parsed.hostname}"
    else:
        origin = f"https://{host_or_url}"
    return origin  # the rest of the path is added by _endpoint_template()

# ------------------------ REST paging/template ------------------------

def _endpoint_template(rest_base: str, site_number: str, limit: int) -> str:
    """
    Build the ORC REST endpoint template with an {offset} placeholder.
    """
    # expand includes nested pieces we often need; finder=findReqs drives the search
    return (
        f"{rest_base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions?"
        f"onlyData=true&expand="
        f"requisitionList.workLocation,"
        f"requisitionList.otherWorkLocations,"
        f"requisitionList.secondaryLocations,"
        f"flexFieldsFacet.values,"
        f"requisitionList.requisitionFlexFields&"
        f"finder=findReqs;siteNumber={site_number},"
        f"facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3B"
        f"CATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,"
        f"limit={limit},sortBy=POSTING_DATES_DESC,offset={{offset}}"
    )

def _get_json(url: str, headers: Dict[str, str], retries: int = 2, backoff: float = 0.8) -> Tuple[int, Any]:
    """
    GET JSON with tiny retry/backoff for 429/5xx.
    Returns (status_code, json_or_None)
    """
    attempt = 0
    while True:
        resp = requests.get(url, headers=headers, timeout=30)
        sc = resp.status_code
        if sc == 200:
            try:
                return sc, resp.json()
            except Exception:
                return sc, None
        if sc in (429, 500, 502, 503, 504) and attempt < retries:
            time.sleep(backoff * (attempt + 1))
            attempt += 1
            continue
        return sc, None

# ------------------------ public fetch ------------------------

def fetch_oracle_orc_jobs(
    careers_url: str,
    rest_base: str,
    site_number: str,
    limit: int = 50,
    max_pages: int = 10,
) -> Dict[str, Any]:
    """
    Fetch job listings from Oracle Recruiting Cloud (ORC) public Candidate Experience REST API.

    Parameters
    ----------
    careers_url : str
        e.g. "https://don.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1003/jobs"
    rest_base : str
        e.g. "https://don.fa.em2.oraclecloud.com" (use build_rest_base to derive)
    site_number : str
        e.g. "CX_1003"
    limit : int
        Results per page (typical 25/50)
    max_pages : int
        Hard page cap to avoid runaway loops

    Returns
    -------
    dict : { "jobs": [raw_job, ...], "last_endpoint": <str> }
    """
    jobs: List[Dict[str, Any]] = []
    last_endpoint = None
    template = _endpoint_template(rest_base, site_number, limit)

    for page in range(max_pages):
        offset = page * limit
        endpoint = template.format(offset=offset)
        last_endpoint = endpoint

        status, data = _get_json(endpoint, DEFAULT_HEADERS)
        if status != 200:
            print(f"[WARN] Oracle ORC non-200 ({status}) at page {page + 1}")
            break
        if not isinstance(data, dict):
            break

        items = data.get("items", [])
        if not items:
            # No more result blocks
            break

        # Each entry in "items" typically contains a "requisitionList" array (the actual jobs)
        got_any = False
        for entry in items:
            reqs = entry.get("requisitionList") or []
            if reqs:
                jobs.extend(reqs)
                got_any = True

        if not got_any:
            break

    return {"jobs": jobs, "last_endpoint": last_endpoint}

# ------------------------ mapping / normalization ------------------------

def _join_locations(primary: str | None, secondary: List[Dict[str, Any]] | None) -> str | None:
    """
    Compose a human-friendly location string from primary & secondary location blocks.
    """
    pieces: List[str] = []
    if primary:
        pieces.append(primary)
    if secondary:
        for loc in secondary:
            name = (loc or {}).get("Name")
            if name:
                pieces.append(name)
    # De-duplicate while preserving order
    seen = set()
    out: List[str] = []
    for p in pieces:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return " | ".join(out) if out else None

def map_oracle_orc_job(raw: Dict[str, Any], company_name: str, source_url: str, site_number: str) -> Dict[str, Any]:
    """
    Normalize a single ORC job object into the common output schema used by main.py.
    """
    job_id = str(raw.get("Id") or raw.get("id") or "").strip() or None
    title = raw.get("Title") or raw.get("title")
    department = raw.get("Department") or raw.get("Organization") or raw.get("department")
    primary_loc = raw.get("PrimaryLocation") or raw.get("primaryLocation")
    secondary_locs = raw.get("secondaryLocations") or []
    location_text = _join_locations(primary_loc, secondary_locs)

    posted_at = raw.get("PostedDate") or raw.get("postingStartDate") or raw.get("postedDate")
    employment_type = (
        raw.get("JobType")
        or raw.get("WorkerType")
        or raw.get("ContractType")
        or raw.get("JobSchedule")
        or raw.get("WorkplaceType")
        or ""
    )
    description_html = raw.get("ShortDescriptionStr") or raw.get("ExternalResponsibilitiesStr") or None

    # Apply URL in CX:
    #   https://<host>/hcmUI/CandidateExperience/en/sites/<SITE>/job/<Id>
    apply_url = source_url
    if job_id:
        base = source_url.split("/jobs", 1)[0]
        apply_url = f"{base}/job/{job_id}"

    return {
        "external_id": job_id,
        "ats_type": "oracle_orc",
        "company_name": company_name,
        "title": title,
        "department": department,
        "location_text": location_text,
        "remote_type": None,
        "employment_type": employment_type,
        "posted_at": posted_at,
        "updated_at_source": None,
        "apply_url": apply_url,
        "source_url": source_url,
        "description_html": description_html,
        "raw_payload": raw,
    }

__all__ = [
    "fetch_oracle_orc_jobs",
    "map_oracle_orc_job",
    "base_url_from_careers",
    "build_rest_base",
    "extract_site_number",
]
