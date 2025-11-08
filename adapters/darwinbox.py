# adapters/darwinbox.py
# Fetch DarwinBox jobs with requests → Playwright fallback, + pagination.
import json, time, math, hashlib, datetime
from typing import Dict, Any, List, Optional
import requests
from urllib.parse import urlparse, urljoin
from playwright.sync_api import sync_playwright

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "X-Requested-With": "XMLHttpRequest",
}

def base_url_from_careers(careers_url: str) -> str:
    p = urlparse(careers_url)
    return f"{p.scheme}://{p.hostname}"

def _endpoint_pattern(base: str, page: int, limit: int) -> str:
    # Standard public endpoint pattern (your example)
    return f"{base}/ms/candidateapi/job?page={page}&limit={limit}"

def _iso_from_epoch(sec: Optional[int]) -> Optional[str]:
    try:
        if sec is None:
            return None
        # Darwinbox returns epoch seconds (not ms) in job_posting_on
        return datetime.datetime.utcfromtimestamp(int(sec)).isoformat() + "Z"
    except Exception:
        return None

def _stable_external_id(raw: Dict[str, Any]) -> str:
    # Prefer 'id'; else hash title+created_on
    if raw.get("id"):
        return str(raw["id"])
    basis = (raw.get("title") or "") + "|" + (raw.get("created_on") or "")
    return hashlib.md5(basis.encode("utf-8")).hexdigest()

def map_darwinbox_job(raw: Dict[str, Any], company_name: str, careers_url: str, base_url: str) -> Dict[str, Any]:
    jid = str(raw.get("id") or "")
    # Darwinbox is SPA-ish; detail pages often live under /ms/candidate/careers with hash routes.
    # This URL works as a stable "apply/detail" link for most tenants:
    apply_url = urljoin(base_url, f"/ms/candidate/careers#/job/{jid}") if jid else careers_url

    # Prefer created_on (ISO) else convert job_posting_on (epoch seconds) to ISO
    posted_at = raw.get("created_on") or _iso_from_epoch(raw.get("job_posting_on"))

    location = raw.get("officelocation_show_arr") or raw.get("officelocation_arr") or None
    department = raw.get("department") or None
    emp_type = raw.get("emp_type") or None
    title = raw.get("title") or raw.get("designation_display_name") or None

    return {
        "external_id": _stable_external_id(raw),
        "ats_type": "darwinbox",
        "company_name": company_name,
        "title": title,
        "department": department,
        "location_text": location,
        "remote_type": None,  # Darwinbox payload doesn’t explicitly flag remote; derive later if you want
        "employment_type": emp_type,
        "posted_at": posted_at,
        "updated_at_source": None,
        "apply_url": apply_url,
        "source_url": careers_url,
        "description_html": None,  # Darwinbox listings API doesn’t return full HTML; details need a second call if desired
        "raw_payload": raw,
    }

def _fetch_page_requests(careers_url: str, page: int, limit: int) -> Dict[str, Any]:
    base = base_url_from_careers(careers_url)
    url = _endpoint_pattern(base, page, limit)
    headers = dict(DEFAULT_HEADERS)
    headers["Referer"] = careers_url
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code == 200:
            return {"ok": True, "endpoint": url, "data": r.json()}
        else:
            print(f"[WARN] DarwinBox non-200 ({r.status_code}) at page {page}")
            return {"ok": False, "endpoint": url, "data": None, "status": r.status_code}
    except Exception as e:
        print(f"[WARN] DarwinBox requests error at page {page}: {e}")
        return {"ok": False, "endpoint": url, "data": None, "status": None}

def _fetch_page_playwright(careers_url: str, page_idx: int, limit: int) -> Dict[str, Any]:
    base = base_url_from_careers(careers_url)
    url = _endpoint_pattern(base, page_idx, limit)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            viewport={"width": 1366, "height": 768},
        )
        page = context.new_page()
        page.set_default_timeout(20000)

        # Load the careers app so the fetch will include cookies/headers
        page.goto(careers_url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=20000)

        try:
            payload = page.evaluate(
                """async (url) => {
                    const r = await fetch(url, {
                        headers: {
                            "X-Requested-With":"XMLHttpRequest",
                            "Accept":"application/json"
                        },
                        credentials: "include"
                    });
                    if (!r.ok) return { ok:false, status:r.status, data:null };
                    const data = await r.json();
                    return { ok:true, status:r.status, data };
                }""",
                url
            )
        except Exception as e:
            browser.close()
            return {"ok": False, "endpoint": url, "data": None, "status": None}

        browser.close()
        if payload and payload.get("ok"):
            return {"ok": True, "endpoint": url, "data": payload.get("data")}
        return {"ok": False, "endpoint": url, "data": None, "status": payload and payload.get("status")}

def fetch_darwinbox_jobs(careers_url: str, page_size: int = 50, max_pages: int = 40) -> Dict[str, Any]:
    """
    Returns:
      {
        "endpoint": "<pattern with {page}&{limit}> or first successful URL",
        "jobs": [ ... ],
        "note": "requests|playwright|mixed"
      }
    """
    base = base_url_from_careers(careers_url)
    endpoint_pattern = _endpoint_pattern(base, page="{page}", limit="{limit}")

    jobs: List[Dict[str, Any]] = []
    note = None
    used_method = None
    first_success_endpoint = None

    # First attempt: plain requests with good headers.
    for page_idx in range(1, max_pages + 1):
        res = _fetch_page_requests(careers_url, page_idx, page_size)
        if not res["ok"]:
            # If the very first page gets blocked (403), switch to Playwright path.
            if page_idx == 1:
                used_method = "playwright"
            break

        data = res["data"] or {}
        msg = data.get("message") if isinstance(data, dict) else {}
        batch = msg.get("jobs") or []
        if first_success_endpoint is None:
            first_success_endpoint = res["endpoint"]
        used_method = "requests"
        if not batch:
            break
        jobs.extend(batch)

        # Stop early if we already have all rows (jobscount present)
        total = msg.get("jobscount")
        if isinstance(total, int) and len(jobs) >= total:
            break

    # Fallback: Playwright in-page fetch if we got nothing or were blocked early
    if used_method != "requests" or len(jobs) == 0:
        jobs.clear()
        for page_idx in range(1, max_pages + 1):
            res = _fetch_page_playwright(careers_url, page_idx, page_size)
            if not res["ok"]:
                break
            data = res["data"] or {}
            msg = data.get("message") if isinstance(data, dict) else {}
            batch = msg.get("jobs") or []
            if first_success_endpoint is None:
                first_success_endpoint = res["endpoint"]
            if not batch:
                break
            jobs.extend(batch)

            total = msg.get("jobscount")
            if isinstance(total, int) and len(jobs) >= total:
                break
        used_method = "playwright" if len(jobs) > 0 else (used_method or "requests")

    note = used_method or "requests"
    return {
        "endpoint": first_success_endpoint or endpoint_pattern,
        "jobs": jobs,
        "note": note,
    }

__all__ = [
    "fetch_darwinbox_jobs",
    "map_darwinbox_job",
    "base_url_from_careers",
]
