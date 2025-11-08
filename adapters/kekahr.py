# adapters/kekahr.py
# Robust KekaHR adapter: API → in-page fetch → XHR sniff → auto-discover embedjobs → DOM + per-job enrichment
# Exports: fetch_kekahr_jobs, map_kekahr_job, _base_url_from_careers

import json
import re
import time
import hashlib
import unicodedata
import requests
from urllib.parse import urlparse, urljoin
# from playwright.sync_api import sync_playwright

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

# ---------------------- URL & payload helpers ----------------------

def _base_url_from_careers(careers_url: str) -> str:
    p = urlparse(careers_url)
    return f"{p.scheme}://{p.hostname}"

def _extract_items(payload):
    """Accepts Keka responses with keys like jobs/data/openings/items/results, or a raw list."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("jobs", "data", "openings", "items", "results"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return None

# ---------------------- tiny text helpers (field quality) ----------------------

def _clean_first_lines(text: str, max_lines: int = 3):
    """Return only a few top lines, stopping before big sections like 'Key Responsibilities'."""
    if not text:
        return None
    lines = [l.strip() for l in str(text).splitlines() if l and l.strip()]
    if not lines:
        return None
    stop_tokens = (
        "job title", "key responsibilities", "about us", "skills required",
        "what you will do", "roles & responsibilities", "roles and responsibilities"
    )
    out = []
    for l in lines:
        if any(tok in l.lower() for tok in stop_tokens):
            break
        out.append(l)
        if len(out) >= max_lines:
            break
    return " | ".join(out) if out else None

_EMPLOYMENT_HINTS = {
    "full-time": "Full-time",
    "full time": "Full-time",
    "part-time": "Part-time",
    "part time": "Part-time",
    "contract": "Contract",
    "internship": "Internship",
    "intern": "Internship",
    "temporary": "Temporary",
    "freelance": "Freelance",
}

def _guess_employment_from_text(text: str):
    if not text:
        return None
    t = text.lower()
    for k, v in _EMPLOYMENT_HINTS.items():
        if k in t:
            return v
    return None

def _parse_posted_from_text(text: str):
    """Return a 'posted/published' line if present."""
    if not text:
        return None
    for line in str(text).splitlines():
        l = line.strip().lower()
        if l.startswith(("posted", "published", "date posted")):
            return line.strip()
    return None

_EXPERIENCE_PAT = re.compile(r"\b(\d+\s*[-–]\s*\d+|\d+\+?)\s*(yrs|years|yr|year)\b", re.I)

def _norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "").strip()

def _split_meta_tokens(text: str):
    """Split the meta/chips line into tokens by common separators."""
    if not text:
        return []
    t = _norm(text)
    parts = re.split(r"[|\u00B7/,\n]+|\s{2,}", t)  # pipes, middots, slashes, commas, newlines, double-spaces
    return [p.strip() for p in parts if p and p.strip()]

def _is_experience(token: str) -> bool:
    t = token.lower().strip()
    return bool(_EXPERIENCE_PAT.search(t)) or "experience" in t

def _is_employment_token(token: str) -> bool:
    t = token.lower()
    return any(k in t for k in ["full-time", "full time", "part-time", "part time", "contract", "intern", "internship", "temporary", "freelance"])

# A small city/state hint list helps Keka tenants in India.
_CITY_HINTS = [
    "chennai","bengaluru","bangalore","hyderabad","mumbai","pune","delhi","noida","gurgaon","gurugram",
    "kochi","cochin","trivandrum","thiruvananthapuram","madurai","coimbatore","ahmedabad","kolkata"
]

def _looks_like_location(token: str, title: str | None) -> bool:
    t = token.strip()
    tl = t.lower()
    if not t:
        return False
    # not the title
    if title and t.lower() == title.lower():
        return False
    # skip pure experience or employment tokens
    if _is_experience(t) or _is_employment_token(t):
        return False
    # explicit location keywords help
    if "location" in tl:
        return True
    # contains a known city/state name?
    if any(c in tl for c in _CITY_HINTS):
        return True
    # heuristic: short, capitalized word looks like a city (e.g., "Chennai")
    if len(t.split()) <= 3 and t[0].isupper():
        return True
    return False

# ---------------------- embedjobs auto-discovery helpers ----------------------

_GUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I)

def _try_embedjobs_direct(base: str, guid: str, referer: str):
    """Try the canonical embedjobs endpoint with a discovered GUID."""
    url = urljoin(base, f"/careers/api/embedjobs/default/active/{guid}")
    headers = dict(DEFAULT_HEADERS)
    headers["Referer"] = referer
    headers["X-Requested-With"] = "XMLHttpRequest"
    try:
        r = requests.get(url, headers=headers, timeout=20)
        ct = (r.headers.get("content-type") or "").lower()
        if r.ok and "json" in ct:
            payload = r.json()
            items = _extract_items(payload) or (payload if isinstance(payload, list) else [])
            if items:
                return url, items
    except Exception:
        pass
    return None, None

def _autodiscover_embedjobs(page, base: str, careers_url: str):
    """
    Heuristics to find the embedjobs API:
    - Prefer network sniff URLs containing '/careers/api/embedjobs'.
    - Else, scan HTML for GUIDs and try the canonical path for each.
    """
    # 1) Scan responses already seen
    # (We attach the response listener in fetch_kekahr_jobs; this function is called after initial load)
    # We re-fetch page content and scan for URLs too, just in case.
    try:
        # Try to pull any URLs from the DOM that look like embedjobs
        urls = page.eval_on_selector_all(
            "script, link, meta",
            "nodes => nodes.map(n => n.outerHTML)"
        ) or []
        for blob in urls:
            if "/careers/api/embedjobs" in blob:
                m = re.search(r"https?://[^\"'>\s]+/careers/api/embedjobs/[^\"'>\s]+", blob, flags=re.I)
                if m:
                    # Try direct GET
                    url = m.group(0)
                    headers = dict(DEFAULT_HEADERS)
                    headers["Referer"] = careers_url
                    headers["X-Requested-With"] = "XMLHttpRequest"
                    try:
                        r = requests.get(url, headers=headers, timeout=20)
                        ct = (r.headers.get("content-type") or "").lower()
                        if r.ok and "json" in ct:
                            payload = r.json()
                            items = _extract_items(payload) or (payload if isinstance(payload, list) else [])
                            if items:
                                return url, items
                    except Exception:
                        pass
    except Exception:
        pass

    # 2) Fallback: scan the HTML for GUIDs and try the canonical embedjobs URL
    try:
        html = page.content()
        guids = list(dict.fromkeys(_GUID_RE.findall(html)))[:5]  # de-dupe, try a few
        for g in guids:
            url, items = _try_embedjobs_direct(base, g, careers_url)
            if items:
                return url, items
    except Exception:
        pass

    return None, None

# ---------------------- main fetcher ----------------------

def fetch_kekahr_jobs(careers_url: str, endpoint_override: str | None = None):
    from playwright.sync_api import sync_playwright 
    """
    Returns: {"endpoint": <str|None>, "jobs": <list>, "note": <str>}
    - endpoint is the discovered API URL if any; otherwise None (caller can print note)
    - jobs is a list of raw items (JSON or DOM-enriched dicts)
    - note indicates the path taken: override_api | page_fetch | page_fetch_guess | sniffed | autodiscovered | dom_enriched
    """
    base = _base_url_from_careers(careers_url)

    # A) Try direct override with requests (works if public)
    if endpoint_override:
        url = endpoint_override if endpoint_override.startswith("http") else urljoin(base, endpoint_override)
        headers = dict(DEFAULT_HEADERS)
        headers["Referer"] = careers_url
        headers["X-Requested-With"] = "XMLHttpRequest"
        try:
            r = requests.get(url, headers=headers, timeout=20)
            ct = (r.headers.get("content-type") or "").lower()
            if r.ok and "json" in ct:
                payload = r.json()
                items = _extract_items(payload) or (payload if isinstance(payload, list) else [])
                if items:
                    return {"endpoint": url, "jobs": items, "note": "override_api"}
        except Exception as e:
            print(f"[WARN] override GET failed: {e}")

    # B) In-page fetch / sniff / auto-discover / DOM using Playwright (single context)
    CANDIDATE_REL = [
        "/careers/GetJobs",
        "/careers/getjobs",
        "/Careers/GetJobs",
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            viewport={"width": 1366, "height": 768},
        )
        page = context.new_page()
        page.set_default_timeout(20000)

        def human_reload():
            page.goto(careers_url, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=20000)
            for _ in range(3):
                page.mouse.wheel(0, 1400)
                time.sleep(0.25)

        # 1) Load listing
        human_reload()

        # 2) If override is given, try it inside the page (cookies/creds included)
        if endpoint_override:
            try:
                url_rel = endpoint_override if endpoint_override.startswith("http") else urljoin(base, endpoint_override)
                items = page.evaluate(
                    """async (url) => {
                        const r = await fetch(url, {
                          headers: { "X-Requested-With": "XMLHttpRequest", "Accept":"application/json" },
                          credentials: "include"
                        });
                        const ct = (r.headers.get("content-type")||"").toLowerCase();
                        if (!r.ok || !ct.includes("application/json")) return null;
                        const data = await r.json();
                        return Array.isArray(data) ? data : (data?.jobs || data?.data || data?.items || data?.results || data);
                    }""",
                    url_rel
                )
                if items and isinstance(items, list) and len(items) > 0:
                    browser.close()
                    return {"endpoint": url_rel, "jobs": items, "note": "page_fetch"}
            except Exception as e:
                print(f"[WARN] page fetch override failed: {e}")

        # 3) Try common Keka endpoints from inside the page
        for rel in CANDIDATE_REL:
            try:
                url_try = urljoin(base, rel)
                items = page.evaluate(
                    """async (url) => {
                        const r = await fetch(url, {
                          headers: { "X-Requested-With": "XMLHttpRequest", "Accept":"application/json" },
                          credentials: "include"
                        });
                        const ct = (r.headers.get("content-type")||"").toLowerCase();
                        if (!r.ok || !ct.includes("application/json")) return null;
                        const data = await r.json();
                        return Array.isArray(data) ? data : (data?.jobs || data?.data || data?.items || data?.results || data);
                    }""",
                    url_try
                )
                if items and isinstance(items, list) and len(items) > 0:
                    browser.close()
                    return {"endpoint": url_try, "jobs": items, "note": "page_fetch_guess"}
            except Exception:
                pass

        # 4) Sniff JSON responses
        sniffed = {"url": None, "items": None}

        def on_response(resp):
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                url_l = resp.url.lower()
                if "application/json" in ct and ("/careers" in url_l or "jobs" in url_l):
                    try:
                        body = resp.json()
                        items = _extract_items(body) or (body if isinstance(body, list) else [])
                        if items and isinstance(items, list) and len(items) > 0:
                            # Prefer embedjobs if we see it
                            if "/careers/api/embedjobs" in url_l or sniffed["url"] is None:
                                sniffed["url"] = resp.url
                                sniffed["items"] = items
                    except Exception:
                        pass
            except Exception:
                pass

        page.on("response", on_response)

        # Scroll a bit to trigger any deferred XHRs
        for _ in range(4):
            page.mouse.wheel(0, 1600)
            time.sleep(0.25)

        if sniffed["items"]:
            browser.close()
            return {"endpoint": sniffed["url"], "jobs": sniffed["items"], "note": "sniffed"}

        # 5) Auto-discover embedjobs by scanning the DOM for GUIDs and trying canonical path
        try:
            url_auto, items_auto = _autodiscover_embedjobs(page, base, careers_url)
            if items_auto:
                browser.close()
                return {"endpoint": url_auto, "jobs": items_auto, "note": "autodiscovered"}
        except Exception:
            pass

        # 6) DOM fallback with retries, then enrich by opening each jobdetails page
        def collect_job_links():
            try:
                return page.eval_on_selector_all(
                    "a[href*='/careers/jobdetails/'], a[href*='/careers/applyjob/']",
                    "nodes => Array.from(new Set(nodes.map(n => n.href)))"
                ) or []
            except Exception:
                return []

        links = collect_job_links()
        tries = 0
        while len(links) == 0 and tries < 2:
            tries += 1
            human_reload()
            links = collect_job_links()

        if len(links) == 0:
            # Persist artifacts for debugging
            try:
                page.screenshot(path="data/kekahr_empty.png", full_page=True)
                with open("data/kekahr_empty.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                print("DEBUG: Saved data/kekahr_empty.png and data/kekahr_empty.html")
            except Exception:
                pass

        jobs_dom = [{"applyUrl": u} for u in links]
        enriched = _enrich_keka_job_details_with_context(context, jobs_dom)

        browser.close()
        return {"endpoint": None, "jobs": enriched, "note": "dom_enriched"}

# ---------------------- DOM enrichment (same context) ----------------------

def _enrich_keka_job_details_with_context(context, dom_jobs):
    """
    Enrich DOM-only jobs using an existing Playwright context (no nested sync_playwright).
    Priority: JSON-LD JobPosting > DOM chips/labels > generic blocks + cleaners.
    """
    if not dom_jobs:
        return dom_jobs

    to_visit = dom_jobs[:80]  # safety cap

    TITLE_SELS = [
        "h1",
        "[data-testid*=title i]",
        "[class*=job-title i]",
    ]
    META_CHIPS_SELS = [
        "[class*=job-meta i]",
        "[class*=meta i]",
        "[class*=chips i]",
        ".job-summary",
        ".job-details header, header .meta"
    ]
    DESC_SELS = [
        "article",
        ".job-description",
        "[data-testid*=description i]",
        ".description",
        ".job-detail-description",
        ".keka-careers-job-description"
    ]
    LABEL_CONTAINER_SEL = "section, article, .container, .job-details, [class*=job], [class*=detail], main"

    def parse_json_ld(page):
        try:
            raw_list = page.eval_on_selector_all(
                "script[type='application/ld+json']",
                "nodes => nodes.map(n => n.textContent)"
            ) or []
            for raw in raw_list:
                try:
                    data = json.loads(raw)
                except Exception:
                    try:
                        data = json.loads(raw.strip())
                    except Exception:
                        continue
                candidates = data if isinstance(data, list) else [data]
                for obj in candidates:
                    if not isinstance(obj, dict):
                        continue
                    t = obj.get('@type')
                    if t == 'JobPosting' or (isinstance(t, list) and 'JobPosting' in t):
                        title = obj.get("title")
                        employment_type = obj.get("employmentType")
                        posted_at = obj.get("datePosted") or obj.get("validThrough")

                        loc_text = None
                        jl = obj.get("jobLocation")
                        if isinstance(jl, list) and jl:
                            first = jl[0]
                            addr = first.get("address") if isinstance(first, dict) else None
                            if isinstance(addr, dict):
                                loc_text = ", ".join([
                                    addr.get("addressLocality") or "",
                                    addr.get("addressRegion") or "",
                                    addr.get("addressCountry") or ""
                                ]).strip(", ")
                        elif isinstance(jl, dict):
                            addr = jl.get("address")
                            if isinstance(addr, dict):
                                loc_text = ", ".join([
                                    addr.get("addressLocality") or "",
                                    addr.get("addressRegion") or "",
                                    addr.get("addressCountry") or ""
                                ]).strip(", ")
                        return {
                            "title": title,
                            "employment_type": employment_type,
                            "posted_at": posted_at,
                            "location": loc_text
                        }
        except Exception:
            pass
        return None

    def extract_label_values(page):
        data = {"department": None, "location": None, "posted_at": None, "employment_type": None}
        try:
            blob = page.eval_on_selector_all(
                LABEL_CONTAINER_SEL,
                """nodes => nodes.slice(0,8).map(n => n.innerText).join("\\n")"""
            ) or ""
            t = blob.lower()

            def pick_many(labels):
                for lab in labels:
                    m = re.search(rf"{lab}\s*[:\-]\s*(.+)", t)
                    if m:
                        return m.group(1).split("\n")[0].strip()
                return None

            data["department"]       = pick_many(["department", "team"])
            data["location"]         = pick_many(["location", "work location", "job location"])
            data["posted_at"]        = pick_many(["posted on", "posted", "published", "date posted"])
            data["employment_type"]  = pick_many(["employment type", "employment", "type", "job type"])
        except Exception:
            pass
        return data

    enriched = []
    for j in to_visit:
        url = j.get("applyUrl")
        page = None
        try:
            page = context.new_page()
            page.set_default_timeout(15000)
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=15000)
            for _ in range(2):
                page.mouse.wheel(0, 1200)
                time.sleep(0.15)

            ld = parse_json_ld(page)

            # Title: prefer crisp selectors, then <title>
            title_dom = None
            try:
                for sel in TITLE_SELS:
                    if page.locator(sel).count() > 0:
                        title_dom = page.locator(sel).first.inner_text().strip()
                        if title_dom:
                            break
                if not title_dom:
                    title_dom = page.title().strip()
            except Exception:
                title_dom = None

            # meta parsing
            chips_text = ""
            try:
                for sel in META_CHIPS_SELS:
                    if page.locator(sel).count() > 0:
                        chips_text = page.locator(sel).first.inner_text().strip()
                        if chips_text:
                            break
            except Exception:
                pass

            meta = extract_label_values(page)
            tokens = _split_meta_tokens(chips_text)

            employment_guess = None
            for tok in tokens:
                if _is_employment_token(tok):
                    employment_guess = _guess_employment_from_text(tok)
                    if employment_guess:
                        break
            if not employment_guess:
                employment_guess = _guess_employment_from_text(meta.get("employment_type"))

            posted_guess = meta.get("posted_at") or _parse_posted_from_text(chips_text)

            location_candidate = None
            for tok in tokens:
                if _looks_like_location(tok, title_dom):
                    location_candidate = tok
                    break

            if not location_candidate:
                try:
                    if page.locator("[class*='location' i], .job-location, .location").count() > 0:
                        loc_dom_raw = page.locator("[class*='location' i], .job-location, .location").first.inner_text().strip()
                        cleaned = _clean_first_lines(loc_dom_raw, 2)
                        if cleaned and not (title_dom and cleaned.lower().startswith(title_dom.lower())) and not _is_experience(cleaned):
                            location_candidate = cleaned
                except Exception:
                    pass

            if not location_candidate:
                from_labels = meta.get("location")
                if from_labels and not _is_experience(from_labels):
                    location_candidate = _clean_first_lines(from_labels, 2)
            if not location_candidate:
                short = _clean_first_lines(chips_text, 2)
                if short and not (title_dom and short.lower().startswith(title_dom.lower())) and not _is_experience(short):
                    location_candidate = short

            desc_html = None
            for sel in [
                "article",".job-description","[data-testid*=description i]",
                ".description",".job-detail-description",".keka-careers-job-description"
            ]:
                try:
                    if page.locator(sel).count() > 0:
                        desc_html = page.locator(sel).first.inner_html().strip()
                        if desc_html:
                            break
                except Exception:
                    pass

            title_final      = (ld and ld.get("title")) or title_dom
            location_final   = (ld and ld.get("location")) or location_candidate
            employment_final = (ld and ld.get("employment_type")) or employment_guess
            posted_final     = (ld and ld.get("posted_at")) or posted_guess

            j_en = dict(j)
            j_en.update({
                "title": title_final,
                "department": meta.get("department"),
                "location": location_final,
                "employment_type": employment_final,
                "posted_at": posted_final,
                "description_html": desc_html,
            })
            enriched.append(j_en)

        except Exception:
            j_en = dict(j)
            j_en.update({
                "title": None, "department": None, "location": None,
                "employment_type": None, "posted_at": None, "description_html": None
            })
            enriched.append(j_en)
        finally:
            try:
                if page:
                    page.close()
            except Exception:
                pass

    return enriched

# ---------------------- mapping to normalized schema ----------------------

_JOBTYPE = {
    1: "Internship",
    2: "Full-time",
    3: "Part-time",
    4: "Contract",
    5: "Temporary",
    6: "Freelance",
}

def _join_location(parts):
    parts = [p for p in parts if p and str(p).strip()]
    return ", ".join(parts) if parts else None

def _stable_external_id(raw: dict) -> str:
    # Prefer explicit ids; otherwise hash URL/title/payload
    for k in ("id", "jobId", "_id", "uuid", "slug"):
        if k in raw and raw[k]:
            return str(raw[k])
    basis = (raw.get("applyUrl") or raw.get("title") or json.dumps(raw, sort_keys=True))[:400]
    return hashlib.md5(basis.encode("utf-8")).hexdigest()

def map_kekahr_job(raw: dict, company_name: str, careers_url: str, base_url: str):
    """
    Works for both JSON items and DOM-enriched dicts.
    """
    job_id = str(raw.get("id") or "")  # JSON case
    apply_url = urljoin(base_url, f"/careers/jobdetails/{job_id}") if job_id else raw.get("applyUrl")

    title = raw.get("title") or raw.get("jobTitle")
    department = raw.get("departmentName") or raw.get("department") or raw.get("team")
    description_html = raw.get("description") or raw.get("description_html")

    # Location
    loc = None
    jl = raw.get("jobLocations") or []
    if isinstance(jl, list) and jl:
        first = jl[0]
        loc = _join_location([first.get("city"), first.get("state"), first.get("countryName")])
    if not loc:
        loc = raw.get("location")  # enrichment fallback string

    # Employment type
    employment = None
    job_type_num = raw.get("jobType")
    if isinstance(job_type_num, int):
        employment = _JOBTYPE.get(job_type_num)
    if not employment:
        employment = raw.get("employment_type")  # enrichment fallback

    posted_at = raw.get("publishedOn") or raw.get("posted_at")

    external_id = _stable_external_id(raw)

    return {
        "external_id": external_id,
        "ats_type": "kekahr",
        "company_name": company_name,
        "title": title,
        "department": department,
        "location_text": loc,
        "remote_type": "remote" if (loc or "").lower().startswith("remote") else None,
        "employment_type": employment,
        "posted_at": posted_at,
        "updated_at_source": None,
        "apply_url": apply_url,
        "source_url": careers_url,
        "description_html": description_html,
        "raw_payload": raw,
    }

__all__ = [
    "fetch_kekahr_jobs",
    "map_kekahr_job",
    "_base_url_from_careers",
]
