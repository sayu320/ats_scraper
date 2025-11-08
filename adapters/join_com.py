# adapters/join_com.py
from playwright.sync_api import sync_playwright
from urllib.parse import urlparse
import time, json, hashlib, requests
from bs4 import BeautifulSoup

# ---------- 1️⃣  DOM SCRAPER ----------
def fetch_join_dom_jobs(careers_url: str):
    """Scrape jobs from Join.com company careers page (SSR DOM fallback)"""
    slug = careers_url.rstrip("/").split("/")[-1]
    JOB_LINK_SEL = f'a[href^="https://join.com/companies/{slug}/"]'

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        )

        page.goto(careers_url, wait_until="domcontentloaded")

        # close cookie banners if present
        for txt in ["Accept all", "Accept All", "I agree", "Allow all", "Agree", "Accept cookies"]:
            try:
                page.get_by_role("button", name=txt, exact=False).click(timeout=800)
            except Exception:
                pass

        page.wait_for_load_state("networkidle", timeout=10000)
        for _ in range(6):
            page.mouse.wheel(0, 2000)
            time.sleep(0.4)

        page.wait_for_selector(JOB_LINK_SEL, timeout=5000)
        hrefs = page.eval_on_selector_all(
            "a[href]",
            "(nodes) => Array.from(new Set(nodes.map(n => n.getAttribute('href'))))"
        )
        hrefs = [h for h in hrefs if h and h.startswith(f"https://join.com/companies/{slug}/")]

        jobs = []
        for h in hrefs:
            title = page.locator(f'a[href="{h}"]').first.inner_text().strip()
            jobs.append({
                "title": title or None,
                "applyUrl": h,
            })

        browser.close()
        return jobs


# ---------- 2️⃣  PARSER ----------
def parse_join_block(text: str) -> dict:
    lines = [ln.strip() for ln in (text or "").split("\n") if ln.strip()]
    role = lines[0] if lines else None
    location = lines[1] if len(lines) >= 2 else None
    employment_type = lines[2] if len(lines) >= 3 else None
    department = lines[3] if len(lines) >= 4 else None

    return {
        "role": role,
        "location": location,
        "employment_type": employment_type,
        "department": department,
    }


# ---------- 3️⃣  NORMALIZER ----------
def map_join_dom_job(job, company_name, careers_url):
    parsed = parse_join_block(job.get("title") or "")

    ext = None
    try:
        path = urlparse(job["applyUrl"]).path
        ext = path.rstrip("/").split("/")[-1] or None
    except Exception:
        pass
    if not ext:
        basis = (job.get("applyUrl") or job.get("title") or "") + (company_name or "")
        ext = hashlib.md5(basis.encode("utf-8")).hexdigest()

    return {
        "external_id": ext,
        "ats_type": "join",
        "company_name": company_name,
        "title": parsed["role"],
        "department": parsed["department"],
        "location_text": parsed["location"],
        "remote_type": "remote" if (parsed["location"] or "").lower().startswith("remote") else None,
        "employment_type": parsed["employment_type"],
        "posted_at": None,
        "updated_at_source": None,
        "apply_url": job["applyUrl"],
        "source_url": careers_url,
        "description_html": None,
        "raw_payload": job,
    }
