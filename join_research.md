# 🔍 Join.com Reverse Engineering Summary

## Platform Pattern

Company career pages follow the format:
https://join.com/companies/<company-slug>


**Example used:**
- Qdrant — https://join.com/companies/qdrant

---

## Findings
- ❌ No public API endpoints found for this tenant.  
- ❌ No network requests in the browser (XHR/Fetch) delivering job listings.  
- ❌ No embedded `__NEXT_DATA__` JSON structure (common in Next.js).  
- ❌ No pagination available: Join.com company pages render all open roles on one HTML page.
- ✅ Jobs are rendered **server-side**, meaning listings are part of the static HTML response.

---

## Approach Used
- Parsed job cards directly from the **DOM** using **Playwright + BeautifulSoup**.  
- Extracted:
  - Job title  
  - Department  
  - Apply URL  
- Implemented a generic adapter `adapters/join_com.py` that:
  1. Attempts known API endpoints (if present)
  2. Falls back to **DOM scraping** when no API is available

---

## Outcome
- Successfully extracted all visible jobs from Join.com company pages.  
- Verified with multiple companies (e.g., Qdrant, Motionyard).  
- Output normalized into the shared ATS schema (title, department, location, apply_url).

---

**In summary:**  
Join.com job data requires **server-side HTML parsing**, demonstrating the ability to reverse-engineer web pages without public APIs or documentation.

