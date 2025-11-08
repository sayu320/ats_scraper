# 🔍 ATS Research & Reverse Engineering Notes

This document summarizes the discovery and reverse-engineering process for each supported ATS (Applicant Tracking System).  
Each section explains how the public job data endpoints were found and how they were integrated into the unified scraper system.

---

## 🟢 1. KekaHR

### Platform Pattern
Company career pages follow:
https://<tenant>.keka.com/careers/


**Example used:**
- 10Decoders → https://10decoders.keka.com/careers/

---

### Findings
- KekaHR embeds job listings via a **JSON API** request:
/careers/api/embedjobs/default/active/{tenant-uuid}

Pagination: ❌ No pagination.
All active jobs returned in a single JSON response.

- Each tenant has a unique `tenant-uuid` at the end of the URL.
- The response contains:
- `jobs` → array of open positions
- Each job has fields like `id`, `title`, `department`, `location`, etc.

---

### Approach Used
- Inspected browser **Network → XHR** panel on the careers page.
- Located an XHR call to `/careers/api/embedjobs/...`.
- Confirmed it returns structured JSON data with all job listings.
- Implemented `adapters/kekahr.py` that:
- Fetches JSON from the API endpoint.
- Maps each job to normalized fields.
- Falls back to tenant-level endpoint if needed.

---

### Outcome
- Clean JSON-based integration.
- Fast, no JavaScript rendering required.
- Easily parameterized for different KekaHR tenants.

---

## 🔵 2. DarwinBox

### Platform Pattern
Company career pages follow:
https://<tenant>.darwinbox.com/ms/candidate/careers


**Example used:**
- ADA → https://adaglobal.darwinbox.com/ms/candidate/careers

---

### Findings
- Initial HTML loads job cards via an AJAX call to:
/ms/candidateapi/job?page=1&limit=50

- The API returns paginated JSON with fields like:
- `jobTitle`, `jobId`, `department`, `location`, `applyLink`
- The API does not require authentication and is consistent across tenants.
- Pagination: Uses page and limit parameters.

---

### Approach Used
- Observed network requests while scrolling on the DarwinBox careers page.
- Identified `/ms/candidateapi/job` as the endpoint returning job data.
- Integrated this into `adapters/darwinbox.py`:
- Iterates over pages until no more results.
- Normalizes all job fields into the shared schema.
- Supports custom page size via query params.

---

### Outcome
- Reliable, structured API-based integration.
- No HTML scraping required.
- Supports pagination and large result sets.

---

## 🟣 3. Oracle ORC (Oracle Recruiting Cloud)

### Platform Pattern
Public job portals hosted under Oracle Cloud use:
https://<host>/hcmUI/CandidateExperience/en/sites/<SITE>/jobs


**Example used:**
- Euroclear → https://don.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1003/jobs

---

### Findings
- Oracle ORC internally uses a REST API under:
/hcmRestApi/resources/latest/

- The relevant endpoint for job requisitions:
recruitingCEJobRequisitionDetails
- Example:
https://don.fa.em2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails?finder=FindJobRequisitions;siteNumber=CX_1003

- Returns JSON with fields:
- `Title`, `JobReqId`, `PrimaryLocation`, `Department`, `ApplyURL`, `PostingDate`, etc.
- Pagination: Controlled by offset and limit query params.
---

### Approach Used
- Started from the visible job site (`CandidateExperience` page).
- Opened the **Network tab** → searched for `hcmRestApi`.
- Identified multiple API calls, among which `recruitingCEJobRequisitionDetails` contained all job data.
- Derived a reusable `rest_base` pattern:
https://<host>/hcmRestApi/resources/latest/
- Implemented `adapters/oracle_orc.py` to:
- Build REST base dynamically.
- Paginate results.
- Normalize job data to the unified schema.

---

### Outcome
- Robust REST-based integration.
- Multi-tenant support via dynamic host + site number.
- Captures full metadata (posting date, location, etc.).

---

## 🟡 4. Join.com

*(See detailed research in `join_research.md` for full analysis.)*

### Summary
- No API; jobs are server-rendered in HTML.
- Parsed using **Playwright + BeautifulSoup**.
- Fallbacks to reading embedded JSON (`__NEXT_DATA__`) if present.
- Extracts job title, department, and apply URL directly from DOM.
- Implemented in `adapters/join_com.py`.

---

## 🧩 Unified Normalization

All four adapters output a consistent dictionary structure:

```python
{
"external_id": str,
"ats_type": str,
"company_name": str,
"title": str,
"department": str,
"location_text": str,
"remote_type": str,
"employment_type": str,
"posted_at": str,
"updated_at_source": str,
"apply_url": str,
"source_url": str,
"description_html": str,
}
