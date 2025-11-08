# 🧠 ATS Scraper — Modular Multi-ATS Job Aggregator

This project implements a **modular scraper system** that collects and normalizes job listings from multiple Applicant Tracking Systems (ATS):

- ✅ KekaHR  
- ✅ DarwinBox  
- ✅ Oracle ORC (Oracle Recruiting Cloud)  
- ✅ Join.com  

The scraper provides both **API endpoints** and a **daily scheduled process** for refreshing data in PostgreSQL.

## 📘 Architecture Overview

The system is built with **FastAPI + APScheduler + PostgreSQL** and designed to be fully modular.

- **FastAPI** provides REST endpoints to trigger and inspect job data.
- **APScheduler** runs an automated background job every 24 hours (configurable) to refresh data.
- **PostgreSQL** stores normalized job listings and `RunLog` entries for each ATS/company.
- **Adapters** (`adapters/*.py`) encapsulate scraping logic for each ATS (KekaHR, DarwinBox, Oracle ORC, Join.com).
- **Core logic** handles delta-based updates — new, updated, and closed jobs.
- **Logging** and **RunLog** track each daily run for observability.

This modular design allows easy extension by adding new ATS adapters without affecting others.


## ⚙️ Setup Instructions

### 1️⃣ Clone & Install
```bash
git clone <https://github.com/sayu320/ats_scraper.git>
cd ats-scraper
pip install -r requirements.txt

2️⃣ Setup Database
Edit .env or app/database.py to point to your PostgreSQL instance.

3️⃣ Run
uvicorn app.main:app --reload

4️⃣ Access Swagger UI
http://localhost:8000/docs

5️⃣ Scheduler
Default run time = 09:00 IST
Change with:
set SCHEDULER_TIME=10:00


✅ That’s the “setup steps” section done.

---

## 🧩 3️⃣ **API Mapping Summary (for each ATS)**
> “API mapping summary for each ATS” → they want a quick table showing how you found / used endpoints.

✅ You already have this table in the README (`ATS API Mapping Summary`).  
Make sure it’s clear and complete:

```markdown
## 🧩 ATS API Mapping Summary

| ATS | Type | API/Pattern | Example |
|------|------|-------------|----------|
| **KekaHR** | JSON API | `/careers/api/embedjobs/default/active/{tenant}` | `https://10decoders.keka.com/careers/api/embedjobs/...` |
| **DarwinBox** | JSON API | `/ms/candidateapi/job?page=N&limit=X` | `https://adaglobal.darwinbox.com/ms/candidateapi/job` |
| **Oracle ORC** | REST API | `/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails` | `https://don.fa.em2.oraclecloud.com/...` |
| **Join.com** | HTML Scrape | Parse `<script id="__NEXT_DATA__">` or job cards | `https://join.com/companies/qdrant` |


🧠 5️⃣ Data Mapping
## 🗂️ Data Mapping Schema

| Normalized Field | Description | Source Example |
|------------------|--------------|----------------|
| external_id | Unique job ID from ATS | KekaHR: `id`, DarwinBox: `jobId` |
| ats_type | ATS system name | `"kekahr"`, `"darwinbox"`, etc. |
| company_name | Employer name | Static / tenant name |
| title | Job title | `jobTitle`, `title` |
| department | Department / function | `department` |
| location_text | City or “Remote” | `location`, `workplace` |
| remote_type | On-site / Hybrid / Remote | Source flag if available |
| employment_type | Full-time, part-time, etc. | From ATS field |
| posted_at | Original posting date | ATS-specific date field |
| updated_at_source | Last update time | ATS-specific field |
| apply_url | Application page URL | Link to apply |
| source_url | Original career page | Company careers URL |
| description_html | Full job description | HTML or text content |


🧾 5️⃣ Research Process
# 🔍 ATS Research Notes

## KekaHR
- Found `/careers/api/embedjobs/default/active/{tenant}` via browser Network tab.
- Returns JSON containing job data under `jobs` key.
- Supports `endpoint_override` parameter for multi-tenant.

## DarwinBox
- Reverse-engineered `/ms/candidateapi/job?page=N&limit=X`.
- Extracted JSON job list with pagination.

## Oracle ORC
- Used `hcmUI` public site and derived REST base from host + site number.
- Built REST base: `https://<host>/hcmRestApi/resources/latest/`.
- Extracted jobs from `/recruitingCEJobRequisitionDetails` endpoint.

## Join.com
- No API. Parsed server-rendered HTML or embedded JSON.
- Used Playwright + BeautifulSoup to extract job cards.

