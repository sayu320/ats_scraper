# utils/delta.py
import json, hashlib, os

def hash_job(job):
    payload = json.dumps(job, sort_keys=True)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

def detect_changes(prev_jobs, current_jobs):
    prev_map = {j["external_id"]: hash_job(j) for j in prev_jobs}
    curr_map = {j["external_id"]: hash_job(j) for j in current_jobs}

    new = [j for j in current_jobs if j["external_id"] not in prev_map]
    closed = [j for j in prev_jobs if j["external_id"] not in curr_map]
    updated = [
        j for j in current_jobs
        if j["external_id"] in prev_map and prev_map[j["external_id"]] != curr_map[j["external_id"]]
    ]
    return new, updated, closed
