"""
PHASE 6 - STORAGE DISPATCHER (IMPROVED)
=======================================
- Sync PostgreSQL → Qdrant + Elasticsearch
- Avoid duplicate indexing
- Safer payload handling
"""

import sys, os, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import psycopg2
import requests
from datetime import datetime
from config.settings import (
    POSTGRES,
    QDRANT_URL,
    QDRANT_COLLECTION,
    ELASTICSEARCH_URL,
    ELASTICSEARCH_INDEX
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STORAGE] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────────

def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        dbname=POSTGRES["database"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
    )


# ─────────────────────────────────────────────
# SAFE TEXT
# ─────────────────────────────────────────────

def safe_text(x):
    if not x:
        return ""
    return str(x)


def safe_list(x):
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        return [i.strip() for i in x.split(",") if i.strip()]
    return []


# ─────────────────────────────────────────────
# QDRANT
# ─────────────────────────────────────────────

def ensure_qdrant():
    try:
        resp = requests.put(
            f"{QDRANT_URL}/collections/{QDRANT_COLLECTION}",
            json={
                "vectors": {
                    "size": 384,
                    "distance": "Cosine"
                }
            },
            timeout=10
        )
        log.info("Qdrant ready")
    except Exception as e:
        log.warning(f"Qdrant error: {e}")


def embed(text: str):
    """
    TEMP embedding (replace with sentence-transformers later)
    """
    import hashlib
    h = hashlib.sha256(text.encode()).hexdigest()

    vec = []
    for i in range(0, 64, 2):
        v = int(h[i % len(h)], 16) / 15.0
        vec.append(v)

    while len(vec) < 384:
        vec.append(0.0)

    return vec[:384]


def push_qdrant(job):
    job_id = job["job_id"]

    text = " ".join([
        safe_text(job.get("job_title")),
        safe_text(job.get("job_description")),
        " ".join(safe_list(job.get("required_skills")))
    ])

    vector = embed(text)

    payload = {
        "job_id": job_id,
        "title": job.get("job_title"),
        "company": job.get("company_name"),
        "city": job.get("city"),
        "salary_min": job.get("salary_min"),
        "salary_max": job.get("salary_max"),
        "url": job.get("source_url"),
        "indexed_at": datetime.utcnow().isoformat()
    }

    try:
        r = requests.put(
            f"{QDRANT_URL}/collections/{QDRANT_COLLECTION}/points",
            json={
                "points": [{
                    "id": job_id,
                    "vector": vector,
                    "payload": payload
                }]
            },
            timeout=15
        )
        return r.ok
    except Exception as e:
        log.warning(f"Qdrant error job={job_id}: {e}")
        return False


# ─────────────────────────────────────────────
# ELASTIC
# ─────────────────────────────────────────────

def ensure_es():
    try:
        requests.put(
            f"{ELASTICSEARCH_URL}/{ELASTICSEARCH_INDEX}",
            json={
                "mappings": {
                    "properties": {
                        "job_title": {"type": "text"},
                        "company_name": {"type": "keyword"},
                        "city": {"type": "keyword"},
                        "job_description": {"type": "text"}
                    }
                }
            },
            timeout=10
        )
        log.info("ES ready")
    except Exception as e:
        log.warning(f"ES error: {e}")


def push_es(job):
    job_id = job["job_id"]

    doc = {
        "job_id": job_id,
        "job_title": job.get("job_title"),
        "company_name": job.get("company_name"),
        "city": job.get("city"),
        "job_description": job.get("job_description"),
        "skills": safe_list(job.get("required_skills")),
        "indexed_at": datetime.utcnow().isoformat()
    }

    try:
        r = requests.put(
            f"{ELASTICSEARCH_URL}/{ELASTICSEARCH_INDEX}/_doc/{job_id}",
            json=doc,
            timeout=15
        )
        return r.ok
    except Exception as e:
        log.warning(f"ES error job={job_id}: {e}")
        return False


# ─────────────────────────────────────────────
# MAIN DISPATCHER
# ─────────────────────────────────────────────

def run(limit=100):
    ensure_qdrant()
    ensure_es()

    conn = get_pg_conn()
    stats = {"ok": 0, "q": 0, "es": 0, "fail": 0}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT job_id, job_title, job_description,
                       company_name, city,
                       salary_min, salary_max,
                       required_skills, source_url
                FROM fact_jobs
                ORDER BY job_id DESC
                LIMIT %s
            """, (limit,))

            rows = cur.fetchall()

            for r in rows:
                job = dict(zip([d[0] for d in cur.description], r))

                q = push_qdrant(job)
                e = push_es(job)

                stats["ok"] += 1
                stats["q"] += int(q)
                stats["es"] += int(e)

                if not (q or e):
                    stats["fail"] += 1

    finally:
        conn.close()

    log.info(f"Done: {stats}")
    return stats


if __name__ == "__main__":
    log.info("=== STORAGE DISPATCHER START ===")
    run(100)
    log.info("=== DONE ===")