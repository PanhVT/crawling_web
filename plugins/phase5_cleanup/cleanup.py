"""
PHASE 5 - CLEANUP (IMPROVED)
============================
- Xóa raw HTML khỏi SeaweedFS sau khi đã load DB
- An toàn hơn: chỉ xóa khi chắc chắn đã insert vào fact_jobs
"""

import sys, os, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import psycopg2
from datetime import datetime, timedelta
from config.settings import SEAWEEDFS, POSTGRES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CLEANUP] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


# ── DB CONNECTION ─────────────────────────────────────────────

def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        dbname=POSTGRES["database"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
    )


# ── SEAWEEDFS DELETE ──────────────────────────────────────────

def delete_from_seaweedfs(file_id: str) -> bool:
    if not file_id:
        return False

    # local file
    if file_id.startswith("local:"):
        path = file_id.replace("local:", "")
        try:
            if os.path.exists(path):
                os.remove(path)
                log.info(f"[LOCAL DELETE] {path}")
            return True
        except Exception as e:
            log.warning(f"Local delete error: {e}")
            return False

    try:
        vol_id = file_id.split(",")[0]

        lookup = requests.get(
            f"{SEAWEEDFS['master_url']}/dir/lookup?volumeId={vol_id}",
            timeout=10
        )
        lookup.raise_for_status()

        vol_location = lookup.json()["locations"][0]["publicUrl"]

        delete_url = f"http://{vol_location}/{file_id}"

        resp = requests.delete(delete_url, timeout=10)

        if resp.status_code in (200, 202, 204):
            log.info(f"[SEAWEEDFS DELETE] {file_id}")
            return True

        log.warning(f"Delete failed HTTP {resp.status_code} for {file_id}")
        return False

    except Exception as e:
        log.warning(f"SeaweedFS delete error ({file_id}): {e}")
        return False


# ── QUERY FILES SAFE ──────────────────────────────────────────

def fetch_candidates(cur, threshold, limit=500):
    """
    Lấy batch file an toàn để cleanup
    """
    cur.execute("""
        SELECT job_id, raw_file_id
        FROM fact_jobs
        WHERE raw_file_id IS NOT NULL
          AND raw_file_id != ''
        ORDER BY job_id DESC
        LIMIT %s
    """, (limit,))
    return cur.fetchall()


# ── CLEANUP CORE ──────────────────────────────────────────────

def run_cleanup(limit=500, dry_run=False):
    conn = get_pg_conn()
    stats = {"found": 0, "deleted": 0, "failed": 0}

    try:
        with conn:
            with conn.cursor() as cur:

                rows = fetch_candidates(cur, None, limit)
                stats["found"] = len(rows)

                log.info(f"Found {len(rows)} files to cleanup")

                for job_id, raw_file_id in rows:
                    log.info(f"Processing job_id={job_id}")

                    if dry_run:
                        log.info(f"[DRY RUN] would delete {raw_file_id}")
                        continue

                    ok = delete_from_seaweedfs(raw_file_id)

                    if ok:
                        cur.execute("""
                            UPDATE fact_jobs
                            SET raw_file_id = NULL
                            WHERE job_id = %s
                        """, (job_id,))
                        stats["deleted"] += 1
                    else:
                        stats["failed"] += 1

    finally:
        conn.close()

    log.info(f"Cleanup result: {stats}")
    return stats


# ── ENTRYPOINT ────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== PHASE 5 CLEANUP START ===")
    run_cleanup(limit=200, dry_run=False)
    log.info("=== CLEANUP DONE ===")