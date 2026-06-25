"""
PHASE 1 - SEEDER (PRODUCTION VERSION)
=====================================
- Lấy job listings từ VietnamWorks API (hoặc mock fallback)
- Validate + filter dữ liệu
- Đẩy message vào RabbitMQ queue
- Hỗ trợ retry + safe connection + confirm publish

Flow:
VietnamWorks API → Seeder → RabbitMQ (q.seed.jobs) → Scraper
"""

import sys
import os
import json
import time
import uuid
import logging
from datetime import datetime

import requests
import pika

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.settings import RABBITMQ, QUEUES, VIETNAMWORKS, PIPELINE


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SEEDER] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# RabbitMQ
# ─────────────────────────────────────────────

def get_rabbitmq_channel():
    creds = pika.PlainCredentials(
        RABBITMQ["user"],
        RABBITMQ["password"]
    )

    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ["host"],
            port=RABBITMQ["port"],
            virtual_host=RABBITMQ["vhost"],
            credentials=creds,
            heartbeat=30
        )
    )

    ch = conn.channel()

    # Enable publish confirm mode (important for reliability)
    ch.confirm_delivery()

    # Declare queues (idempotent)
    for q in QUEUES.values():
        ch.queue_declare(queue=q, durable=True)

    return conn, ch


# ─────────────────────────────────────────────
# VietnamWorks API
# ─────────────────────────────────────────────

def fetch_job_listings(keyword="data engineer", page=0, size=10):
    url = VIETNAMWORKS["api_search"]

    params = {
        "query": keyword,
        "page": page,
        "size": size,
        "langs": "vi",
    }

    headers = VIETNAMWORKS["headers"]

    log.info(f"Fetching jobs | keyword={keyword} page={page} size={size}")

    # Retry mechanism
    for attempt in range(3):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=15
            )
            resp.raise_for_status()

            data = resp.json()
            jobs = data.get("data", {}).get("jobs", [])

            log.info(f"Fetched {len(jobs)} jobs from API")
            return jobs

        except requests.RequestException as e:
            log.warning(f"API attempt {attempt+1} failed: {e}")
            time.sleep(2 * (attempt + 1))

    # fallback mock
    log.error("API failed after retries → using MOCK data")
    return _mock_job_listings()


def _mock_job_listings():
    return [
        {
            "jobId": "mock-001",
            "jobTitle": "Data Engineer (Python/Spark)",
            "jobUrl": "https://www.vietnamworks.com/mock-job",
            "companyName": "TechCorp Vietnam",
        }
    ]


# ─────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────

def is_valid_job(job: dict) -> bool:
    return bool(job.get("jobId") and job.get("jobUrl"))


# ─────────────────────────────────────────────
# Publish to Queue
# ─────────────────────────────────────────────

def publish_message(ch, queue, message: dict) -> bool:
    return ch.basic_publish(
        exchange="",
        routing_key=queue,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent
            content_type="application/json",
            message_id=str(uuid.uuid4()),
        )
    )


# ─────────────────────────────────────────────
# Seeder Core
# ─────────────────────────────────────────────

def seed_jobs_to_queue(run_id: str, keyword="data engineer"):
    conn = None
    ch = None
    seeded_count = 0

    try:
        conn, ch = get_rabbitmq_channel()

        limit = 1 if PIPELINE.get("test_mode") else 20
        jobs = fetch_job_listings(keyword=keyword, size=limit)

        if PIPELINE.get("test_mode"):
            jobs = jobs[:PIPELINE.get("test_job_limit", 5)]
            log.info(f"TEST MODE → limiting to {len(jobs)} jobs")

        for job in jobs:

            if not is_valid_job(job):
                log.warning(f"Invalid job skipped: {job}")
                continue

            message = {
                "run_id": run_id,
                "job_id": str(job.get("jobId")),
                "job_url": job.get("jobUrl"),
                "job_title": job.get("jobTitle"),
                "company_name": job.get("companyName"),
                "seeded_at": datetime.utcnow().isoformat(),
                "phase": "seed",
            }

            success = publish_message(ch, QUEUES["seed"], message)

            if not success:
                log.error(f"Failed to publish job_id={message['job_id']}")
                continue

            seeded_count += 1
            log.info(f"Seeded job: {message['job_id']}")

    finally:
        if ch:
            ch.close()
        if conn:
            conn.close()

    log.info(f"SEEDER DONE → {seeded_count} jobs pushed")
    return seeded_count


# ─────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────

if __name__ == "__main__":
    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    keyword = sys.argv[1] if len(sys.argv) > 1 else "data engineer"

    log.info(f"START SEEDER | run_id={run_id} | keyword={keyword}")

    count = seed_jobs_to_queue(run_id, keyword)

    log.info(f"FINISH SEEDER | total={count}")