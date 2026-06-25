"""
PHASE 2 - SCRAPER (PRODUCTION VERSION)
======================================

Flow:
[q.seed.jobs] → Scraper → SeaweedFS → [q.parse.jobs]

Responsibilities:
1. Consume job messages from RabbitMQ
2. Crawl job detail HTML
3. Store raw HTML to SeaweedFS (or local fallback)
4. Publish enriched message to parse queue
"""

import sys
import os
import json
import time
import logging
from datetime import datetime

import requests
import pika

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.settings import RABBITMQ, QUEUES, VIETNAMWORKS, SEAWEEDFS, PIPELINE


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCRAPER] %(levelname)s - %(message)s"
)

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SeaweedFS Storage
# ─────────────────────────────────────────────

def save_to_seaweedfs(content: bytes, filename: str) -> str:
    """
    Upload HTML to SeaweedFS.
    Fallback: local disk if SeaweedFS unavailable.
    """
    try:
        assign = requests.get(
            f"{SEAWEEDFS['master_url']}/dir/assign",
            timeout=10
        )
        assign.raise_for_status()
        data = assign.json()

        file_id = data["fid"]
        volume_host = data.get("url", "")

        if "localhost" in volume_host or "127." in volume_host:
            upload_url = f"{SEAWEEDFS['volume_url']}/{file_id}"
        else:
            upload_url = f"http://{volume_host}/{file_id}"

        resp = requests.post(
            upload_url,
            files={"file": (filename, content, "text/html")},
            timeout=30
        )
        resp.raise_for_status()

        log.info(f"Saved to SeaweedFS → fid={file_id}")
        return file_id

    except Exception as e:
        log.warning(f"SeaweedFS failed: {e}")

        fallback_path = f"/tmp/{filename}"
        with open(fallback_path, "wb") as f:
            f.write(content)

        log.info(f"Fallback saved locally → {fallback_path}")
        return f"local:{fallback_path}"


# ─────────────────────────────────────────────
# Crawl Logic
# ─────────────────────────────────────────────

def crawl_job_page(job_url: str) -> bytes:
    """
    Crawl job detail page with retry.
    """
    if not job_url or "mock" in job_url:
        return _mock_html()

    headers = VIETNAMWORKS["headers"]
    max_retries = VIETNAMWORKS.get("max_retries", 3)

    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"Crawling attempt {attempt} → {job_url}")

            resp = requests.get(
                job_url,
                headers=headers,
                timeout=20
            )
            resp.raise_for_status()

            log.info(f"OK → {len(resp.content)} bytes")
            return resp.content

        except requests.RequestException as e:
            log.warning(f"Attempt {attempt} failed: {e}")
            time.sleep(attempt * 2)

    raise RuntimeError(f"Failed to crawl after {max_retries} attempts: {job_url}")


def _mock_html() -> bytes:
    return b"""
    <html>
      <head><title>Mock Job</title></head>
      <body>
        <h1 class="job-title">Data Engineer</h1>
        <div class="company-name">TechCorp</div>
        <div class="salary">25-40M</div>
        <div class="location">Hanoi</div>
        <div class="job-description">Mock description</div>
      </body>
    </html>
    """


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

    # Ensure queues exist
    for q in QUEUES.values():
        ch.queue_declare(queue=q, durable=True)

    ch.basic_qos(prefetch_count=1)

    return conn, ch


# ─────────────────────────────────────────────
# Process Message
# ─────────────────────────────────────────────

def process_message(ch, method, properties, body: bytes):
    msg = json.loads(body)

    job_url = msg.get("job_url")
    job_id = msg.get("job_id", "unknown")
    run_id = msg.get("run_id")

    log.info(f"Processing job_id={job_id}")

    try:
        # 1. Crawl HTML
        html = crawl_job_page(job_url)

        # 2. Store raw HTML
        filename = f"job_{job_id}_{int(time.time())}.html"
        file_id = save_to_seaweedfs(html, filename)

        # 3. Build message for parser
        out_msg = {
            **msg,
            "raw_file_id": file_id,
            "scraped_at": datetime.utcnow().isoformat(),
            "html_size": len(html),
            "phase": "scraped",
        }

        # 4. Publish to parse queue
        ok = ch.basic_publish(
            exchange="",
            routing_key=QUEUES["parse"],
            body=json.dumps(out_msg),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

        if not ok:
            raise RuntimeError("Publish to parse queue failed")

        # 5. ACK success
        ch.basic_ack(delivery_tag=method.delivery_tag)

        log.info(f"Done job_id={job_id} → file_id={file_id}")

        # rate limit
        time.sleep(VIETNAMWORKS.get("delay_between_requests", 1))

    except Exception as e:
        log.error(f"FAILED job_id={job_id}: {e}")

        # decide retry or drop
        requeue = PIPELINE.get("requeue_failed", False)

        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=requeue
        )


# ─────────────────────────────────────────────
# Consumer Loop (PROPER)
# ─────────────────────────────────────────────

def run_scraper():
    conn, ch = get_rabbitmq_channel()

    queue_name = QUEUES["seed"]

    log.info(f"Scraper listening on {queue_name}")

    def callback(ch, method, properties, body):
        process_message(ch, method, properties, body)

    ch.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=False
    )

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        log.info("Stopping scraper...")
        ch.stop_consuming()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== PHASE 2 SCRAPER START ===")
    run_scraper()
    log.info("=== SCRAPER STOP ===")