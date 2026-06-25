"""
dag_pipeline.py  –  VietnamWorks Job Scraping Pipeline
=======================================================

Architecture
------------
  [Task: init]
      └─► ghi pipeline_runs (status=running)
  [Task: seed]
      └─► VietnamWorks API → RabbitMQ: q.seed.jobs
  [Task: scrape]
      └─► q.seed.jobs → crawl HTML → SeaweedFS → q.parse.jobs
  [Task: parse]
      └─► q.parse.jobs → fetch SeaweedFS → BeautifulSoup → q.load.jobs
  [Task: load]
      └─► q.load.jobs → upsert dim_* → insert fact_jobs
  [Task: report]
      └─► cập nhật pipeline_runs (status=completed/failed)

Schedule : 01:00 UTC hằng ngày
Retries  : 3 lần, exponential backoff (5 → 30 phút)
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timedelta

import pika
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Project imports ────────────────────────────────────────────────────────────
# Đặt PIPELINE_HOME trong airflow.cfg hoặc Dockerfile:
#   ENV PIPELINE_HOME=/opt/pipeline
import sys, os
sys.path.insert(0, os.environ.get("PIPELINE_HOME", "/opt/pipeline"))

from config.settings import RABBITMQ, QUEUES, POSTGRES, PIPELINE

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers dùng chung
# ══════════════════════════════════════════════════════════════════════════════

def _pg_conn() -> psycopg2.extensions.connection:
    """Trả về connection mới tới PostgreSQL vietnamworks DB."""
    return psycopg2.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        dbname=POSTGRES["database"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
    )


def _make_rabbit_channel() -> tuple:
    """Tạo RabbitMQ BlockingConnection + channel, khai báo tất cả queues."""
    creds = pika.PlainCredentials(RABBITMQ["user"], RABBITMQ["password"])
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ["host"],
            port=RABBITMQ["port"],
            virtual_host=RABBITMQ["vhost"],
            credentials=creds,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
    )
    ch = conn.channel()
    for q in QUEUES.values():
        ch.queue_declare(queue=q, durable=True)
    return conn, ch


def _queue_depth(queue_name: str) -> int:
    """Số message sẵn sàng trong queue (passive declare, không tạo mới)."""
    conn, ch = _make_rabbit_channel()
    try:
        resp = ch.queue_declare(queue=queue_name, durable=True, passive=True)
        return resp.method.message_count
    finally:
        conn.close()


def _wait_queue_drained(
    queue_name: str,
    poll_interval: int = 5,
    timeout: int = 7200,
) -> None:
    """
    Block cho đến khi queue về 0 message (hoặc timeout).
    Gọi ngay sau mỗi consumer để đảm bảo phase kế tiếp
    được trigger chỉ khi toàn bộ message đã được xử lý.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        depth = _queue_depth(queue_name)
        log.info("  ⏳ queue '%s': %d message(s) còn lại …", queue_name, depth)
        if depth == 0:
            log.info("  ✓ queue '%s' đã drain xong.", queue_name)
            return
        time.sleep(poll_interval)
    raise TimeoutError(
        f"Queue '{queue_name}' vẫn chưa drain sau {timeout}s – "
        "kiểm tra consumer log hoặc tăng execution_timeout."
    )


# ── pipeline_runs helpers ──────────────────────────────────────────────────────

def _run_upsert(run_id: str, dag_run_id: str, **cols) -> None:
    """
    INSERT hoặc UPDATE 1 row trong pipeline_runs.
    Chỉ cập nhật các cột được truyền vào (không ghi đè cột khác).

    Các cột hợp lệ (từ init.sql):
      status, started_at, finished_at,
      jobs_seeded, jobs_scraped, jobs_parsed, jobs_loaded,
      error_message, dag_id
    """
    set_pairs  = ", ".join(f"{k} = %s" for k in cols)
    set_values = list(cols.values())

    sql = f"""
        INSERT INTO pipeline_runs (run_id, dag_id, started_at, status)
        VALUES (%s, %s, NOW(), 'running')
        ON CONFLICT (run_id)
        DO UPDATE SET {set_pairs}
    """

    conn = _pg_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, [run_id, dag_run_id] + set_values)
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
#  Task 0 – init
# ══════════════════════════════════════════════════════════════════════════════

def task_init(**context) -> None:
    """
    Tạo run_id duy nhất cho toàn DAG run,
    push lên XCom, ghi hàng đầu vào pipeline_runs (status=running).
    """
    logical_date: datetime = context["logical_date"]
    run_id = (
        f"run_{logical_date.strftime('%Y%m%d_%H%M%S')}"
        f"_{uuid.uuid4().hex[:6]}"
    )

    log.info("=== PIPELINE BẮT ĐẦU | run_id=%s ===", run_id)
    context["ti"].xcom_push(key="run_id", value=run_id)

    _run_upsert(
        run_id=run_id,
        dag_run_id=context["run_id"],
        dag_id="vietnamworks_scrape_pipeline",
        status="running",
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Task 1 – seed
# ══════════════════════════════════════════════════════════════════════════════

def task_seed(**context) -> None:
    """
    Phase 1 – Seeder:
      - Gọi VietnamWorks API để lấy danh sách job
      - Push từng URL vào RabbitMQ q.seed.jobs
    Kết quả: jobs_seeded lưu vào XCom + pipeline_runs.
    """
    from seeder import seed_jobs_to_queue

    run_id:  str = context["ti"].xcom_pull(task_ids="init", key="run_id")
    keyword: str = context["dag_run"].conf.get("keyword", "data engineer")

    log.info("▶ [SEED] run_id=%s  keyword='%s'", run_id, keyword)

    seeded = seed_jobs_to_queue(run_id=run_id, keyword=keyword)

    # Kiểm tra xác nhận message đã vào queue
    depth = _queue_depth(QUEUES["seed"])
    log.info("  [SEED] seeded=%d | q.seed.jobs depth=%d", seeded, depth)

    context["ti"].xcom_push(key="seeded_count", value=seeded)
    _run_upsert(
        run_id=run_id,
        dag_run_id=context["run_id"],
        status="seeded",
        jobs_seeded=seeded,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Task 2 – scrape
# ══════════════════════════════════════════════════════════════════════════════

def task_scrape(**context) -> None:
    """
    Phase 2 – Scraper:
      - Consume q.seed.jobs
      - Crawl HTML từng job URL
      - Lưu raw HTML lên SeaweedFS (fallback: /tmp)
      - Push message (+ raw_file_id) vào q.parse.jobs
    Đợi q.seed.jobs drain hẳn trước khi kết thúc task.
    """
    from scraper import run_scraper

    run_id:  str = context["ti"].xcom_pull(task_ids="init", key="run_id")
    seeded:  int = context["ti"].xcom_pull(task_ids="seed", key="seeded_count") or 0

    log.info("▶ [SCRAPE] run_id=%s  expect ~%d jobs", run_id, seeded)

    scraped = run_scraper(max_messages=seeded or None)

    _wait_queue_drained(QUEUES["seed"], poll_interval=5, timeout=7200)

    log.info("  [SCRAPE] scraped=%d", scraped)
    context["ti"].xcom_push(key="scraped_count", value=scraped)
    _run_upsert(
        run_id=run_id,
        dag_run_id=context["run_id"],
        status="scraped",
        jobs_scraped=scraped,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Task 3 – parse
# ══════════════════════════════════════════════════════════════════════════════

def task_parse(**context) -> None:
    """
    Phase 3 – Parser:
      - Consume q.parse.jobs
      - Lấy raw HTML từ SeaweedFS (hoặc /tmp fallback)
      - BeautifulSoup → structured dict
      - Push vào q.load.jobs
    Đợi q.parse.jobs drain trước khi kết thúc task.
    """
    from parser import run_parser

    run_id:  str = context["ti"].xcom_pull(task_ids="init", key="run_id")
    scraped: int = context["ti"].xcom_pull(task_ids="scrape", key="scraped_count") or 0

    log.info("▶ [PARSE] run_id=%s  expect ~%d records", run_id, scraped)

    parsed = run_parser(max_messages=scraped or None)

    _wait_queue_drained(QUEUES["parse"], poll_interval=5, timeout=3600)

    log.info("  [PARSE] parsed=%d", parsed)
    context["ti"].xcom_push(key="parsed_count", value=parsed)
    _run_upsert(
        run_id=run_id,
        dag_run_id=context["run_id"],
        status="parsed",
        jobs_parsed=parsed,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Task 4 – load
# ══════════════════════════════════════════════════════════════════════════════

def task_load(**context) -> None:
    """
    Phase 4 – DbLoader:
      - Consume q.load.jobs
      - Upsert dim_company, dim_location, dim_category, dim_date
      - INSERT fact_jobs (+ pipeline_run_id)
    Đợi q.load.jobs drain trước khi kết thúc task.
    """
    from dbloader import run_dbloader

    run_id: str = context["ti"].xcom_pull(task_ids="init", key="run_id")
    parsed: int = context["ti"].xcom_pull(task_ids="parse", key="parsed_count") or 0

    log.info("▶ [LOAD] run_id=%s  expect ~%d records → PostgreSQL", run_id, parsed)

    loaded = run_dbloader(max_messages=parsed or None)

    _wait_queue_drained(QUEUES["load"], poll_interval=5, timeout=1800)

    log.info("  [LOAD] loaded=%d", loaded)
    context["ti"].xcom_push(key="loaded_count", value=loaded)
    _run_upsert(
        run_id=run_id,
        dag_run_id=context["run_id"],
        status="loaded",
        jobs_loaded=loaded,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Task 5 – report
# ══════════════════════════════════════════════════════════════════════════════

def task_report(**context) -> None:
    """
    Tổng kết run:
      - Đọc counts từ XCom
      - Cập nhật pipeline_runs (status=completed | failed, finished_at=NOW())
      - In bảng summary lên Airflow log
      - Cảnh báo nếu drop rate > 20% giữa bất kỳ 2 phase nào
    trigger_rule=all_done → luôn chạy kể cả khi upstream fail.
    """
    ti = context["ti"]
    run_id   = ti.xcom_pull(task_ids="init",   key="run_id")   or "unknown"
    seeded   = ti.xcom_pull(task_ids="seed",   key="seeded_count")  or 0
    scraped  = ti.xcom_pull(task_ids="scrape", key="scraped_count") or 0
    parsed   = ti.xcom_pull(task_ids="parse",  key="parsed_count")  or 0
    loaded   = ti.xcom_pull(task_ids="load",   key="loaded_count")  or 0

    # Xác định trạng thái cuối
    all_success = all([seeded, scraped, parsed, loaded])
    final_status = "completed" if all_success else "failed"

    # Cập nhật pipeline_runs
    conn = _pg_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_runs SET
                        status       = %s,
                        finished_at  = NOW(),
                        jobs_seeded  = %s,
                        jobs_scraped = %s,
                        jobs_parsed  = %s,
                        jobs_loaded  = %s
                    WHERE run_id = %s
                """, (final_status, seeded, scraped, parsed, loaded, run_id))
    finally:
        conn.close()

    # Summary log
    log.info(
        "\n"
        "╔══════════════════════════════════════════════╗\n"
        "║       VIETNAMWORKS PIPELINE  –  SUMMARY      ║\n"
        "╠══════════════════════════════════════════════╣\n"
        "║  run_id   : %-32s║\n"
        "║  status   : %-32s║\n"
        "╠══════════════════════════════════════════════╣\n"
        "║  seeded   : %-5d                            ║\n"
        "║  scraped  : %-5d                            ║\n"
        "║  parsed   : %-5d                            ║\n"
        "║  loaded   : %-5d                            ║\n"
        "╚══════════════════════════════════════════════╝",
        run_id, final_status,
        seeded, scraped, parsed, loaded,
    )

    # Drop-rate warnings
    pairs = [
        ("seed→scrape", seeded,  scraped),
        ("scrape→parse", scraped, parsed),
        ("parse→load",  parsed,  loaded),
    ]
    for label, total, done in pairs:
        if total and done < total * 0.8:
            log.warning(
                "⚠ Drop rate cao [%s]: %d → %d (%.0f%% mất)",
                label, total, done,
                100 * (total - done) / total,
            )


# ══════════════════════════════════════════════════════════════════════════════
#  DAG definition
# ══════════════════════════════════════════════════════════════════════════════

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="vietnamworks_scrape_pipeline",
    description=(
        "VietnamWorks Job Pipeline: "
        "Seed → Scrape (SeaweedFS) → Parse → Load (PostgreSQL Star Schema)"
    ),
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 1 * * *",   # 01:00 UTC hằng ngày
    catchup=False,
    max_active_runs=1,
    params={
        "keyword": "data engineer",  # override khi trigger manual
    },
    tags=["vietnamworks", "scraper", "rabbitmq", "etl", "postgres"],
    doc_md="""
## VietnamWorks Job Scraping Pipeline

### Dependency chain
```
init → seed → scrape → parse → load → report
```

### Infrastructure
| Component  | Vai trò                               |
|------------|---------------------------------------|
| RabbitMQ   | Message broker giữa các phase         |
| SeaweedFS  | Lưu raw HTML (Phase 2)                |
| PostgreSQL | Star schema: dim_* + fact_jobs        |

### Trigger manual với keyword tùy chọn
```bash
airflow dags trigger vietnamworks_scrape_pipeline \\
  --conf '{"keyword": "backend engineer"}'
```

### Monitoring
- Xem trạng thái từng run: `SELECT * FROM pipeline_runs ORDER BY started_at DESC;`
- Query tổng hợp: `SELECT * FROM v_jobs_full LIMIT 100;`
""",
) as dag:

    init = PythonOperator(
        task_id="init",
        python_callable=task_init,
        doc_md="Sinh run_id duy nhất, ghi row đầu vào **pipeline_runs**.",
    )

    seed = PythonOperator(
        task_id="seed",
        python_callable=task_seed,
        doc_md="VietnamWorks API → **q.seed.jobs** (RabbitMQ).",
        execution_timeout=timedelta(minutes=15),
    )

    scrape = PythonOperator(
        task_id="scrape",
        python_callable=task_scrape,
        doc_md=(
            "**q.seed.jobs** → crawl HTML → SeaweedFS → **q.parse.jobs**. "
            "Timeout 3h để cover trường hợp site chậm."
        ),
        execution_timeout=timedelta(hours=3),
    )

    parse = PythonOperator(
        task_id="parse",
        python_callable=task_parse,
        doc_md=(
            "**q.parse.jobs** → fetch SeaweedFS → BeautifulSoup → **q.load.jobs**."
        ),
        execution_timeout=timedelta(hours=1),
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
        doc_md=(
            "**q.load.jobs** → upsert dim_company / dim_location / "
            "dim_category / dim_date → INSERT **fact_jobs**."
        ),
        execution_timeout=timedelta(minutes=30),
    )

    report = PythonOperator(
        task_id="report",
        python_callable=task_report,
        trigger_rule="all_done",   # luôn chạy để có summary dù upstream fail
        doc_md=(
            "Cập nhật **pipeline_runs** (status, finished_at, counts). "
            "In summary log. Cảnh báo drop rate > 20%."
        ),
    )

    # Dependency chain
    init >> seed >> scrape >> parse >> load >> report