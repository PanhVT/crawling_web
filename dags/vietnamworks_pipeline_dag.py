"""
AIRFLOW DAG - VietnamWorks Job Pipeline
=======================================
Airflow 3.2.2 + CeleryExecutor (theo docker-compose.yml)

Orchestrate 6 phases:
  Phase 1: Seeder      → push URLs vào RabbitMQ (admin/admin)
  Phase 2: Scraper     → crawl HTML → SeaweedFS (9333/8088) + parse queue
  Phase 3: Parser      → parse HTML → load queue
  Phase 4: DbLoader    → load vào airflow:5432/vietnamworks (star schema)
  Phase 5: Cleanup     → xóa raw files khỏi SeaweedFS
  Phase 6: Storage     → sync sang Qdrant:6333 + ElasticSearch:9200

DAG ID   : vietnamworks_job_pipeline
Schedule : @daily (02:00 UTC)
"""

from __future__ import annotations

import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Plugins mount: ./plugins → /opt/airflow/plugins
sys.path.insert(0, "/opt/airflow/plugins")
sys.path.insert(0, "/opt/airflow/dags")

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "execution_timeout":timedelta(minutes=30),
}


# ── Task callables ────────────────────────────────────────────

def task_seeder(**context) -> int:
    from phase1_seeder.seeder import seed_jobs_to_queue
    run_id  = context["run_id"]
    keyword = "data engineer"
    if context.get("dag_run") and context["dag_run"].conf:
        keyword = context["dag_run"].conf.get("keyword", keyword)
    log.info(f"[SEEDER] run_id={run_id} keyword='{keyword}'")
    count = seed_jobs_to_queue(run_id=run_id, keyword=keyword)
    context["ti"].xcom_push(key="seeded_count",    value=count)
    context["ti"].xcom_push(key="pipeline_run_id", value=run_id)
    return count


def task_scraper(**context) -> int:
    from phase2_scraper.scraper import run_scraper
    seeded = context["ti"].xcom_pull(task_ids="phase1_seeder", key="seeded_count") or 1
    count  = run_scraper(max_messages=seeded)
    context["ti"].xcom_push(key="scraped_count", value=count)
    return count


def task_parser(**context) -> int:
    from phase3_parser.parser import run_parser
    scraped = context["ti"].xcom_pull(task_ids="phase2_scraper", key="scraped_count") or 1
    count   = run_parser(max_messages=scraped)
    context["ti"].xcom_push(key="parsed_count", value=count)
    return count


def task_dbloader(**context) -> int:
    from phase4_dbloader.dbloader import run_dbloader
    parsed = context["ti"].xcom_pull(task_ids="phase3_parser", key="parsed_count") or 1
    count  = run_dbloader(max_messages=parsed)
    context["ti"].xcom_push(key="loaded_count", value=count)
    return count


def task_cleanup(**context):
    from phase5_cleanup.cleanup import run_cleanup
    run_id = context["ti"].xcom_pull(task_ids="phase1_seeder", key="pipeline_run_id")
    return run_cleanup(run_id=run_id, older_than_hours=0)


def task_storage(**context):
    from phase6_storage.storage_dispatcher import run_storage_dispatcher
    run_id = context["ti"].xcom_pull(task_ids="phase1_seeder", key="pipeline_run_id")
    loaded = context["ti"].xcom_pull(task_ids="phase4_dbloader", key="loaded_count") or 100
    return run_storage_dispatcher(run_id=run_id, limit=loaded)


def task_report(**context):
    ti      = context["ti"]
    seeded  = ti.xcom_pull(task_ids="phase1_seeder",   key="seeded_count")  or 0
    scraped = ti.xcom_pull(task_ids="phase2_scraper",  key="scraped_count") or 0
    parsed  = ti.xcom_pull(task_ids="phase3_parser",   key="parsed_count")  or 0
    loaded  = ti.xcom_pull(task_ids="phase4_dbloader", key="loaded_count")  or 0
    run_id  = ti.xcom_pull(task_ids="phase1_seeder",   key="pipeline_run_id")

    report = f"""
╔══════════════════════════════════════════════════╗
║   VIETNAMWORKS PIPELINE — KẾT QUẢ              ║
╠══════════════════════════════════════════════════╣
║  Run ID   : {run_id}
║  Thời gian: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
╠══════════════════════════════════════════════════╣
║  Phase 1 Seeder  : {seeded:>5} jobs → RabbitMQ (admin/admin)
║  Phase 2 Scraper : {scraped:>5} jobs → SeaweedFS :9333/:8088
║  Phase 3 Parser  : {parsed:>5} jobs parsed
║  Phase 4 DbLoader: {loaded:>5} jobs → airflow:5432
║  Phase 5 Cleanup : SeaweedFS raw files cleaned
║  Phase 6 Storage : Qdrant:6333 + ES:9200 synced
╚══════════════════════════════════════════════════╝
    """
    log.info(report)
    print(report)
    return {"seeded": seeded, "scraped": scraped, "parsed": parsed, "loaded": loaded}


# ── DAG ───────────────────────────────────────────────────────

with DAG(
    dag_id="vietnamworks_job_pipeline",
    description="Crawl VietnamWorks → PostgreSQL + Qdrant + ElasticSearch",
    default_args=DEFAULT_ARGS,
    schedule="0 2 * * *",      # Airflow 3.x dùng 'schedule' thay 'schedule_interval'
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vietnamworks", "data-pipeline", "crawl"],
    params={"keyword": "data engineer"},
    doc_md="""
## VietnamWorks Job Pipeline

**Infrastructure (docker-compose.yml):**

| Service | Container | Port |
|---------|-----------|------|
| Airflow UI | airflow-apiserver | :8080 |
| PostgreSQL (Airflow) | airflow-postgres | :5432 |
| Redis | airflow-redis | :6379 |
| RabbitMQ | rabbitmq | :5672 / :15672 |
| SeaweedFS | seaweedfs | :9333 / :8088 |
| ElasticSearch | elasticsearch | :9200 |
| Qdrant | qdrant | :6333 |
| PgAdmin | pgadmin | :5050 |

**Trigger với keyword khác:**
```json
{"keyword": "backend developer"}
```
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    phase1 = PythonOperator(
        task_id="phase1_seeder",
        python_callable=task_seeder,
    )
    phase2 = PythonOperator(
        task_id="phase2_scraper",
        python_callable=task_scraper,
    )
    phase3 = PythonOperator(
        task_id="phase3_parser",
        python_callable=task_parser,
    )
    phase4 = PythonOperator(
        task_id="phase4_dbloader",
        python_callable=task_dbloader,
    )
    # Phase 5 + 6 chạy song song sau Phase 4
    phase5 = PythonOperator(
        task_id="phase5_cleanup",
        python_callable=task_cleanup,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    phase6 = PythonOperator(
        task_id="phase6_storage",
        python_callable=task_storage,
    )
    report = PythonOperator(
        task_id="pipeline_report",
        python_callable=task_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # start → p1 → p2 → p3 → p4 → [p5, p6] → report → end
    start >> phase1 >> phase2 >> phase3 >> phase4
    phase4 >> [phase5, phase6]
    [phase5, phase6] >> report >> end
