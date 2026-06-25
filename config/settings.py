# config/settings.py
# Config chung cho toàn bộ pipeline
# Sync với docker-compose.yml (Airflow 3.2.2 + CeleryExecutor)
import os

# ── RabbitMQ ─────────────────────────────────────────────────
# Credentials: admin/admin (theo docker-compose)
RABBITMQ = {
    "host":     os.getenv("RABBITMQ_HOST",     "localhost"),
    "port":     int(os.getenv("RABBITMQ_PORT", "5672")),
    "user":     os.getenv("RABBITMQ_USER",     "admin"),
    "password": os.getenv("RABBITMQ_PASSWORD", "admin"),
    "vhost":    "/",
}

QUEUES = {
    "seed":  "q.seed.jobs",
    "parse": "q.parse.jobs",
    "load":  "q.load.jobs",
    "dlq":   "q.dead.letter",
}

# ── PostgreSQL pipeline data ──────────────────────────────────

POSTGRES = {
    "host": os.getenv("PIPELINE_PG_HOST", "localhost"),
    "port": int(os.getenv("PIPELINE_PG_PORT", "5432")),
    "database": os.getenv("PIPELINE_PG_DB", "vietnamworks"),
    "user": os.getenv("PIPELINE_PG_USER", "airflow"),
    "password": os.getenv("PIPELINE_PG_PASSWORD", "airflow"),
}

POSTGRES_DSN = (
    f"postgresql://{POSTGRES['user']}:{POSTGRES['password']}"
    f"@{POSTGRES['host']}:{POSTGRES['port']}/{POSTGRES['database']}"

# ── SeaweedFS ─────────────────────────────────────────────────
# command: server -dir=/data -volume.max=5  → master + volume cùng 1 container
# Master: port 9333 | Volume: port 8080 (host: 8088)
SEAWEEDFS = {
    "master_url": os.getenv("SEAWEEDFS_MASTER_URL", "http://localhost:9333"),
    # Trong Docker network dùng port 8080; từ host dùng 8088
    "volume_url": os.getenv("SEAWEEDFS_VOLUME_URL", "http://localhost:8088"),
}

# ── ElasticSearch ─────────────────────────────────────────────
ELASTICSEARCH_URL   = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH_INDEX = "vietnamworks_jobs"

# ── Qdrant ────────────────────────────────────────────────────
QDRANT_URL        = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_COLLECTION = "vw_jobs"

# ── VietnamWorks scraping ─────────────────────────────────────
VIETNAMWORKS = {
    "base_url":   "https://www.vietnamworks.com",
    "api_search": "https://ms.vietnamworks.com/job-search/v1.0/jobs",
    "headers": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json",
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        "Referer":         "https://www.vietnamworks.com/",
    },
    "delay_between_requests": 2,
    "max_retries": 3,
}

# ── Pipeline ──────────────────────────────────────────────────
PIPELINE = {
    "batch_size":      10,
    "test_mode":       True,
    "test_job_limit":  1,
}
