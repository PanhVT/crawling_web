"""
PHASE 4 - DB LOADER
===================
Consume q.load.jobs → upsert dim tables → insert fact_jobs → PostgreSQL
"""

import sys, os, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pika
import psycopg2
from psycopg2 import extras
from datetime import datetime
from dateutil import parser as date_parser

from config.settings import RABBITMQ, QUEUES, POSTGRES, PIPELINE

# ───────────────────────────────────────────────
# LOGGING
# ───────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DBLOADER] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


# ───────────────────────────────────────────────
# POSTGRES CONNECTION
# ───────────────────────────────────────────────

def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        dbname=POSTGRES["database"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
    )


# ───────────────────────────────────────────────
# DIM UPSERTS
# ───────────────────────────────────────────────

def upsert_company(cur, data):
    name = (data.get("company_name") or "").strip()
    if not name:
        return None

    slug = name.lower().replace(" ", "-").replace(",", "")[:200]

    cur.execute("""
        INSERT INTO dim_company (company_name, company_slug, industry, company_size)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (company_slug)
        DO UPDATE SET
            industry = COALESCE(EXCLUDED.industry, dim_company.industry),
            company_size = COALESCE(EXCLUDED.company_size, dim_company.company_size)
        RETURNING company_id
    """, (name, slug, data.get("industry"), data.get("company_size")))

    return cur.fetchone()[0]


def upsert_location(cur, data):
    city = (data.get("city") or "").strip()
    if not city:
        return None

    cur.execute("""
        INSERT INTO dim_location (city, country)
        VALUES (%s, 'Vietnam')
        ON CONFLICT (city)
        DO UPDATE SET country = EXCLUDED.country
        RETURNING location_id
    """, (city,))

    return cur.fetchone()[0]


def upsert_category(cur, data):
    cat = (data.get("category_name") or "").strip()
    if not cat:
        return None

    cur.execute("""
        INSERT INTO dim_category (category_name)
        VALUES (%s)
        ON CONFLICT (category_name)
        DO UPDATE SET category_name = EXCLUDED.category_name
        RETURNING category_id
    """, (cat,))

    return cur.fetchone()[0]


def upsert_date(cur, date_str):
    if not date_str:
        return None

    try:
        d = date_parser.parse(date_str).date()
    except Exception:
        return None

    cur.execute("""
        INSERT INTO dim_date (full_date, day, month, quarter, year)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (full_date)
        DO UPDATE SET full_date = EXCLUDED.full_date
        RETURNING date_id
    """, (d, d.day, d.month, (d.month - 1) // 3 + 1, d.year))

    return cur.fetchone()[0]


# ───────────────────────────────────────────────
# FACT INSERT (UPSERT IDEMPOTENT)
# ───────────────────────────────────────────────

def insert_fact_job(cur, data, company_id, location_id, category_id,
                    post_date_id, deadline_date_id):

    skills = data.get("required_skills") or []
    if isinstance(skills, str):
        skills = [s.strip() for s in skills.split(",") if s.strip()]

    cur.execute("""
        INSERT INTO fact_jobs (
            source_url, raw_file_id, scraped_at,
            company_id, location_id, category_id,
            post_date_id, deadline_date_id,
            job_title,
            salary_min, salary_max,
            salary_currency, salary_negotiable,
            experience_years, job_level, job_type,
            required_skills, job_description,
            pipeline_run_id
        )
        VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s,
            %s
        )
        ON CONFLICT (source_url)
        DO UPDATE SET
            scraped_at = EXCLUDED.scraped_at,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            job_description = EXCLUDED.job_description
        RETURNING job_id
    """, (
        data.get("source_url"),
        data.get("raw_file_id"),
        data.get("scraped_at"),

        company_id,
        location_id,
        category_id,

        post_date_id,
        deadline_date_id,

        data.get("job_title"),

        data.get("salary_min"),
        data.get("salary_max"),

        data.get("salary_currency", "VND"),
        data.get("salary_negotiable", False),

        data.get("experience_years"),
        data.get("job_level"),
        data.get("job_type"),

        skills,
        data.get("job_description"),

        data.get("run_id"),
    ))

    return cur.fetchone()[0]


# ───────────────────────────────────────────────
# MAIN LOAD FUNCTION
# ───────────────────────────────────────────────

def load_job(data):
    conn = get_pg_conn()

    try:
        with conn:
            with conn.cursor() as cur:

                company_id = upsert_company(cur, data)
                location_id = upsert_location(cur, data)
                category_id = upsert_category(cur, data)

                post_date_id = upsert_date(cur, data.get("post_date"))
                deadline_id = upsert_date(cur, data.get("deadline_date"))

                job_id = insert_fact_job(
                    cur, data,
                    company_id, location_id, category_id,
                    post_date_id, deadline_id
                )

                log.info(
                    f"Loaded job_id={job_id} | company={company_id} "
                    f"location={location_id} category={category_id}"
                )

        return job_id

    finally:
        conn.close()


# ───────────────────────────────────────────────
# RABBITMQ
# ───────────────────────────────────────────────

def get_channel():
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
        )
    )

    ch = conn.channel()

    for q in QUEUES.values():
        ch.queue_declare(queue=q, durable=True)

    return conn, ch


# ───────────────────────────────────────────────
# CONSUMER
# ───────────────────────────────────────────────

def process_message(ch, method, properties, body):
    data = json.loads(body)
    job_src_id = data.get("job_id")

    try:
        pg_id = load_job(data)

        ch.basic_ack(method.delivery_tag)

        log.info(f"OK: {job_src_id} → pg_job_id={pg_id}")

    except Exception as e:
        log.error(f"FAIL job={job_src_id}: {e}", exc_info=True)
        ch.basic_nack(method.delivery_tag, requeue=False)


def run():
    conn, ch = get_channel()
    ch.basic_qos(prefetch_count=1)

    limit = 1 if PIPELINE["test_mode"] else None
    count = 0

    log.info(f"DBLoader listening queue: {QUEUES['load']}")

    while True:
        method, props, body = ch.basic_get(queue=QUEUES["load"], auto_ack=False)

        if not body:
            break

        process_message(ch, method, props, body)
        count += 1

        if limit and count >= limit:
            break

    conn.close()
    return count


# ───────────────────────────────────────────────
# ENTRY
# ───────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== PHASE 4 DB LOADER START ===")
    n = run()
    log.info(f"=== DONE: {n} jobs loaded ===")