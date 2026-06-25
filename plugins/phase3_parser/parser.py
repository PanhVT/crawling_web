"""
PHASE 3 - PARSER
================
Nhiệm vụ:
  1. Consume message từ q.parse.jobs
  2. Lấy raw HTML từ SeaweedFS (theo raw_file_id)
  3. Parse HTML → structured dict (job data)
  4. Đẩy structured data vào q.load.jobs cho DbLoader

Flow: [q.parse.jobs] → Parser → [q.load.jobs]
"""

import sys, os, json, re, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import pika
from bs4 import BeautifulSoup
from datetime import datetime
from config.settings import RABBITMQ, QUEUES, SEAWEEDFS, PIPELINE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PARSER] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


# ── SeaweedFS: đọc file ───────────────────────────────────────

def fetch_from_seaweedfs(file_id: str) -> bytes:
    """Lấy raw HTML từ SeaweedFS theo file_id."""

    # Local fallback (khi SeaweedFS chưa chạy)
    if file_id.startswith("local:"):
        local_path = file_id.replace("local:", "")
        with open(local_path, "rb") as f:
            return f.read()

    try:
        # Lookup volume URL từ master
        lookup = requests.get(
            f"{SEAWEEDFS['master_url']}/dir/lookup?volumeId={file_id.split(',')[0]}",
            timeout=10
        )
        lookup.raise_for_status()
        vol_location = lookup.json()["locations"][0]["publicUrl"]
        file_url = f"http://{vol_location}/{file_id}"
        resp = requests.get(file_url, timeout=20)
        resp.raise_for_status()
        return resp.content

    except Exception as e:
        log.error(f"Không lấy được file từ SeaweedFS (fid={file_id}): {e}")
        raise


# ── HTML Parser ───────────────────────────────────────────────

def parse_salary(salary_text: str) -> dict:
    """
    Parse salary string → {min, max, currency, negotiable}
    VD: '25,000,000 - 40,000,000 VND' → {min: 25000000, max: 40000000, ...}
    """
    if not salary_text:
        return {"min": None, "max": None, "currency": "VND", "negotiable": True}

    salary_text = salary_text.strip()

    if any(kw in salary_text.lower() for kw in ["thỏa thuận", "negotiable", "thoả thuận"]):
        return {"min": None, "max": None, "currency": "VND", "negotiable": True}

    # Tìm các con số
    numbers = re.findall(r'[\d,]+', salary_text)
    nums = [int(n.replace(",", "")) for n in numbers if n.replace(",", "").isdigit()]

    currency = "USD" if "usd" in salary_text.lower() else "VND"

    if len(nums) >= 2:
        return {"min": min(nums), "max": max(nums), "currency": currency, "negotiable": False}
    elif len(nums) == 1:
        return {"min": nums[0], "max": nums[0], "currency": currency, "negotiable": False}

    return {"min": None, "max": None, "currency": "VND", "negotiable": True}


def parse_experience(text: str) -> int | None:
    """
    Parse '3 năm' hoặc '3 years' → 3
    """
    if not text:
        return None
    m = re.search(r'(\d+)', text)
    return int(m.group(1)) if m else None


def parse_skills(skills_text: str) -> list:
    """Parse 'Python, SQL, Spark' → ['Python', 'SQL', 'Spark']"""
    if not skills_text:
        return []
    return [s.strip() for s in re.split(r'[,;/]', skills_text) if s.strip()]


def parse_html(html: bytes, source_url: str = "") -> dict:
    """
    Parse VietnamWorks job detail page.

    Không phụ thuộc class hash của React/styled-components.
    """

    soup = BeautifulSoup(html, "html.parser")

    # --------------------------------------------------
    # Job title
    # --------------------------------------------------

    title_el = soup.find("h1", attrs={"name": "title"})

    job_title = (
        title_el.get_text(strip=True)
        if title_el
        else ""
    )

    # --------------------------------------------------
    # Salary
    # --------------------------------------------------

    salary_raw = ""

    salary_el = soup.find("span", attrs={"name": "label"})

    if salary_el:
        salary_raw = salary_el.get_text(strip=True)

    salary = parse_salary(salary_raw)

    # --------------------------------------------------
    # Location
    # --------------------------------------------------

    city = ""

    location_keywords = [
        "Hà Nội",
        "Hồ Chí Minh",
        "Đà Nẵng",
        "Hải Phòng",
        "Cần Thơ",
        "Bình Dương",
        "Đồng Nai",
        "Long An",
        "Bắc Ninh",
        "Hưng Yên",
        "Quảng Ninh",
    ]

    for span in soup.find_all("span"):
        txt = span.get_text(" ", strip=True)

        if any(loc in txt for loc in location_keywords):
            city = txt
            break

    # --------------------------------------------------
    # Company
    # --------------------------------------------------

    company_name = ""

    company_candidates = soup.find_all(
        lambda tag:
        tag.name in ["div", "span", "a"]
        and tag.get_text(strip=True)
    )

    for el in company_candidates:

        txt = el.get_text(" ", strip=True)

        if (
            "công ty" in txt.lower()
            or "company" in txt.lower()
        ):
            company_name = txt[:300]
            break

    # --------------------------------------------------
    # Description
    # --------------------------------------------------

    description_parts = []

    keywords = [
        "mô tả công việc",
        "job description",
        "trách nhiệm",
        "yêu cầu",
        "requirements",
        "quyền lợi",
        "benefits",
    ]

    for tag in soup.find_all(["div", "section"]):

        text_content = tag.get_text("\n", strip=True)

        if not text_content:
            continue

        text_lower = text_content.lower()

        if any(k in text_lower for k in keywords):

            if len(text_content) > 100:
                description_parts.append(text_content)

    job_description = "\n\n".join(description_parts)

    if not job_description:
        body = soup.body

        if body:
            job_description = body.get_text(
                separator="\n",
                strip=True
            )[:10000]

    # --------------------------------------------------
    # Skills
    # --------------------------------------------------

    skills = []

    common_skills = [
        "Python",
        "SQL",
        "Spark",
        "Airflow",
        "Kafka",
        "dbt",
        "Docker",
        "Kubernetes",
        "AWS",
        "Azure",
        "GCP",
        "Java",
        "C#",
        "JavaScript",
        "TypeScript",
        "React",
        "NodeJS",
        "PostgreSQL",
        "MySQL",
        "Oracle",
    ]

    page_text = soup.get_text(" ", strip=True)

    for skill in common_skills:

        if re.search(
            rf"\b{re.escape(skill)}\b",
            page_text,
            re.IGNORECASE,
        ):
            skills.append(skill)

    skills = sorted(set(skills))

    # --------------------------------------------------
    # Experience
    # --------------------------------------------------

    experience_years = None

    exp_patterns = [
        r'(\d+)\+?\s*năm',
        r'(\d+)\+?\s*years',
        r'từ\s*(\d+)\s*năm',
    ]

    for pattern in exp_patterns:

        m = re.search(
            pattern,
            page_text,
            re.IGNORECASE,
        )

        if m:
            experience_years = int(m.group(1))
            break

    result = {
        # Job info
        "job_title": job_title,
        "job_level": None,
        "job_type": None,
        "category_name": None,
        "required_skills": skills,
        "experience_years": experience_years,
        "job_description": job_description,

        # Salary
        "salary_min": salary["min"],
        "salary_max": salary["max"],
        "salary_currency": salary["currency"],
        "salary_negotiable": salary["negotiable"],

        # Company
        "company_name": company_name,
        "company_size": None,
        "industry": None,

        # Location
        "city": city,

        # Dates
        "post_date": None,
        "deadline_date": None,

        # Raw
        "salary_raw": salary_raw,

        # Meta
        "source_url": source_url,
        "parsed_at": datetime.utcnow().isoformat(),
    }

    log.info(
        f"Parsed job='{job_title}' "
        f"company='{company_name}' "
        f"city='{city}' "
        f"skills={len(skills)}"
    )

    return result
def get_rabbitmq_channel():
    creds = pika.PlainCredentials(RABBITMQ["user"], RABBITMQ["password"])
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


def process_one_message(ch, method, properties, body: bytes):
    msg = json.loads(body)
    raw_file_id = msg.get("raw_file_id", "")
    job_url     = msg.get("job_url", "")
    job_id      = msg.get("job_id", "unknown")

    log.info(f"Parsing job_id={job_id} fid={raw_file_id}")

    try:
        # 1. Lấy HTML từ SeaweedFS
        html = fetch_from_seaweedfs(raw_file_id)

        # 2. Parse HTML → structured dict
        parsed = parse_html(html, source_url=job_url)

        # 3. Build load message
        load_msg = {
            **msg,         # forward toàn bộ fields từ Phase 2
            **parsed,      # thêm parsed data
            "phase": "parsed",
        }

        ch.basic_publish(
            exchange='',
            routing_key=QUEUES["load"],
            body=json.dumps(load_msg, ensure_ascii=False),
            properties=pika.BasicProperties(delivery_mode=2),
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info(f"✓ Parse xong job_id={job_id} → đẩy sang load queue")

    except Exception as e:
        log.error(f"✗ Parse lỗi job_id={job_id}: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def run_parser(max_messages: int = None):
    conn, ch = get_rabbitmq_channel()
    ch.basic_qos(prefetch_count=1)

    consumed = 0
    limit = max_messages or (1 if PIPELINE["test_mode"] else None)
    log.info(f"Parser lắng nghe '{QUEUES['parse']}' (limit={limit})")

    while True:
        method, props, body = ch.basic_get(queue=QUEUES["parse"], auto_ack=False)
        if body is None:
            log.info("Queue rỗng, parser dừng.")
            break

        process_one_message(ch, method, props, body)
        consumed += 1

        if limit and consumed >= limit:
            break

    conn.close()
    return consumed


# ── Entrypoint ────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== PHASE 3: PARSER BẮT ĐẦU ===")
    count = run_parser()
    log.info(f"=== PARSER XONG: {count} jobs ===")