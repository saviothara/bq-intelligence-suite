"""Run the 5 seeded inefficient queries so they appear in INFORMATION_SCHEMA.JOBS."""
import time
import sys
import io
from google.cloud import bigquery

# Force UTF-8 output on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PROJECT = "tharalab1-lab-590b81"
client  = bigquery.Client(project=PROJECT)

inefficient_queries = [
    (
        "SELECT * with no filters on large partitioned table",
        f"""
        SELECT *
        FROM `{PROJECT}.sales.transactions` t
        JOIN `{PROJECT}.sales.customers` c ON t.customer_id = c.customer_id
        JOIN `{PROJECT}.sales.products`  p ON t.product_id  = p.product_id
        WHERE t.status = 'completed'
        ORDER BY t.amount DESC
        LIMIT 1000
        """
    ),
    (
        "SELECT * events with LIKE filter and no partition pruning",
        f"""
        SELECT *
        FROM `{PROJECT}.analytics.events`
        WHERE LOWER(page_url) LIKE '%page%'
          AND event_type IN ('page_view', 'click', 'purchase')
        ORDER BY event_date DESC
        LIMIT 500
        """
    ),
    (
        "Correlated subquery for per-customer totals",
        f"""
        SELECT
          c.customer_id,
          c.full_name,
          c.country,
          c.loyalty_tier,
          (
            SELECT SUM(t.amount)
            FROM `{PROJECT}.sales.transactions` t
            WHERE t.customer_id = c.customer_id
          ) AS total_spent,
          (
            SELECT COUNT(*)
            FROM `{PROJECT}.sales.transactions` t
            WHERE t.customer_id = c.customer_id
          ) AS total_orders
        FROM `{PROJECT}.sales.customers` c
        ORDER BY total_spent DESC
        LIMIT 100
        """
    ),
    (
        "JOIN events x sessions with no date partition filter",
        f"""
        SELECT
          e.user_id,
          e.event_type,
          e.country,
          s.traffic_source,
          s.pages_viewed,
          COUNT(*) AS combo_count
        FROM `{PROJECT}.analytics.events`   e
        JOIN `{PROJECT}.analytics.sessions` s USING (user_id)
        GROUP BY 1,2,3,4,5
        ORDER BY combo_count DESC
        LIMIT 200
        """
    ),
    (
        "Repeated subquery aggregation without CTE on employees",
        f"""
        SELECT
          department,
          AVG(salary)  AS avg_salary,
          MAX(salary)  AS max_salary,
          MIN(salary)  AS min_salary,
          COUNT(*)     AS headcount,
          SUM(salary)  AS total_payroll
        FROM `{PROJECT}.hr.employees`
        WHERE salary > (
          SELECT AVG(salary) * 0.5 FROM `{PROJECT}.hr.employees`
        )
        GROUP BY department
        ORDER BY total_payroll DESC
        """
    ),
]

print("Running 5 inefficient queries...")
for i, (label, sql) in enumerate(inefficient_queries, 1):
    print(f"\n[{i}/5] {label}")
    try:
        job = client.query(sql.strip())
        job.result()
        print(f"      job_id: {job.job_id}")
        print(f"      bytes : {job.total_bytes_processed:,}")
    except Exception as exc:
        print(f"      ERROR: {exc}")
    time.sleep(2)

print("\nDone. Wait ~1 minute then run: python main.py")
