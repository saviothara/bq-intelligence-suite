"""
Seed 20 intentionally inefficient BigQuery queries for optimizer testing.
Covers a wide range of anti-patterns so the optimizer has diverse material.

Project: tharalab1-lab-590b81
Tables : sales.transactions (partitioned), sales.customers, sales.products
         analytics.events (partitioned), analytics.sessions (partitioned)
         hr.employees
"""
import time
import sys
import io
from google.cloud import bigquery

# Force UTF-8 on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

P = "tharalab1-lab-590b81"
client = bigquery.Client(project=P)

BAD_QUERIES = [

    # ── 1. SELECT * + no partition filter ────────────────────────────────────
    (
        "1. SELECT * full scan on partitioned transactions (no date filter)",
        f"""
        SELECT *
        FROM `{P}.sales.transactions`
        ORDER BY amount DESC
        LIMIT 500
        """
    ),

    # ── 2. LOWER() on WHERE column breaks cluster pruning ────────────────────
    (
        "2. LOWER() on clustered event_type prevents cluster pruning",
        f"""
        SELECT *
        FROM `{P}.analytics.events`
        WHERE LOWER(event_type) = 'purchase'
        ORDER BY event_date DESC
        LIMIT 200
        """
    ),

    # ── 3. Leading wildcard LIKE on large column ──────────────────────────────
    (
        "3. Leading wildcard LIKE on page_url forces full scan",
        f"""
        SELECT user_id, page_url, event_date, country
        FROM `{P}.analytics.events`
        WHERE page_url LIKE '%checkout%'
        LIMIT 1000
        """
    ),

    # ── 4. CAST on partition column breaks partition pruning ──────────────────
    (
        "4. CAST(transaction_date AS STRING) prevents partition pruning",
        f"""
        SELECT transaction_id, amount, status
        FROM `{P}.sales.transactions`
        WHERE CAST(transaction_date AS STRING) >= '2024-01-01'
        ORDER BY amount DESC
        LIMIT 300
        """
    ),

    # ── 5. Correlated subquery (two passes per customer row) ─────────────────
    (
        "5. Correlated subquery scans transactions once per customer",
        f"""
        SELECT
          c.customer_id,
          c.full_name,
          c.loyalty_tier,
          (SELECT MAX(t.amount)
           FROM `{P}.sales.transactions` t
           WHERE t.customer_id = c.customer_id) AS max_order,
          (SELECT MIN(t.amount)
           FROM `{P}.sales.transactions` t
           WHERE t.customer_id = c.customer_id) AS min_order,
          (SELECT AVG(t.amount)
           FROM `{P}.sales.transactions` t
           WHERE t.customer_id = c.customer_id) AS avg_order
        FROM `{P}.sales.customers` c
        LIMIT 200
        """
    ),

    # ── 6. UNION DISTINCT instead of UNION ALL (forces dedup) ────────────────
    (
        "6. UNION DISTINCT (not UNION ALL) between two full table scans forces dedup",
        f"""
        SELECT user_id, 'event' AS source, event_date AS dt
        FROM `{P}.analytics.events`
        UNION DISTINCT
        SELECT user_id, 'session' AS source, session_date AS dt
        FROM `{P}.analytics.sessions`
        ORDER BY dt DESC
        LIMIT 500
        """
    ),

    # ── 7. CROSS JOIN (cartesian product) ────────────────────────────────────
    (
        "7. Accidental CROSS JOIN products x customers (5k x 1k = 5M rows)",
        f"""
        SELECT
          c.country,
          p.category,
          COUNT(*) AS combos
        FROM `{P}.sales.customers` c
        CROSS JOIN `{P}.sales.products` p
        GROUP BY 1, 2
        ORDER BY combos DESC
        LIMIT 100
        """
    ),

    # ── 8. COUNT(DISTINCT) on high-cardinality column ────────────────────────
    (
        "8. COUNT(DISTINCT page_url) over full events table",
        f"""
        SELECT
          event_type,
          country,
          COUNT(DISTINCT page_url)   AS unique_pages,
          COUNT(DISTINCT user_id)    AS unique_users,
          COUNT(DISTINCT session_duration_sec) AS unique_durations
        FROM `{P}.analytics.events`
        GROUP BY 1, 2
        ORDER BY unique_users DESC
        """
    ),

    # ── 9. Subquery in SELECT for each row (scalar subquery) ─────────────────
    (
        "9. Scalar subquery in SELECT computes avg salary per department per row",
        f"""
        SELECT
          e.employee_id,
          e.full_name,
          e.department,
          e.salary,
          (SELECT AVG(e2.salary)
           FROM `{P}.hr.employees` e2
           WHERE e2.department = e.department) AS dept_avg_salary,
          e.salary - (SELECT AVG(e2.salary)
                      FROM `{P}.hr.employees` e2
                      WHERE e2.department = e.department) AS delta_from_avg
        FROM `{P}.hr.employees` e
        ORDER BY delta_from_avg DESC
        """
    ),

    # ── 10. NOT IN with subquery (null-unsafe, full scans both sides) ─────────
    (
        "10. NOT IN subquery is null-unsafe and scans both tables fully",
        f"""
        SELECT *
        FROM `{P}.sales.customers`
        WHERE customer_id NOT IN (
          SELECT DISTINCT customer_id
          FROM `{P}.sales.transactions`
          WHERE status = 'completed'
        )
        """
    ),

    # ── 11. OR on different columns prevents partition/cluster pruning ────────
    (
        "11. OR across columns disables partition and cluster pruning",
        f"""
        SELECT *
        FROM `{P}.analytics.events`
        WHERE event_type = 'purchase'
           OR country = 'Canada'
           OR browser = 'Chrome'
        ORDER BY event_date DESC
        LIMIT 1000
        """
    ),

    # ── 12. Self-join without filter (full scan twice) ────────────────────────
    (
        "12. Self-join on transactions to find same-customer repeat orders",
        f"""
        SELECT
          a.customer_id,
          a.transaction_id AS order_1,
          b.transaction_id AS order_2,
          a.amount,
          b.amount AS amount_2
        FROM `{P}.sales.transactions` a
        JOIN `{P}.sales.transactions` b
          ON a.customer_id = b.customer_id
         AND a.transaction_id < b.transaction_id
        ORDER BY a.customer_id
        LIMIT 500
        """
    ),

    # ── 13. Window function over entire table with no PARTITION BY ────────────
    (
        "13. ROW_NUMBER() with no PARTITION BY forces global sort of events",
        f"""
        SELECT
          event_id,
          user_id,
          event_type,
          event_date,
          ROW_NUMBER() OVER (ORDER BY event_date DESC, event_id) AS global_rank
        FROM `{P}.analytics.events`
        QUALIFY global_rank <= 100
        """
    ),

    # ── 14. Nested subqueries instead of a single pass ────────────────────────
    (
        "14. Triple-nested subquery on transactions (three full scans)",
        f"""
        SELECT *
        FROM (
          SELECT *
          FROM (
            SELECT
              customer_id,
              SUM(amount) AS total
            FROM `{P}.sales.transactions`
            GROUP BY customer_id
          )
          WHERE total > 1000
        )
        WHERE customer_id IN (
          SELECT customer_id FROM `{P}.sales.customers`
          WHERE loyalty_tier = 'Gold'
        )
        ORDER BY total DESC
        LIMIT 100
        """
    ),

    # ── 15. SELECT * three-way join with no column pruning ────────────────────
    (
        "15. SELECT * three-way join pulls all columns across 3 large tables",
        f"""
        SELECT *
        FROM `{P}.sales.transactions` t
        JOIN `{P}.sales.customers`    c ON t.customer_id = c.customer_id
        JOIN `{P}.sales.products`     p ON t.product_id  = p.product_id
        WHERE t.payment_method = 'credit_card'
        ORDER BY t.amount DESC
        LIMIT 1000
        """
    ),

    # ── 16. Repeated aggregation subqueries without CTE ──────────────────────
    (
        "16. Same aggregation subquery repeated 4x without CTE",
        f"""
        SELECT
          department,
          headcount,
          total_payroll,
          total_payroll / headcount AS cost_per_head,
          total_payroll / (SELECT SUM(salary) FROM `{P}.hr.employees`) AS payroll_share
        FROM (
          SELECT
            department,
            COUNT(*)    AS headcount,
            SUM(salary) AS total_payroll
          FROM `{P}.hr.employees`
          WHERE hire_date >= (SELECT DATE_SUB(MAX(hire_date), INTERVAL 5 YEAR)
                              FROM `{P}.hr.employees`)
          GROUP BY department
        )
        ORDER BY payroll_share DESC
        """
    ),

    # ── 17. ORDER BY on non-clustered column of large table ──────────────────
    (
        "17. Full table ORDER BY session_duration_sec (not a cluster key)",
        f"""
        SELECT
          user_id,
          session_date,
          traffic_source,
          duration_seconds,
          pages_viewed
        FROM `{P}.analytics.sessions`
        ORDER BY duration_seconds DESC
        LIMIT 50
        """
    ),

    # ── 18. DISTINCT on SELECT * from large joined result ────────────────────
    (
        "18. SELECT DISTINCT * across events+sessions forces full dedup pass",
        f"""
        SELECT DISTINCT
          e.user_id,
          e.event_type,
          e.country,
          e.browser,
          s.traffic_source
        FROM `{P}.analytics.events`   e
        JOIN `{P}.analytics.sessions` s USING (user_id)
        ORDER BY e.user_id
        LIMIT 500
        """
    ),

    # ── 19. Subquery per row in WHERE (EXISTS anti-pattern) ──────────────────
    (
        "19. NOT EXISTS correlated subquery scans sessions per event row",
        f"""
        SELECT *
        FROM `{P}.analytics.events` e
        WHERE NOT EXISTS (
          SELECT 1
          FROM `{P}.analytics.sessions` s
          WHERE s.user_id = e.user_id
            AND s.session_date = e.event_date
        )
        ORDER BY event_date DESC
        LIMIT 200
        """
    ),

    # ── 20. FORMAT() on partition column breaks pruning ───────────────────────
    (
        "20. FORMAT() on transaction_date column breaks partition pruning",
        f"""
        SELECT
          FORMAT_DATE('%Y-%m', transaction_date) AS month,
          COUNT(*)                               AS txn_count,
          SUM(amount)                            AS revenue,
          AVG(amount)                            AS avg_order
        FROM `{P}.sales.transactions`
        WHERE FORMAT_DATE('%Y', transaction_date) = '2023'
        GROUP BY month
        ORDER BY month
        """
    ),

]

total = len(BAD_QUERIES)
print(f"Seeding {total} intentionally inefficient queries...\n")

passed = 0
failed = 0
for i, (label, sql) in enumerate(BAD_QUERIES, 1):
    print(f"[{i:2d}/{total}] {label}")
    try:
        job = client.query(sql.strip())
        job.result()
        gb = round((job.total_bytes_processed or 0) / (1024 ** 3), 4)
        print(f"         OK  | job: {job.job_id} | {gb} GB scanned")
        passed += 1
    except Exception as exc:
        print(f"         ERR | {exc}")
        failed += 1
    time.sleep(1)   # small gap so INFORMATION_SCHEMA sees distinct jobs

print(f"\nDone. {passed} succeeded, {failed} failed.")
print("Wait ~1 minute, then run:  python main.py")
