"""
BigQuery Setup Script
Creates sample datasets, tables, loads data, and runs intentionally
inefficient queries so they appear in INFORMATION_SCHEMA.JOBS.

Project : tharalab1-lab-590b81
Region  : northamerica-northeast1
"""

import time
import random
from datetime import datetime, timedelta
from google.cloud import bigquery

PROJECT  = "tharalab1-lab-590b81"
LOCATION = "northamerica-northeast1"

client = bigquery.Client(project=PROJECT)


# ── Helpers ──────────────────────────────────────────────────────────────────

def run(label: str, sql: str) -> None:
    print(f"  Running: {label} ...", end=" ", flush=True)
    client.query(sql).result()
    print("done")


def create_dataset(dataset_id: str) -> None:
    ds = bigquery.Dataset(f"{PROJECT}.{dataset_id}")
    ds.location = LOCATION
    client.create_dataset(ds, exists_ok=True)
    print(f"[Dataset] {dataset_id} ready")


# ── 1. Create datasets ────────────────────────────────────────────────────────

print("\n=== Creating datasets ===")
create_dataset("sales")
create_dataset("analytics")
create_dataset("hr")


# ── 2. Create & populate tables ───────────────────────────────────────────────

print("\n=== Creating tables ===")

# ── sales.customers ──────────────────────────────────────────────────────────
run("DROP sales.customers", f"DROP TABLE IF EXISTS `{PROJECT}.sales.customers`")
run("CREATE sales.customers", f"""
CREATE TABLE `{PROJECT}.sales.customers` (
  customer_id   INT64,
  full_name     STRING,
  email         STRING,
  country       STRING,
  signup_date   DATE,
  loyalty_tier  STRING,
  lifetime_value FLOAT64
)
""")

customer_rows = ",\n".join([
    f"({i}, 'Customer {i}', 'customer{i}@example.com', "
    f"'{random.choice(['Canada','USA','UK','France','Germany'])}', "
    f"DATE '{(datetime(2020,1,1) + timedelta(days=random.randint(0,1460))).strftime('%Y-%m-%d')}', "
    f"'{random.choice(['Bronze','Silver','Gold','Platinum'])}', "
    f"{round(random.uniform(50, 50000), 2)})"
    for i in range(1, 5001)
])
run("INSERT sales.customers", f"""
INSERT INTO `{PROJECT}.sales.customers` VALUES
{customer_rows}
""")

# ── sales.products ───────────────────────────────────────────────────────────
run("DROP sales.products", f"DROP TABLE IF EXISTS `{PROJECT}.sales.products`")
run("CREATE sales.products", f"""
CREATE TABLE `{PROJECT}.sales.products` (
  product_id   INT64,
  product_name STRING,
  category     STRING,
  subcategory  STRING,
  unit_price   FLOAT64,
  cost         FLOAT64,
  supplier_id  INT64
)
""")

categories = {
    'Electronics': ['Phones','Laptops','Tablets','Accessories'],
    'Clothing':    ['Shirts','Pants','Shoes','Hats'],
    'Home':        ['Furniture','Decor','Kitchen','Bedding'],
    'Sports':      ['Outdoor','Fitness','Team Sports','Water Sports'],
}
product_rows = []
for i in range(1, 1001):
    cat = random.choice(list(categories.keys()))
    sub = random.choice(categories[cat])
    price = round(random.uniform(5, 2000), 2)
    product_rows.append(
        f"({i}, 'Product {i}', '{cat}', '{sub}', {price}, {round(price*0.6,2)}, {random.randint(1,50)})"
    )
run("INSERT sales.products", f"""
INSERT INTO `{PROJECT}.sales.products` VALUES
{", ".join(product_rows)}
""")

# ── sales.transactions (partitioned by date) ─────────────────────────────────
run("DROP sales.transactions", f"DROP TABLE IF EXISTS `{PROJECT}.sales.transactions`")
run("CREATE sales.transactions", f"""
CREATE TABLE `{PROJECT}.sales.transactions`
PARTITION BY transaction_date
CLUSTER BY customer_id, product_id
AS
WITH dates AS (
  SELECT DATE_ADD('2022-01-01', INTERVAL n DAY) AS d
  FROM UNNEST(GENERATE_ARRAY(0, 1095)) AS n      -- 3 years
),
base AS (
  SELECT
    ROW_NUMBER() OVER ()                             AS transaction_id,
    d                                                AS transaction_date,
    MOD(ABS(FARM_FINGERPRINT(CAST(n*17 AS STRING))), 5000) + 1  AS customer_id,
    MOD(ABS(FARM_FINGERPRINT(CAST(n*31 AS STRING))), 1000) + 1  AS product_id,
    MOD(ABS(FARM_FINGERPRINT(CAST(n*7  AS STRING))), 10)  + 1   AS quantity,
    ROUND(10 + RAND() * 1990, 2)                     AS amount,
    ['credit_card','debit_card','paypal','bank_transfer'][
      MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 4)]          AS payment_method,
    ['completed','completed','completed','pending','refunded'][
      MOD(ABS(FARM_FINGERPRINT(CAST(n*3 AS STRING))), 5)]        AS status
  FROM dates, UNNEST(GENERATE_ARRAY(1, 300)) AS n   -- 300 rows per day ≈ 328K rows
)
SELECT * FROM base
""")

# ── analytics.events (partitioned, large) ────────────────────────────────────
run("DROP analytics.events", f"DROP TABLE IF EXISTS `{PROJECT}.analytics.events`")
run("CREATE analytics.events", f"""
CREATE TABLE `{PROJECT}.analytics.events`
PARTITION BY event_date
CLUSTER BY user_id, event_type
AS
WITH base AS (
  SELECT
    ROW_NUMBER() OVER ()                                          AS event_id,
    DATE_ADD('2023-01-01', INTERVAL MOD(n, 730) DAY)             AS event_date,
    CONCAT('user_', CAST(MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 10000) + 1 AS STRING)) AS user_id,
    ['page_view','click','purchase','search','login','logout','add_to_cart','checkout'][
      MOD(ABS(FARM_FINGERPRINT(CAST(n*5 AS STRING))), 8)]        AS event_type,
    CONCAT('https://example.com/page/', CAST(MOD(n,500)+1 AS STRING)) AS page_url,
    ['Chrome','Firefox','Safari','Edge'][
      MOD(ABS(FARM_FINGERPRINT(CAST(n*11 AS STRING))), 4)]       AS browser,
    ['Desktop','Mobile','Tablet'][
      MOD(ABS(FARM_FINGERPRINT(CAST(n*13 AS STRING))), 3)]       AS device_type,
    ['Canada','USA','UK','France','Germany'][
      MOD(ABS(FARM_FINGERPRINT(CAST(n*7 AS STRING))), 5)]        AS country,
    ROUND(RAND() * 300, 2)                                        AS session_duration_sec
  FROM UNNEST(GENERATE_ARRAY(1, 500000)) AS n
)
SELECT * FROM base
""")

# ── analytics.sessions ───────────────────────────────────────────────────────
run("DROP analytics.sessions", f"DROP TABLE IF EXISTS `{PROJECT}.analytics.sessions`")
run("CREATE analytics.sessions", f"""
CREATE TABLE `{PROJECT}.analytics.sessions`
PARTITION BY session_date
AS
SELECT
  ROW_NUMBER() OVER ()                                            AS session_id,
  CONCAT('user_', CAST(MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 10000)+1 AS STRING)) AS user_id,
  DATE_ADD('2023-01-01', INTERVAL MOD(n, 730) DAY)               AS session_date,
  TIMESTAMP_ADD(TIMESTAMP '2023-01-01 00:00:00',
    INTERVAL (n * 17) SECOND)                                     AS session_start,
  MOD(n, 1800) + 30                                               AS duration_seconds,
  ['organic','paid','social','email','direct'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*3 AS STRING))), 5)]           AS traffic_source,
  MOD(ABS(FARM_FINGERPRINT(CAST(n*19 AS STRING))), 20) + 1        AS pages_viewed
FROM UNNEST(GENERATE_ARRAY(1, 200000)) AS n
""")

# ── hr.employees ─────────────────────────────────────────────────────────────
run("DROP hr.employees", f"DROP TABLE IF EXISTS `{PROJECT}.hr.employees`")
run("CREATE hr.employees", f"""
CREATE TABLE `{PROJECT}.hr.employees` (
  employee_id   INT64,
  full_name     STRING,
  department    STRING,
  job_title     STRING,
  salary        FLOAT64,
  hire_date     DATE,
  manager_id    INT64,
  office_city   STRING
)
""")

depts = ['Engineering','Sales','Marketing','Finance','HR','Operations','Legal','Support']
emp_rows = ",\n".join([
    f"({i}, 'Employee {i}', '{random.choice(depts)}', 'Title {i}', "
    f"{round(random.uniform(40000, 200000), 2)}, "
    f"DATE '{(datetime(2015,1,1) + timedelta(days=random.randint(0,3000))).strftime('%Y-%m-%d')}', "
    f"{random.randint(1, min(i, 50))}, "
    f"'{random.choice(['Toronto','Montreal','Vancouver','Ottawa','Calgary'])}')"
    for i in range(1, 2001)
])
run("INSERT hr.employees", f"""
INSERT INTO `{PROJECT}.hr.employees` VALUES
{emp_rows}
""")


# ── 3. Run intentionally inefficient queries ──────────────────────────────────
# These will surface in INFORMATION_SCHEMA.JOBS for Claude to optimize.

print("\n=== Running inefficient queries (will appear in INFORMATION_SCHEMA.JOBS) ===")

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
        "CROSS JOIN events x sessions with no date partition filter",
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
        "Repeated aggregation without CTE on transactions",
        f"""
        SELECT
          department,
          AVG(salary)    AS avg_salary,
          MAX(salary)    AS max_salary,
          MIN(salary)    AS min_salary,
          COUNT(*)       AS headcount,
          SUM(salary)    AS total_payroll
        FROM `{PROJECT}.hr.employees`
        WHERE salary > (
          SELECT AVG(salary) * 0.5 FROM `{PROJECT}.hr.employees`
        )
        GROUP BY department
        ORDER BY total_payroll DESC
        """
    ),
]

for label, sql in inefficient_queries:
    print(f"  [{label}]")
    try:
        client.query(sql.strip()).result()
        print("   -> completed")
    except Exception as exc:
        print(f"   -> ERROR: {exc}")
    time.sleep(2)   # small gap so jobs are distinct in INFORMATION_SCHEMA

print("\n=== Setup complete ===")
print(f"Project  : {PROJECT}")
print(f"Location : {LOCATION}")
print("\nDatasets & tables created:")
print("  sales      → customers, products, transactions (partitioned)")
print("  analytics  → events (partitioned), sessions (partitioned)")
print("  hr         → employees")
print("\n5 inefficient queries have been run and will appear in INFORMATION_SCHEMA.JOBS.")
print("Wait ~1 minute, then run:  python main.py")
