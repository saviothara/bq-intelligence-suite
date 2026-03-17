"""
Seed 15 more intentionally inefficient BigQuery queries (anti-patterns 21-35).
Covers patterns not in the original seed_bad_queries.py.

Project: tharalab1-lab-590b81
Tables : sales.transactions, sales.customers, sales.products
         analytics.events, analytics.sessions
         hr.employees
"""
import time
import sys
import io
from google.cloud import bigquery

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

P = "tharalab1-lab-590b81"
client = bigquery.Client(project=P)

BAD_QUERIES = [

    # 21. Wildcard table scan across all sales_ prefixed tables
    (
        "21. Wildcard scan across sales_* tables pulls all historical partitions",
        f"""
        SELECT transaction_id, amount, status, transaction_date
        FROM `{P}.sales.transactions`
        WHERE status = 'refunded'
        ORDER BY amount DESC
        LIMIT 500
        """
    ),

    # 22. ARRAY_AGG without LIMIT on full table
    (
        "22. ARRAY_AGG with no LIMIT materialises all page_urls per user in memory",
        f"""
        SELECT
          user_id,
          ARRAY_AGG(page_url)        AS all_pages,
          ARRAY_AGG(event_type)      AS all_events,
          COUNT(*)                   AS event_count
        FROM `{P}.analytics.events`
        GROUP BY user_id
        ORDER BY event_count DESC
        LIMIT 100
        """
    ),

    # 23. STRING_AGG over millions of rows
    (
        "23. STRING_AGG concatenates every event_type per country - huge shuffle",
        f"""
        SELECT
          country,
          STRING_AGG(DISTINCT event_type, ', ' ORDER BY event_type) AS all_event_types,
          STRING_AGG(DISTINCT browser,    ', ' ORDER BY browser)    AS all_browsers,
          COUNT(*) AS total_events
        FROM `{P}.analytics.events`
        GROUP BY country
        ORDER BY total_events DESC
        """
    ),

    # 24. CAST on JOIN key forces full type coercion on both sides
    (
        "24. CAST(customer_id AS STRING) on JOIN key prevents hash-join optimisation",
        f"""
        SELECT
          CAST(t.customer_id AS STRING) AS cid,
          c.full_name,
          c.loyalty_tier,
          SUM(t.amount)  AS revenue,
          COUNT(*)       AS orders
        FROM `{P}.sales.transactions` t
        JOIN `{P}.sales.customers` c
          ON CAST(t.customer_id AS STRING) = CAST(c.customer_id AS STRING)
        GROUP BY 1, 2, 3
        ORDER BY revenue DESC
        LIMIT 200
        """
    ),

    # 25. Subquery instead of window function for running total
    (
        "25. Correlated subquery for running revenue total instead of window SUM()",
        f"""
        SELECT
          t1.transaction_date,
          t1.transaction_id,
          t1.amount,
          (
            SELECT SUM(t2.amount)
            FROM `{P}.sales.transactions` t2
            WHERE t2.transaction_date <= t1.transaction_date
          ) AS running_total
        FROM `{P}.sales.transactions` t1
        ORDER BY t1.transaction_date
        LIMIT 100
        """
    ),

    # 26. REGEXP_CONTAINS on every row of large table
    (
        "26. REGEXP_CONTAINS on page_url scans all 500k event rows",
        f"""
        SELECT
          user_id,
          page_url,
          event_date,
          country
        FROM `{P}.analytics.events`
        WHERE REGEXP_CONTAINS(page_url, r'/page/[1-9][0-9]{2,}')
          AND REGEXP_CONTAINS(user_id,  r'^user_[0-9]+$')
        ORDER BY event_date DESC
        LIMIT 1000
        """
    ),

    # 27. COUNT(*) with no filter to get table size (use TABLE_STORAGE instead)
    (
        "27. COUNT(*) full table scan just to count rows - use metadata instead",
        f"""
        SELECT
          'transactions' AS tbl, COUNT(*) AS row_count FROM `{P}.sales.transactions`
        UNION ALL
        SELECT 'events',      COUNT(*) FROM `{P}.analytics.events`
        UNION ALL
        SELECT 'sessions',    COUNT(*) FROM `{P}.analytics.sessions`
        UNION ALL
        SELECT 'customers',   COUNT(*) FROM `{P}.sales.customers`
        UNION ALL
        SELECT 'employees',   COUNT(*) FROM `{P}.hr.employees`
        """
    ),

    # 28. GROUP BY on high-cardinality STRING column with no filter
    (
        "28. GROUP BY page_url (500+ distinct values) over full events table",
        f"""
        SELECT
          page_url,
          COUNT(*)                          AS hits,
          COUNT(DISTINCT user_id)           AS unique_users,
          AVG(session_duration_sec)         AS avg_duration,
          COUNTIF(event_type = 'purchase')  AS purchases
        FROM `{P}.analytics.events`
        GROUP BY page_url
        ORDER BY hits DESC
        LIMIT 50
        """
    ),

    # 29. Implicit cross-join from comma-separated FROM
    (
        "29. Implicit CROSS JOIN via comma in FROM (employees x departments lookup)",
        f"""
        SELECT
          e.full_name,
          e.department,
          e.salary,
          e.hire_date,
          e.office_city,
          d.dept_head
        FROM `{P}.hr.employees` e,
        (
          SELECT department, MAX(full_name) AS dept_head
          FROM `{P}.hr.employees`
          GROUP BY department
        ) d
        WHERE e.department = d.department
        ORDER BY e.salary DESC
        LIMIT 200
        """
    ),

    # 30. Unnecessary DISTINCT on a primary-key column
    (
        "30. SELECT DISTINCT transaction_id on transactions - PK is already unique",
        f"""
        SELECT DISTINCT
          transaction_id,
          customer_id,
          product_id,
          amount,
          status,
          payment_method,
          transaction_date
        FROM `{P}.sales.transactions`
        WHERE payment_method IN ('credit_card', 'paypal')
        ORDER BY amount DESC
        LIMIT 500
        """
    ),

    # 31. HAVING clause without WHERE - full scan then filter
    (
        "31. HAVING without WHERE filter means full scan before aggregation",
        f"""
        SELECT
          customer_id,
          COUNT(*)       AS order_count,
          SUM(amount)    AS total_spent,
          AVG(amount)    AS avg_order,
          MAX(amount)    AS largest_order
        FROM `{P}.sales.transactions`
        GROUP BY customer_id
        HAVING SUM(amount) > 5000
           AND COUNT(*) > 3
        ORDER BY total_spent DESC
        LIMIT 200
        """
    ),

    # 32. Multiple COUNT(DISTINCT) forcing multiple passes
    (
        "32. Five COUNT(DISTINCT) expressions each trigger a separate HyperLogLog pass",
        f"""
        SELECT
          event_date,
          COUNT(DISTINCT user_id)           AS dau,
          COUNT(DISTINCT session_duration_sec) AS unique_durations,
          COUNT(DISTINCT page_url)          AS unique_pages,
          COUNT(DISTINCT country)           AS unique_countries,
          COUNT(DISTINCT browser)           AS unique_browsers,
          COUNT(*)                          AS total_events
        FROM `{P}.analytics.events`
        GROUP BY event_date
        ORDER BY event_date DESC
        """
    ),

    # 33. Large IN list instead of JOIN or temp table
    (
        "33. IN with 200-element literal list forces sequential scan on every row",
        f"""
        SELECT
          customer_id,
          full_name,
          country,
          loyalty_tier,
          lifetime_value
        FROM `{P}.sales.customers`
        WHERE customer_id IN (
          {', '.join(str(i) for i in range(1, 201))}
        )
        ORDER BY lifetime_value DESC
        """
    ),

    # 34. Chained LEFT JOINs with no column pruning
    (
        "34. Five-table LEFT JOIN with SELECT * brings back all columns",
        f"""
        SELECT *
        FROM `{P}.sales.transactions`    t
        LEFT JOIN `{P}.sales.customers`  c ON t.customer_id = c.customer_id
        LEFT JOIN `{P}.sales.products`   p ON t.product_id  = p.product_id
        LEFT JOIN `{P}.analytics.events` e ON t.customer_id = SAFE_CAST(REPLACE(e.user_id, 'user_', '') AS INT64)
        LEFT JOIN `{P}.hr.employees`     h ON MOD(t.customer_id, 2000) + 1 = h.employee_id
        WHERE t.status = 'completed'
        ORDER BY t.amount DESC
        LIMIT 100
        """
    ),

    # 35. Using MAX/MIN via full scan instead of ORDER BY LIMIT 1
    (
        "35. Twelve aggregations computed in subqueries instead of a single pass",
        f"""
        SELECT
          (SELECT MAX(amount)        FROM `{P}.sales.transactions`) AS max_amount,
          (SELECT MIN(amount)        FROM `{P}.sales.transactions`) AS min_amount,
          (SELECT AVG(amount)        FROM `{P}.sales.transactions`) AS avg_amount,
          (SELECT SUM(amount)        FROM `{P}.sales.transactions`) AS total_amount,
          (SELECT COUNT(*)           FROM `{P}.sales.transactions`) AS txn_count,
          (SELECT COUNT(DISTINCT customer_id) FROM `{P}.sales.transactions`) AS unique_customers,
          (SELECT MAX(event_date)    FROM `{P}.analytics.events`)  AS latest_event,
          (SELECT MIN(event_date)    FROM `{P}.analytics.events`)  AS earliest_event,
          (SELECT COUNT(DISTINCT user_id) FROM `{P}.analytics.events`) AS unique_users,
          (SELECT AVG(salary)        FROM `{P}.hr.employees`)      AS avg_salary,
          (SELECT MAX(salary)        FROM `{P}.hr.employees`)      AS max_salary,
          (SELECT COUNT(*)           FROM `{P}.hr.employees`)      AS headcount
        """
    ),

]

total = len(BAD_QUERIES)
print(f"Seeding {total} additional bad queries (patterns 21-35)...\n")

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
    time.sleep(1)

print(f"\nDone. {passed} succeeded, {failed} failed.")
print("These queries now appear in INFORMATION_SCHEMA.JOBS for the optimizer to find.")
