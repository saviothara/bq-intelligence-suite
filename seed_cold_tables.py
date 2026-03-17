"""
Create tables that will appear as cold/unqueried in the Storage Advisor.

These tables are created with realistic data volumes but are NEVER queried
afterwards, so they surface as archive candidates in INFORMATION_SCHEMA.TABLE_STORAGE.

New datasets:
  archive  -> legacy_orders_2021, legacy_events_2020, deprecated_users_2022,
              old_product_catalog, historical_sessions_2021
  staging  -> temp_exports_q1_2023, raw_imports_jan2023, backup_transactions_2022,
              etl_staging_load
  finance  -> budget_actuals_2021, expense_reports_2020, forecast_archive_2022,
              payroll_history_2021
  logs     -> app_logs_2022, api_request_log_2021, audit_trail_2020,
              error_log_archive

Project: tharalab1-lab-590b81
"""
import sys
import io
import time
from google.cloud import bigquery

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

P = "tharalab1-lab-590b81"
LOC = "northamerica-northeast1"
client = bigquery.Client(project=P)


def create_dataset(dataset_id):
    ds = bigquery.Dataset(f"{P}.{dataset_id}")
    ds.location = LOC
    client.create_dataset(ds, exists_ok=True)
    print(f"  [dataset] {dataset_id} ready")


def run(label, sql):
    print(f"  Creating {label} ...", end=" ", flush=True)
    try:
        client.query(sql.strip()).result()
        print("done")
    except Exception as exc:
        print(f"ERROR: {exc}")


# ── Datasets ──────────────────────────────────────────────────────────────────
print("\n=== Creating datasets ===")
for ds in ["archive", "staging", "finance", "logs"]:
    create_dataset(ds)


# ── archive dataset ───────────────────────────────────────────────────────────
print("\n=== archive dataset ===")

run("archive.legacy_orders_2021", f"""
CREATE OR REPLACE TABLE `{P}.archive.legacy_orders_2021`
PARTITION BY order_date
AS
SELECT
  ROW_NUMBER() OVER ()                                              AS order_id,
  DATE_ADD('2021-01-01', INTERVAL MOD(n, 365) DAY)                AS order_date,
  MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 5000) + 1         AS customer_id,
  MOD(ABS(FARM_FINGERPRINT(CAST(n*7 AS STRING))), 1000) + 1       AS product_id,
  ROUND(10 + RAND() * 990, 2)                                      AS order_total,
  ['completed','refunded','cancelled'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*3 AS STRING))), 3)]            AS status,
  ['credit_card','paypal','bank_transfer'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*11 AS STRING))), 3)]           AS payment_method,
  ['Canada','USA','UK','France','Germany'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*5 AS STRING))), 5)]            AS ship_country
FROM UNNEST(GENERATE_ARRAY(1, 150000)) AS n
""")

run("archive.legacy_events_2020", f"""
CREATE OR REPLACE TABLE `{P}.archive.legacy_events_2020`
PARTITION BY event_date
AS
SELECT
  ROW_NUMBER() OVER ()                                              AS event_id,
  DATE_ADD('2020-01-01', INTERVAL MOD(n, 366) DAY)                AS event_date,
  CONCAT('user_', CAST(MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 8000)+1 AS STRING)) AS user_id,
  ['page_view','click','purchase','search','login'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*5 AS STRING))), 5)]            AS event_type,
  CONCAT('https://legacy.example.com/page/', CAST(MOD(n,200)+1 AS STRING)) AS page_url,
  ['Chrome','Firefox','Safari'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*13 AS STRING))), 3)]           AS browser,
  ['Canada','USA','UK'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*7 AS STRING))), 3)]            AS country,
  ROUND(RAND() * 250, 2)                                           AS session_duration_sec
FROM UNNEST(GENERATE_ARRAY(1, 300000)) AS n
""")

run("archive.deprecated_users_2022", f"""
CREATE OR REPLACE TABLE `{P}.archive.deprecated_users_2022` (
  user_id        INT64,
  email          STRING,
  full_name      STRING,
  country        STRING,
  signup_date    DATE,
  last_login     DATE,
  account_status STRING,
  deactivated_at DATE
)
AS
SELECT
  n AS user_id,
  CONCAT('user', CAST(n AS STRING), '@old.example.com') AS email,
  CONCAT('Deleted User ', CAST(n AS STRING))             AS full_name,
  ['Canada','USA','UK','France'][MOD(n, 4)]              AS country,
  DATE_ADD('2019-01-01', INTERVAL MOD(n, 1000) DAY)     AS signup_date,
  DATE_ADD('2021-06-01', INTERVAL MOD(n, 400) DAY)      AS last_login,
  'DEACTIVATED'                                          AS account_status,
  DATE_ADD('2022-01-01', INTERVAL MOD(n, 365) DAY)      AS deactivated_at
FROM UNNEST(GENERATE_ARRAY(1, 25000)) AS n
""")

run("archive.old_product_catalog", f"""
CREATE OR REPLACE TABLE `{P}.archive.old_product_catalog` (
  product_id    INT64,
  sku           STRING,
  product_name  STRING,
  category      STRING,
  list_price    FLOAT64,
  cost          FLOAT64,
  discontinued  BOOL,
  last_sold     DATE
)
AS
SELECT
  n AS product_id,
  CONCAT('SKU-', LPAD(CAST(n AS STRING), 6, '0'))          AS sku,
  CONCAT('Discontinued Product ', CAST(n AS STRING))        AS product_name,
  ['Electronics','Clothing','Home','Sports'][MOD(n, 4)]     AS category,
  ROUND(5 + RAND() * 995, 2)                                AS list_price,
  ROUND(2 + RAND() * 400, 2)                                AS cost,
  TRUE                                                      AS discontinued,
  DATE_ADD('2020-01-01', INTERVAL MOD(n, 730) DAY)         AS last_sold
FROM UNNEST(GENERATE_ARRAY(1, 8000)) AS n
""")

run("archive.historical_sessions_2021", f"""
CREATE OR REPLACE TABLE `{P}.archive.historical_sessions_2021`
PARTITION BY session_date
AS
SELECT
  ROW_NUMBER() OVER ()                                              AS session_id,
  CONCAT('user_', CAST(MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 8000)+1 AS STRING)) AS user_id,
  DATE_ADD('2021-01-01', INTERVAL MOD(n, 365) DAY)                AS session_date,
  MOD(n, 3600) + 10                                                AS duration_seconds,
  ['organic','paid','social','email','direct'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*3 AS STRING))), 5)]            AS traffic_source,
  MOD(ABS(FARM_FINGERPRINT(CAST(n*19 AS STRING))), 15) + 1         AS pages_viewed,
  ['Chrome','Firefox','Safari','Edge'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*7 AS STRING))), 4)]            AS browser
FROM UNNEST(GENERATE_ARRAY(1, 120000)) AS n
""")


# ── staging dataset ───────────────────────────────────────────────────────────
print("\n=== staging dataset ===")

run("staging.temp_exports_q1_2023", f"""
CREATE OR REPLACE TABLE `{P}.staging.temp_exports_q1_2023` (
  export_id      INT64,
  export_date    DATE,
  table_name     STRING,
  row_count      INT64,
  file_path      STRING,
  status         STRING,
  created_by     STRING
)
AS
SELECT
  n AS export_id,
  DATE_ADD('2023-01-01', INTERVAL MOD(n, 90) DAY) AS export_date,
  ['sales.transactions','analytics.events','hr.employees'][MOD(n,3)] AS table_name,
  MOD(n, 100000) + 1000                             AS row_count,
  CONCAT('gs://exports-2023/q1/file_', CAST(n AS STRING), '.parquet') AS file_path,
  ['DONE','DONE','FAILED'][MOD(n, 3)]               AS status,
  CONCAT('etl_job_', CAST(MOD(n, 5)+1 AS STRING))  AS created_by
FROM UNNEST(GENERATE_ARRAY(1, 5000)) AS n
""")

run("staging.raw_imports_jan2023", f"""
CREATE OR REPLACE TABLE `{P}.staging.raw_imports_jan2023`
AS
SELECT
  ROW_NUMBER() OVER ()                                              AS import_id,
  DATE_ADD('2023-01-01', INTERVAL MOD(n, 31) DAY)                 AS import_date,
  CONCAT('record_', CAST(n AS STRING))                             AS raw_key,
  CONCAT('{{"id":', CAST(n AS STRING), ',"val":', CAST(ROUND(RAND()*1000,2) AS STRING), '}}') AS raw_payload,
  ['PROCESSED','PENDING','ERROR'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 3)]              AS processing_status,
  TIMESTAMP_ADD(TIMESTAMP '2023-01-01 08:00:00', INTERVAL n SECOND) AS ingested_at
FROM UNNEST(GENERATE_ARRAY(1, 80000)) AS n
""")

run("staging.backup_transactions_2022", f"""
CREATE OR REPLACE TABLE `{P}.staging.backup_transactions_2022`
PARTITION BY transaction_date
AS
SELECT
  ROW_NUMBER() OVER ()                                              AS transaction_id,
  DATE_ADD('2022-01-01', INTERVAL MOD(n, 365) DAY)                AS transaction_date,
  MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 5000) + 1         AS customer_id,
  MOD(ABS(FARM_FINGERPRINT(CAST(n*7 AS STRING))), 1000) + 1       AS product_id,
  ROUND(10 + RAND() * 1990, 2)                                     AS amount,
  ['completed','pending','refunded'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*3 AS STRING))), 3)]            AS status,
  'BACKUP'                                                         AS source_tag
FROM UNNEST(GENERATE_ARRAY(1, 200000)) AS n
""")

run("staging.etl_staging_load", f"""
CREATE OR REPLACE TABLE `{P}.staging.etl_staging_load` (
  batch_id       INT64,
  load_timestamp TIMESTAMP,
  source_system  STRING,
  record_count   INT64,
  error_count    INT64,
  duration_ms    INT64,
  operator       STRING,
  notes          STRING
)
AS
SELECT
  n AS batch_id,
  TIMESTAMP_ADD(TIMESTAMP '2022-06-01 00:00:00', INTERVAL n * 3600 SECOND) AS load_timestamp,
  ['CRM','ERP','POS','API','Manual'][MOD(n, 5)]            AS source_system,
  MOD(n * 997, 50000) + 100                                AS record_count,
  MOD(n, 20)                                               AS error_count,
  MOD(n * 113, 30000) + 500                                AS duration_ms,
  CONCAT('operator_', CAST(MOD(n, 8)+1 AS STRING))         AS operator,
  IF(MOD(n, 10) = 0, 'Retry after timeout', NULL)          AS notes
FROM UNNEST(GENERATE_ARRAY(1, 3000)) AS n
""")


# ── finance dataset ───────────────────────────────────────────────────────────
print("\n=== finance dataset ===")

run("finance.budget_actuals_2021", f"""
CREATE OR REPLACE TABLE `{P}.finance.budget_actuals_2021` (
  period        STRING,
  department    STRING,
  cost_centre   STRING,
  budget_usd    FLOAT64,
  actual_usd    FLOAT64,
  variance_usd  FLOAT64,
  category      STRING,
  approved_by   STRING
)
AS
SELECT
  CONCAT('2021-', LPAD(CAST(MOD(n,12)+1 AS STRING), 2, '0'))       AS period,
  ['Engineering','Sales','Marketing','Finance','HR','Ops'][MOD(n,6)] AS department,
  CONCAT('CC-', CAST(MOD(n,50)+100 AS STRING))                      AS cost_centre,
  ROUND(10000 + RAND() * 490000, 2)                                  AS budget_usd,
  ROUND(8000  + RAND() * 510000, 2)                                  AS actual_usd,
  ROUND((RAND() - 0.5) * 100000, 2)                                  AS variance_usd,
  ['Headcount','Software','Infrastructure','Marketing','Travel'][MOD(n,5)] AS category,
  CONCAT('manager_', CAST(MOD(n,10)+1 AS STRING))                   AS approved_by
FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS n
""")

run("finance.expense_reports_2020", f"""
CREATE OR REPLACE TABLE `{P}.finance.expense_reports_2020` (
  report_id      INT64,
  employee_id    INT64,
  expense_date   DATE,
  category       STRING,
  amount_usd     FLOAT64,
  currency       STRING,
  description    STRING,
  status         STRING,
  submitted_at   DATE
)
AS
SELECT
  n AS report_id,
  MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 2000) + 1           AS employee_id,
  DATE_ADD('2020-01-01', INTERVAL MOD(n, 366) DAY)                  AS expense_date,
  ['Travel','Meals','Software','Equipment','Training'][MOD(n, 5)]    AS category,
  ROUND(10 + RAND() * 4990, 2)                                       AS amount_usd,
  ['CAD','USD','EUR','GBP'][MOD(n,4)]                                AS currency,
  CONCAT('Expense item ', CAST(n AS STRING))                         AS description,
  ['APPROVED','REJECTED','PENDING'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*3 AS STRING))), 3)]              AS status,
  DATE_ADD('2020-01-01', INTERVAL MOD(n, 366) DAY)                  AS submitted_at
FROM UNNEST(GENERATE_ARRAY(1, 40000)) AS n
""")

run("finance.forecast_archive_2022", f"""
CREATE OR REPLACE TABLE `{P}.finance.forecast_archive_2022` (
  forecast_id     INT64,
  forecast_month  STRING,
  department      STRING,
  metric_name     STRING,
  forecast_value  FLOAT64,
  actual_value    FLOAT64,
  accuracy_pct    FLOAT64,
  model_version   STRING
)
AS
SELECT
  n AS forecast_id,
  CONCAT('2022-', LPAD(CAST(MOD(n,12)+1 AS STRING), 2, '0'))       AS forecast_month,
  ['Engineering','Sales','Marketing','Finance'][MOD(n,4)]            AS department,
  ['revenue','headcount','cost','churn_rate'][MOD(n,4)]              AS metric_name,
  ROUND(1000 + RAND() * 999000, 2)                                   AS forecast_value,
  ROUND(900  + RAND() * 1010000, 2)                                  AS actual_value,
  ROUND(70 + RAND() * 30, 2)                                         AS accuracy_pct,
  CONCAT('v', CAST(MOD(n,5)+1 AS STRING), '.0')                     AS model_version
FROM UNNEST(GENERATE_ARRAY(1, 6000)) AS n
""")

run("finance.payroll_history_2021", f"""
CREATE OR REPLACE TABLE `{P}.finance.payroll_history_2021`
PARTITION BY pay_date
AS
SELECT
  n AS payroll_id,
  MOD(ABS(FARM_FINGERPRINT(CAST(n AS STRING))), 2000) + 1           AS employee_id,
  DATE_ADD('2021-01-01', INTERVAL MOD(n, 24) * 15 DAY)             AS pay_date,
  ROUND(2000 + RAND() * 8000, 2)                                     AS gross_pay,
  ROUND(300  + RAND() * 2000, 2)                                     AS tax_withheld,
  ROUND(50   + RAND() * 500,  2)                                     AS benefits_deduction,
  ROUND(1500 + RAND() * 6000, 2)                                     AS net_pay,
  ['Salary','Hourly','Contract'][MOD(n, 3)]                          AS pay_type
FROM UNNEST(GENERATE_ARRAY(1, 50000)) AS n
""")


# ── logs dataset ──────────────────────────────────────────────────────────────
print("\n=== logs dataset ===")

run("logs.app_logs_2022", f"""
CREATE OR REPLACE TABLE `{P}.logs.app_logs_2022`
PARTITION BY log_date
AS
SELECT
  ROW_NUMBER() OVER ()                                              AS log_id,
  DATE_ADD('2022-01-01', INTERVAL MOD(n, 365) DAY)                AS log_date,
  TIMESTAMP_ADD(TIMESTAMP '2022-01-01 00:00:00', INTERVAL n SECOND) AS log_timestamp,
  ['INFO','WARN','ERROR','DEBUG'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*5 AS STRING))), 4)]            AS log_level,
  ['auth-service','payment-service','api-gateway','data-pipeline','scheduler'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*7 AS STRING))), 5)]            AS service,
  CONCAT('Message log entry number ', CAST(n AS STRING))           AS message,
  IF(MOD(n, 20) = 0, CONCAT('ERR-', CAST(MOD(n,1000) AS STRING)), NULL) AS error_code,
  ROUND(RAND() * 2000, 0)                                          AS response_ms
FROM UNNEST(GENERATE_ARRAY(1, 400000)) AS n
""")

run("logs.api_request_log_2021", f"""
CREATE OR REPLACE TABLE `{P}.logs.api_request_log_2021`
PARTITION BY request_date
AS
SELECT
  ROW_NUMBER() OVER ()                                              AS request_id,
  DATE_ADD('2021-01-01', INTERVAL MOD(n, 365) DAY)                AS request_date,
  TIMESTAMP_ADD(TIMESTAMP '2021-01-01 00:00:00', INTERVAL n * 2 SECOND) AS request_ts,
  ['GET','POST','PUT','DELETE','PATCH'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*3 AS STRING))), 5)]            AS method,
  CONCAT('/api/v', CAST(MOD(n,3)+1 AS STRING), '/resource/', CAST(MOD(n,100) AS STRING)) AS endpoint,
  [200, 201, 400, 401, 403, 404, 500][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*11 AS STRING))), 7)]           AS status_code,
  ROUND(10 + RAND() * 990, 0)                                      AS latency_ms,
  CONCAT('client_', CAST(MOD(n, 500)+1 AS STRING))                 AS client_id
FROM UNNEST(GENERATE_ARRAY(1, 250000)) AS n
""")

run("logs.audit_trail_2020", f"""
CREATE OR REPLACE TABLE `{P}.logs.audit_trail_2020` (
  audit_id       INT64,
  audit_date     DATE,
  user_id        STRING,
  action         STRING,
  resource_type  STRING,
  resource_id    STRING,
  outcome        STRING,
  ip_address     STRING
)
AS
SELECT
  n AS audit_id,
  DATE_ADD('2020-01-01', INTERVAL MOD(n, 366) DAY)                AS audit_date,
  CONCAT('user_', CAST(MOD(n, 2000)+1 AS STRING))                 AS user_id,
  ['LOGIN','LOGOUT','CREATE','UPDATE','DELETE','EXPORT','VIEW'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*5 AS STRING))), 7)]            AS action,
  ['record','report','dataset','table','user','role'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*13 AS STRING))), 6)]           AS resource_type,
  CONCAT('res_', CAST(MOD(n, 10000) AS STRING))                   AS resource_id,
  ['SUCCESS','SUCCESS','SUCCESS','DENIED','ERROR'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*7 AS STRING))), 5)]            AS outcome,
  CONCAT(CAST(MOD(n,256) AS STRING),'.',CAST(MOD(n*3,256) AS STRING),'.1.1') AS ip_address
FROM UNNEST(GENERATE_ARRAY(1, 180000)) AS n
""")

run("logs.error_log_archive", f"""
CREATE OR REPLACE TABLE `{P}.logs.error_log_archive`
PARTITION BY error_date
AS
SELECT
  ROW_NUMBER() OVER ()                                              AS error_id,
  DATE_ADD('2020-01-01', INTERVAL MOD(n, 1095) DAY)               AS error_date,
  TIMESTAMP_ADD(TIMESTAMP '2020-01-01 00:00:00', INTERVAL n * 5 SECOND) AS error_ts,
  ['NullPointerException','TimeoutError','ConnectionRefused',
   'OutOfMemoryError','DivisionByZero','KeyError','ValueError'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*7 AS STRING))), 7)]            AS error_type,
  ['auth-service','payment-service','api-gateway','data-pipeline'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*3 AS STRING))), 4)]            AS service,
  ['CRITICAL','ERROR','WARN'][
    MOD(ABS(FARM_FINGERPRINT(CAST(n*11 AS STRING))), 3)]           AS severity,
  CONCAT('Stack trace line ', CAST(n AS STRING), ' in module X')   AS stack_trace_snippet,
  IF(MOD(n, 15) = 0, TRUE, FALSE)                                  AS alerted
FROM UNNEST(GENERATE_ARRAY(1, 100000)) AS n
""")


print("""
=== Cold tables created successfully ===

New datasets and tables (NOT queried - will appear as cold in Storage Advisor):

  archive/
    legacy_orders_2021          ~150k rows  (2021 order history)
    legacy_events_2020          ~300k rows  (2020 web events)
    deprecated_users_2022       ~25k rows   (deactivated accounts)
    old_product_catalog         ~8k rows    (discontinued products)
    historical_sessions_2021    ~120k rows  (2021 session data)

  staging/
    temp_exports_q1_2023        ~5k rows    (old ETL exports)
    raw_imports_jan2023         ~80k rows   (unprocessed raw data)
    backup_transactions_2022    ~200k rows  (transaction backup)
    etl_staging_load            ~3k rows    (ETL batch log)

  finance/
    budget_actuals_2021         ~10k rows   (2021 budget vs actuals)
    expense_reports_2020        ~40k rows   (2020 expense claims)
    forecast_archive_2022       ~6k rows    (model forecasts)
    payroll_history_2021        ~50k rows   (2021 payroll runs)

  logs/
    app_logs_2022               ~400k rows  (application logs)
    api_request_log_2021        ~250k rows  (API access logs)
    audit_trail_2020            ~180k rows  (security audit)
    error_log_archive           ~100k rows  (error records 2020-2022)

Run Storage Advisor to see these flagged as archive candidates.
(Note: TABLE_STORAGE metadata may take ~10 minutes to reflect new tables.)
""")
