"""
BigQuery Query Optimizer - Streamlit UI
Run locally with:  streamlit run app.py
"""

import json
import os
import time
import streamlit as st
from google.cloud import bigquery
from openai import OpenAI

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="BQ Query Optimizer",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar - Configuration ───────────────────────────────────────────────────
with st.sidebar:
    st.title("Configuration")

    # ── Connection ──────────────────────────────────────────────────────────
    st.subheader("🔗 Connection")

    project_id = st.text_input(
        "GCP Project ID",
        value=os.environ.get("GCP_PROJECT", "tharalab1-lab-590b81"),
        help="Your Google Cloud project ID"
    )

    bq_region = st.selectbox(
        "BigQuery Region",
        options=[
            "northamerica-northeast1",
            "northamerica-northeast2",
            "us",
            "us-central1",
            "us-east1",
            "us-east4",
            "us-west1",
            "eu",
            "europe-west1",
            "europe-west2",
            "asia-east1",
            "asia-southeast1",
        ],
        index=0,
        help="Region where your BigQuery jobs run"
    )

    fuelix_key = st.text_input(
        "Fuel IX API Key",
        type="password",
        value=os.environ.get("FUELIX_API_KEY", ""),
        help="Your company Fuel IX API key"
    )

    # ── Model ───────────────────────────────────────────────────────────────
    st.subheader("🤖 Model")

    model = st.selectbox(
        "Model",
        options=[
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-5-2025-08-07",
            "claude-sonnet-4-6",
            "gemini-2.5-pro",
            "gemini-2.0-flash",
            "gemini-3.1-pro",
        ],
        index=6,
    )

    # ── Shared ──────────────────────────────────────────────────────────────
    st.subheader("📅 Shared")

    lookback_days = st.slider(
        "Look-back window (days)",
        min_value=1, max_value=30, value=7,
        help="Used by all three agents. For Anomaly Detector this is the length of each comparison window (last N days vs prior N days)."
    )

    # ── Query Optimizer ─────────────────────────────────────────────────────
    st.subheader("⚡ Query Optimizer")

    top_n = st.slider(
        "Queries to analyze",
        min_value=1, max_value=10, value=4,
        help="Number of most-expensive queries the optimizer will inspect and suggest rewrites for."
    )

    # ── Cost Attribution ────────────────────────────────────────────────────
    st.subheader("💰 Cost Attribution")

    top_n_users = st.slider(
        "Users to show",
        min_value=5, max_value=25, value=10,
        help="Number of top users returned in the cost attribution breakdown."
    )

    st.divider()
    st.caption("Reports are saved as timestamped HTML files in the app directory.")


# ── Tool implementations ───────────────────────────────────────────────────────

# INFORMATION_SCHEMA.JOBS silently truncates the query column at ~1 MB.
# Flag anything >= 900 KB as potentially truncated and attempt a Jobs API fetch.
_BQ_QUERY_TRUNCATION_LIMIT = 900_000


def get_inefficient_queries(bq_client, region, limit=4, hours_back=168):
    from datetime import datetime, timezone
    sql = f"""
    SELECT
      job_id,
      query,
      total_bytes_processed,
      total_slot_ms,
      creation_time,
      user_email
    FROM `region-{region}`.INFORMATION_SCHEMA.JOBS
    WHERE
      creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
      AND job_type  = 'QUERY'
      AND state     = 'DONE'
      AND error_result IS NULL
      AND total_bytes_processed > 0
      AND LOWER(query) NOT LIKE '%information_schema%'
      AND TRIM(query) != ''
    ORDER BY total_bytes_processed DESC
    LIMIT {limit}
    """
    try:
        job   = bq_client.query(sql)
        result = job.result()
        rows  = []
        for row in result:
            query_text = row.query or ""
            truncated  = len(query_text) >= _BQ_QUERY_TRUNCATION_LIMIT

            # Fetch full query text via Jobs API when truncation is detected
            if truncated:
                try:
                    full_job   = bq_client.get_job(row.job_id)
                    query_text = full_job.query or query_text
                    truncated  = False
                except Exception:
                    pass  # fall back to truncated version

            rows.append({
                "job_id":                   row.job_id,
                "query":                    query_text,
                "truncated":                truncated,
                "total_bytes_processed":    row.total_bytes_processed,
                "total_bytes_processed_gb": round(row.total_bytes_processed / (1024 ** 3), 4),
                "total_slot_ms":            row.total_slot_ms,
                "creation_time":            str(row.creation_time),
                "user_email":               row.user_email,
            })

        # Capture the optimizer's own job stats
        optimizer_slots = job.slot_millis or 0
        duration_ms     = int((job.ended - job.started).total_seconds() * 1000) if job.ended and job.started else 0

        optimizer_stats = {
            "report_generated_at":    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "optimizer_job_id":       job.job_id,
            "optimizer_slot_ms":      optimizer_slots,
            "optimizer_slot_seconds": round(optimizer_slots / 1000, 3),
            "optimizer_duration_ms":  duration_ms,
        }

        return {"queries": rows, "count": len(rows), "optimizer_stats": optimizer_stats}
    except Exception as exc:
        return {"error": str(exc), "queries": [], "count": 0}


def dry_run_query(bq_client, sql):
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = bq_client.query(sql, job_config=job_config)
        bytes_p = job.total_bytes_processed
        return {
            "estimated_bytes_processed": bytes_p,
            "estimated_gb":              round(bytes_p / (1024 ** 3), 4),
            "valid":                     True,
        }
    except Exception as exc:
        return {"error": str(exc), "estimated_bytes_processed": None, "valid": False}


def save_html_report(html_content, output_path):
    try:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(html_content)
        return {"saved_to": output_path, "success": True}
    except Exception as exc:
        return {"error": str(exc), "success": False}


def get_cost_attribution(bq_client, region, hours_back=168, limit=10):
    sql = f"""
    SELECT
      user_email,
      COUNT(*)                                                  AS query_count,
      SUM(total_bytes_processed)                                AS total_bytes_processed,
      ROUND(SUM(total_bytes_processed) / POW(1024, 4), 6)       AS total_tb_processed,
      ROUND(SUM(total_bytes_processed) / POW(1024, 4) * 5, 4)   AS estimated_cost_usd,
      SUM(total_slot_ms)                                        AS total_slot_ms,
      MAX(total_bytes_processed)                                AS max_bytes_single_query,
      ROUND(AVG(total_bytes_processed) / POW(1024, 3), 4)       AS avg_bytes_per_query_gb
    FROM `region-{region}`.INFORMATION_SCHEMA.JOBS
    WHERE
      creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
      AND job_type  = 'QUERY'
      AND state     = 'DONE'
      AND error_result IS NULL
      AND total_bytes_processed > 0
      AND LOWER(query) NOT LIKE '%information_schema%'
      AND TRIM(query) != ''
    GROUP BY user_email
    ORDER BY total_bytes_processed DESC
    LIMIT {limit}
    """
    try:
        rows = []
        for row in bq_client.query(sql).result():
            rows.append({
                "user_email":            row.user_email,
                "query_count":           row.query_count,
                "total_bytes_processed": row.total_bytes_processed,
                "total_tb_processed":    float(row.total_tb_processed),
                "estimated_cost_usd":    float(row.estimated_cost_usd),
                "total_slot_ms":         row.total_slot_ms,
                "max_bytes_single_query": row.max_bytes_single_query,
                "avg_bytes_per_query_gb": float(row.avg_bytes_per_query_gb),
            })
        return {"attributions": rows, "count": len(rows), "hours_back": hours_back}
    except Exception as exc:
        return {"error": str(exc), "attributions": [], "count": 0}


def get_spend_anomalies(bq_client, region, recent_days=7):
    """Compare each user's spend over the last recent_days vs the equal prior window."""
    sql = f"""
    WITH recent AS (
      SELECT
        user_email,
        COUNT(*)                                              AS query_count_recent,
        SUM(total_bytes_processed)                            AS bytes_recent,
        ROUND(SUM(total_bytes_processed) / POW(1024,4) * 5, 4) AS cost_usd_recent
      FROM `region-{region}`.INFORMATION_SCHEMA.JOBS
      WHERE
        creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {recent_days} DAY)
        AND job_type = 'QUERY' AND state = 'DONE'
        AND error_result IS NULL AND total_bytes_processed > 0
        AND LOWER(query) NOT LIKE '%information_schema%'
      GROUP BY user_email
    ),
    prior AS (
      SELECT
        user_email,
        COUNT(*)                                              AS query_count_prior,
        SUM(total_bytes_processed)                            AS bytes_prior,
        ROUND(SUM(total_bytes_processed) / POW(1024,4) * 5, 4) AS cost_usd_prior
      FROM `region-{region}`.INFORMATION_SCHEMA.JOBS
      WHERE
        creation_time BETWEEN
          TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {recent_days * 2} DAY)
          AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {recent_days} DAY)
        AND job_type = 'QUERY' AND state = 'DONE'
        AND error_result IS NULL AND total_bytes_processed > 0
        AND LOWER(query) NOT LIKE '%information_schema%'
      GROUP BY user_email
    )
    SELECT
      r.user_email,
      r.query_count_recent,
      COALESCE(p.query_count_prior, 0)                       AS query_count_prior,
      ROUND(r.bytes_recent  / POW(1024,3), 2)                AS gb_recent,
      ROUND(COALESCE(p.bytes_prior, 0) / POW(1024,3), 2)     AS gb_prior,
      r.cost_usd_recent,
      COALESCE(p.cost_usd_prior, 0)                          AS cost_usd_prior,
      CASE
        WHEN p.bytes_prior IS NULL OR p.bytes_prior = 0 THEN NULL
        ELSE ROUND(r.bytes_recent / p.bytes_prior, 2)
      END                                                    AS change_ratio,
      CASE
        WHEN p.bytes_prior IS NULL THEN 'NEW_USER'
        WHEN p.bytes_prior = 0    THEN 'NEW_ACTIVITY'
        WHEN r.bytes_recent / p.bytes_prior >= 3 THEN 'RED'
        WHEN r.bytes_recent / p.bytes_prior >= 2 THEN 'AMBER'
        ELSE 'GREEN'
      END                                                    AS status
    FROM recent r
    LEFT JOIN prior p USING (user_email)
    ORDER BY r.bytes_recent DESC
    """
    try:
        rows = []
        for row in bq_client.query(sql).result():
            rows.append({
                "user_email":         row.user_email,
                "query_count_recent": row.query_count_recent,
                "query_count_prior":  row.query_count_prior,
                "gb_recent":          float(row.gb_recent),
                "gb_prior":           float(row.gb_prior),
                "cost_usd_recent":    float(row.cost_usd_recent),
                "cost_usd_prior":     float(row.cost_usd_prior),
                "change_ratio":       float(row.change_ratio) if row.change_ratio is not None else None,
                "status":             row.status,
            })
        return {"anomalies": rows, "count": len(rows), "recent_days": recent_days}
    except Exception as exc:
        return {"error": str(exc), "anomalies": [], "count": 0}


def get_user_top_queries(bq_client, region, user_email, hours_back=168, limit=3):
    """Fetch the top N most expensive recent queries for a specific user."""
    sql = f"""
    SELECT
      job_id,
      SUBSTR(query, 1, 500)                                   AS query_preview,
      ROUND(total_bytes_processed / POW(1024,3), 2)           AS gb_processed,
      ROUND(total_bytes_processed / POW(1024,4) * 5, 4)       AS estimated_cost_usd,
      total_slot_ms,
      creation_time
    FROM `region-{region}`.INFORMATION_SCHEMA.JOBS
    WHERE
      creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
      AND user_email = '{user_email}'
      AND job_type = 'QUERY' AND state = 'DONE'
      AND error_result IS NULL AND total_bytes_processed > 0
      AND LOWER(query) NOT LIKE '%information_schema%'
    ORDER BY total_bytes_processed DESC
    LIMIT {limit}
    """
    try:
        rows = []
        for row in bq_client.query(sql).result():
            rows.append({
                "job_id":             row.job_id,
                "query_preview":      row.query_preview,
                "gb_processed":       float(row.gb_processed),
                "estimated_cost_usd": float(row.estimated_cost_usd),
                "total_slot_ms":      row.total_slot_ms,
                "creation_time":      str(row.creation_time),
            })
        return {"user_email": user_email, "queries": rows, "count": len(rows)}
    except Exception as exc:
        return {"error": str(exc), "user_email": user_email, "queries": [], "count": 0}


def get_most_hit_tables(bq_client, region, hours_back=168, limit=15):
    """
    Unnest referenced_tables from INFORMATION_SCHEMA.JOBS to find the most
    frequently accessed tables. query_count = distinct jobs that touched the table.
    total_bytes_from_jobs is the sum of job-level bytes - note this double-counts
    jobs that reference multiple tables, so treat it as a cost-association proxy,
    not an exact scan cost.
    """
    sql = f"""
    SELECT
      CONCAT(rt.project_id, '.', rt.dataset_id, '.', rt.table_id) AS full_table_name,
      rt.dataset_id,
      rt.table_id,
      COUNT(DISTINCT j.job_id)                                     AS query_count,
      COUNT(DISTINCT j.user_email)                                 AS distinct_users,
      SUM(j.total_bytes_processed)                                 AS total_bytes_from_jobs,
      ROUND(SUM(j.total_bytes_processed) / POW(1024, 3), 2)        AS total_gb_from_jobs,
      ROUND(SUM(j.total_bytes_processed) / POW(1024, 4) * 5, 4)   AS associated_cost_usd
    FROM `region-{region}`.INFORMATION_SCHEMA.JOBS AS j,
    UNNEST(j.referenced_tables) AS rt
    WHERE
      j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
      AND j.job_type   = 'QUERY'
      AND j.state      = 'DONE'
      AND j.error_result IS NULL
      AND j.total_bytes_processed > 0
      AND LOWER(j.query) NOT LIKE '%information_schema%'
    GROUP BY 1, 2, 3
    ORDER BY query_count DESC
    LIMIT {limit}
    """
    try:
        rows = []
        for row in bq_client.query(sql).result():
            rows.append({
                "full_table_name":      row.full_table_name,
                "dataset_id":           row.dataset_id,
                "table_id":             row.table_id,
                "query_count":          row.query_count,
                "distinct_users":       row.distinct_users,
                "total_gb_from_jobs":   float(row.total_gb_from_jobs),
                "associated_cost_usd":  float(row.associated_cost_usd),
            })
        return {
            "tables": rows,
            "count": len(rows),
            "note": "associated_cost_usd is a proxy - bytes are summed at the job level and may double-count multi-table queries.",
        }
    except Exception as exc:
        return {"error": str(exc), "tables": [], "count": 0}


# ── Storage & query-pattern tools ─────────────────────────────────────────────
def get_table_storage_stats(bq_client, project_id, region, min_gb=0.1, limit=25):
    """Return storage size and cost breakdown for the largest tables in the project."""
    sql = f"""
    SELECT
      table_schema                                                         AS dataset_id,
      table_name                                                           AS table_id,
      CONCAT(table_schema, '.', table_name)                               AS full_table_name,
      ROUND(total_logical_bytes / POW(1024, 3), 2)                        AS size_gb,
      ROUND(total_logical_bytes / POW(1024, 3) * 0.023,  4)              AS monthly_active_cost_usd,
      ROUND(total_logical_bytes / POW(1024, 3) * 0.016,  4)              AS monthly_longterm_cost_usd,
      ROUND(total_logical_bytes / POW(1024, 3) * 0.0012, 4)              AS monthly_archive_cost_usd,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), storage_last_modified_time, DAY) AS days_since_modified,
      storage_last_modified_time                                           AS last_modified_time,
      total_rows                                                           AS row_count,
      CASE
        WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), storage_last_modified_time, DAY) >= 90
        THEN 'LONG_TERM' ELSE 'ACTIVE'
      END                                                                  AS storage_class
    FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE total_logical_bytes >= POW(1024, 3) * {min_gb}
    ORDER BY total_logical_bytes DESC
    LIMIT {limit}
    """
    try:
        rows = []
        for row in bq_client.query(sql).result():
            rows.append({
                "dataset_id":               row.dataset_id,
                "table_id":                 row.table_id,
                "full_table_name":          row.full_table_name,
                "size_gb":                  float(row.size_gb),
                "monthly_active_cost_usd":  float(row.monthly_active_cost_usd),
                "monthly_longterm_cost_usd": float(row.monthly_longterm_cost_usd),
                "monthly_archive_cost_usd": float(row.monthly_archive_cost_usd),
                "days_since_modified":      row.days_since_modified,
                "last_modified_time":       str(row.last_modified_time),
                "row_count":                row.row_count,
                "storage_class":            row.storage_class,
            })
        total_monthly = sum(r["monthly_active_cost_usd"] for r in rows)
        return {"tables": rows, "count": len(rows), "total_monthly_cost_usd": round(total_monthly, 4)}
    except Exception as exc:
        return {"error": str(exc), "tables": [], "count": 0}


def get_cold_tables(bq_client, project_id, region, min_days_unqueried=90, min_gb=5, limit=20):
    """Find tables not queried recently - prime candidates for GCS archiving.

    Runs two separate queries (TABLE_STORAGE and JOBS) and merges in Python
    to avoid cross-resource JOIN limitations in BigQuery.
    """
    from datetime import datetime, timezone

    # ── Query 1: large tables from TABLE_STORAGE ──────────────────────────────
    storage_sql = f"""
    SELECT
      table_schema                                                         AS dataset_id,
      table_name                                                           AS table_id,
      ROUND(total_logical_bytes / POW(1024, 3), 2)                        AS size_gb,
      ROUND(total_logical_bytes / POW(1024, 3) * 0.023,   4)             AS monthly_cost_usd,
      ROUND(total_logical_bytes / POW(1024, 3) * 0.023  * 12, 2)         AS annual_cost_usd,
      ROUND(total_logical_bytes / POW(1024, 3) * 0.0012 * 12, 2)         AS annual_archive_cost_usd,
      ROUND((0.023 - 0.0012) * total_logical_bytes / POW(1024, 3) * 12, 2) AS annual_savings_if_archived_usd,
      storage_last_modified_time                                           AS last_modified_time
    FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE total_logical_bytes >= POW(1024, 3) * {min_gb}
    ORDER BY total_logical_bytes DESC
    LIMIT 200
    """

    # ── Query 2: last query time per table from JOBS ───────────────────────────
    jobs_sql = f"""
    SELECT
      CONCAT(rt.project_id, '.', rt.dataset_id, '.', rt.table_id) AS full_table_name,
      MAX(j.creation_time)                                          AS last_queried_at
    FROM `region-{region}`.INFORMATION_SCHEMA.JOBS AS j,
    UNNEST(j.referenced_tables) AS rt
    WHERE
      j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
      AND j.job_type = 'QUERY'
      AND j.state    = 'DONE'
      AND j.project_id = '{project_id}'
      AND LOWER(j.query) NOT LIKE '%information_schema%'
    GROUP BY 1
    """

    try:
        # Run storage query
        storage_rows = {
            f"{project_id}.{r.dataset_id}.{r.table_id}": r
            for r in bq_client.query(storage_sql).result()
        }
    except Exception as exc:
        return {"error": f"TABLE_STORAGE query failed: {exc}", "cold_tables": [], "count": 0}

    try:
        # Run jobs query and build lookup {full_table_name -> last_queried_at}
        last_queried = {}
        for r in bq_client.query(jobs_sql).result():
            last_queried[r.full_table_name] = r.last_queried_at
    except Exception:
        # JOBS query is best-effort; fall back to modification time only
        last_queried = {}

    now = datetime.now(timezone.utc)
    rows = []
    for full_name, s in storage_rows.items():
        lm = s.last_modified_time
        days_modified = (now - lm).days if lm else None

        lq = last_queried.get(full_name)
        days_queried = (now - lq).days if lq else None

        # Use last_queried if available, otherwise fall back to last_modified
        cold_days = days_queried if days_queried is not None else days_modified

        if cold_days is None or cold_days < min_days_unqueried:
            continue

        if cold_days >= 180:
            coldness = "NEVER_QUERIED_IN_180_DAYS"
        else:
            coldness = "COLD"

        rows.append({
            "dataset_id":                     s.dataset_id,
            "table_id":                       s.table_id,
            "full_table_name":                full_name,
            "size_gb":                        float(s.size_gb),
            "monthly_cost_usd":               float(s.monthly_cost_usd),
            "annual_cost_usd":                float(s.annual_cost_usd),
            "annual_archive_cost_usd":        float(s.annual_archive_cost_usd),
            "annual_savings_if_archived_usd": float(s.annual_savings_if_archived_usd),
            "last_modified_time":             str(lm),
            "days_since_modified":            days_modified,
            "last_queried_at":                str(lq) if lq else None,
            "days_since_queried":             days_queried,
            "coldness_status":                coldness,
        })

    rows.sort(key=lambda r: r["annual_savings_if_archived_usd"], reverse=True)
    rows = rows[:limit]
    total_savings = sum(r["annual_savings_if_archived_usd"] for r in rows)
    return {
        "cold_tables":  rows,
        "count":        len(rows),
        "total_annual_savings_if_all_archived_usd": round(total_savings, 2),
        "min_days_unqueried": min_days_unqueried,
    }


def get_partition_filter_violations(bq_client, region, hours_back=168, limit=10):
    """
    Find expensive queries (>= 1 GB) that likely lack date/timestamp filters -
    scanning full tables instead of targeted partitions.
    Heuristic: absence of date-like column patterns in WHERE clause.
    """
    sql = f"""
    SELECT
      job_id,
      SUBSTR(query, 1, 600)                                              AS query_preview,
      total_bytes_processed,
      ROUND(total_bytes_processed / POW(1024, 3), 2)                     AS gb_processed,
      ROUND(total_bytes_processed / POW(1024, 4) * 5, 4)                 AS estimated_cost_usd,
      user_email,
      creation_time,
      REGEXP_CONTAINS(LOWER(query), r'select\\s+\\*')                     AS uses_select_star
    FROM `region-{region}`.INFORMATION_SCHEMA.JOBS
    WHERE
      creation_time  >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
      AND job_type   = 'QUERY'
      AND state      = 'DONE'
      AND error_result IS NULL
      AND total_bytes_processed >= POW(1024, 3)
      AND LOWER(query) NOT LIKE '%information_schema%'
      AND TRIM(query) != ''
      AND NOT REGEXP_CONTAINS(
            LOWER(query),
            r'(where|and)\\s+.{{0,120}}(_date|_time|_at|\\bdate\\b|\\btimestamp\\b|created_at|updated_at|partition_date|event_date|_day|_month|_year)'
          )
    ORDER BY total_bytes_processed DESC
    LIMIT {limit}
    """
    try:
        rows = []
        for row in bq_client.query(sql).result():
            rows.append({
                "job_id":             row.job_id,
                "query_preview":      row.query_preview,
                "gb_processed":       float(row.gb_processed),
                "estimated_cost_usd": float(row.estimated_cost_usd),
                "user_email":         row.user_email,
                "creation_time":      str(row.creation_time),
                "uses_select_star":   row.uses_select_star,
            })
        total_wasted = sum(r["estimated_cost_usd"] for r in rows)
        return {
            "violations":            rows,
            "count":                 len(rows),
            "total_wasted_cost_usd": round(total_wasted, 4),
            "note": "Heuristic: queries >= 1 GB with no date/timestamp filter in WHERE clause.",
        }
    except Exception as exc:
        return {"error": str(exc), "violations": [], "count": 0}


def get_wildcard_scan_queries(bq_client, region, hours_back=168, limit=10):
    """Find queries using wildcard table patterns - silently scanning all matching historical tables."""
    sql = f"""
    SELECT
      job_id,
      SUBSTR(query, 1, 600)                                AS query_preview,
      total_bytes_processed,
      ROUND(total_bytes_processed / POW(1024, 3), 2)       AS gb_processed,
      ROUND(total_bytes_processed / POW(1024, 4) * 5, 4)   AS estimated_cost_usd,
      user_email,
      creation_time,
      REGEXP_EXTRACT(query, r'`([^`]+\\*[^`]*)`')           AS wildcard_pattern
    FROM `region-{region}`.INFORMATION_SCHEMA.JOBS
    WHERE
      creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
      AND job_type   = 'QUERY'
      AND state      = 'DONE'
      AND error_result IS NULL
      AND total_bytes_processed > 0
      AND LOWER(query) NOT LIKE '%information_schema%'
      AND REGEXP_CONTAINS(query, r'`[^`]+\\*[^`]*`')
    ORDER BY total_bytes_processed DESC
    LIMIT {limit}
    """
    try:
        rows = []
        for row in bq_client.query(sql).result():
            rows.append({
                "job_id":             row.job_id,
                "query_preview":      row.query_preview,
                "gb_processed":       float(row.gb_processed),
                "estimated_cost_usd": float(row.estimated_cost_usd),
                "user_email":         row.user_email,
                "creation_time":      str(row.creation_time),
                "wildcard_pattern":   row.wildcard_pattern,
            })
        total_cost = sum(r["estimated_cost_usd"] for r in rows)
        return {
            "wildcard_queries": rows,
            "count":            len(rows),
            "total_cost_usd":   round(total_cost, 4),
            "note": "Queries using wildcard table patterns (e.g. `table_*`) scan all matching historical tables.",
        }
    except Exception as exc:
        return {"error": str(exc), "wildcard_queries": [], "count": 0}


# ── ASCII sanitizer (Fuel IX proxy requires ASCII-safe request bodies) ─────────
def _ascii_safe(obj):
    """Recursively replace non-ASCII characters in strings with ASCII equivalents.

    Needed because the Fuel IX proxy serialises request bodies with ASCII encoding.
    Applied to the messages list before every API call so that model-generated text
    (which may contain em-dashes, curly quotes, etc.) doesn't break the next turn.
    """
    if isinstance(obj, str):
        return (obj
                .replace('\u2014', '-')    # em dash
                .replace('\u2013', '-')    # en dash
                .replace('\u2018', "'")    # left single quote
                .replace('\u2019', "'")    # right single quote
                .replace('\u201c', '"')    # left double quote
                .replace('\u201d', '"')    # right double quote
                .replace('\u2026', '...')  # ellipsis
                .replace('\u2192', '->')   # right arrow
                .replace('\u2190', '<-')   # left arrow
                .encode('ascii', errors='replace').decode('ascii')  # catch-all
                )
    if isinstance(obj, dict):
        return {k: _ascii_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_ascii_safe(item) for item in obj]
    return obj


def _safe_messages(messages):
    """Convert a list of messages (dicts or SDK objects) to ASCII-safe plain dicts."""
    result = []
    for msg in messages:
        if isinstance(msg, dict):
            result.append(_ascii_safe(msg))
        else:
            # OpenAI SDK ChatCompletionMessage object — serialise to dict first
            try:
                d = msg.model_dump()          # pydantic v2
            except AttributeError:
                d = msg.dict()                # pydantic v1
            result.append(_ascii_safe(d))
    return result


# ── Shared tool schema registry ───────────────────────────────────────────────
def build_tool_schemas(cfg=None):
    """
    Build all OpenAI function-call schemas in one place.
    Each agent picks only the tools it needs by key.

    cfg keys (all optional - used for description defaults):
        hours_back   int   default 168
        top_n        int   default 4
        top_n_users  int   default 10
        lookback_days int  default 7
    """
    if cfg is None:
        cfg = {}
    h  = cfg.get("hours_back",    168)
    n  = cfg.get("top_n",           4)
    nu = cfg.get("top_n_users",    10)
    rd = cfg.get("lookback_days",   7)

    return {
        "get_inefficient_queries": {
            "type": "function",
            "function": {
                "name": "get_inefficient_queries",
                "description": (
                    "Query BigQuery INFORMATION_SCHEMA.JOBS to find the top N most "
                    "expensive queries ranked by total bytes processed."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit":      {"type": "integer", "description": f"Number of queries (default {n})"},
                        "hours_back": {"type": "integer", "description": f"Hours to look back (default {h})"},
                    },
                    "required": []
                }
            }
        },
        "get_cost_attribution": {
            "type": "function",
            "function": {
                "name": "get_cost_attribution",
                "description": (
                    "Query BigQuery INFORMATION_SCHEMA.JOBS grouped by user_email. "
                    "Returns per-user total cost (USD), query count, avg GB/query, max GB single query."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit":      {"type": "integer", "description": f"Number of top users (default {nu})"},
                        "hours_back": {"type": "integer", "description": f"Hours to look back (default {h})"},
                    },
                    "required": []
                }
            }
        },
        "get_most_hit_tables": {
            "type": "function",
            "function": {
                "name": "get_most_hit_tables",
                "description": (
                    "Unnest referenced_tables from INFORMATION_SCHEMA.JOBS to find the most "
                    "frequently accessed BigQuery tables, ranked by query_count. "
                    "Also returns distinct_users and an associated_cost_usd proxy."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit":      {"type": "integer", "description": "Number of top tables (default 15)"},
                        "hours_back": {"type": "integer", "description": f"Hours to look back (default {h})"},
                    },
                    "required": []
                }
            }
        },
        "get_spend_anomalies": {
            "type": "function",
            "function": {
                "name": "get_spend_anomalies",
                "description": (
                    "Compare each user's BigQuery spend over the last N days against the equal prior window. "
                    "Returns per-user change_ratio and status (GREEN / AMBER / RED / NEW_USER / NEW_ACTIVITY)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "recent_days": {"type": "integer", "description": f"Comparison window in days (default {rd})"},
                    },
                    "required": []
                }
            }
        },
        "get_user_top_queries": {
            "type": "function",
            "function": {
                "name": "get_user_top_queries",
                "description": (
                    "Fetch the top N most expensive recent queries for a specific user. "
                    "Use to inspect the actual SQL a user is running."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "user_email": {"type": "string", "description": "Email of the user to drill into"},
                        "hours_back": {"type": "integer", "description": f"Hours to look back (default {h})"},
                        "limit":      {"type": "integer", "description": "Number of queries to return (default 3)"},
                    },
                    "required": ["user_email"]
                }
            }
        },
        "get_user_health_scores": {
            "type": "function",
            "function": {
                "name": "get_user_health_scores",
                "description": (
                    "Compute a 0-100 health score per user by joining cost attribution and anomaly data. "
                    "Score = Cost (0-40) + Optimisation (0-30) + Anomaly (0-30). "
                    "Returns users sorted worst-first with score, label, and component breakdown."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit":       {"type": "integer", "description": f"Number of users to score (default {nu})"},
                        "hours_back":  {"type": "integer", "description": f"Attribution look-back in hours (default {h})"},
                        "recent_days": {"type": "integer", "description": f"Anomaly window in days (default {rd})"},
                    },
                    "required": []
                }
            }
        },
        "dry_run_query": {
            "type": "function",
            "function": {
                "name": "dry_run_query",
                "description": "Estimate bytes processed for a SQL query via BigQuery dry run.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "SQL to dry run"}
                    },
                    "required": ["sql"]
                }
            }
        },
        "save_html_report": {
            "type": "function",
            "function": {
                "name": "save_html_report",
                "description": "Save the final HTML report to disk.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "html_content": {"type": "string", "description": "Full HTML document"}
                    },
                    "required": ["html_content"]
                }
            }
        },
        "preview_revoke_access": {
            "type": "function",
            "function": {
                "name": "preview_revoke_access",
                "description": (
                    "Preview removing a user's IAM access from a BigQuery table WITHOUT applying any changes. "
                    "Always call this first when asked to block/revoke/remove a user's table access. "
                    "Present the result to the user and ask them to type CONFIRM to proceed or CANCEL to abort."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "user_email": {"type": "string", "description": "Email of the user whose access will be revoked"},
                        "dataset_id": {"type": "string", "description": "BigQuery dataset name"},
                        "table_id":   {"type": "string", "description": "BigQuery table name"},
                    },
                    "required": ["user_email", "dataset_id", "table_id"]
                }
            }
        },
        "execute_revoke_access": {
            "type": "function",
            "function": {
                "name": "execute_revoke_access",
                "description": (
                    "Actually remove a user's IAM access from a BigQuery table. "
                    "ONLY call this after the user has explicitly typed CONFIRM in their message. "
                    "Never call this without prior explicit user confirmation."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "user_email": {"type": "string", "description": "Email of the user whose access will be revoked"},
                        "dataset_id": {"type": "string", "description": "BigQuery dataset name"},
                        "table_id":   {"type": "string", "description": "BigQuery table name"},
                    },
                    "required": ["user_email", "dataset_id", "table_id"]
                }
            }
        },
        "get_table_storage_stats": {
            "type": "function",
            "function": {
                "name": "get_table_storage_stats",
                "description": (
                    "Return storage size and monthly cost breakdown for the largest tables in the project. "
                    "Shows active ($0.023/GB), long-term ($0.016/GB), and GCS Archive ($0.0012/GB) costs. "
                    "Use to understand where storage budget is going."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "min_gb": {"type": "number",  "description": "Minimum table size in GB to include (default 0.1)"},
                        "limit":  {"type": "integer", "description": "Number of tables to return (default 25)"},
                    },
                    "required": []
                }
            }
        },
        "get_cold_tables": {
            "type": "function",
            "function": {
                "name": "get_cold_tables",
                "description": (
                    "Find tables not queried recently - prime candidates for archiving to GCS. "
                    "Estimates annual savings if migrated from BigQuery active storage ($0.023/GB) "
                    "to GCS Archive ($0.0012/GB). Results sorted by savings descending."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "min_days_unqueried": {"type": "integer", "description": f"Days without a query to flag as cold (default 90)"},
                        "min_gb":             {"type": "number",  "description": "Minimum table size in GB (default 5)"},
                        "limit":              {"type": "integer", "description": "Number of tables to return (default 20)"},
                    },
                    "required": []
                }
            }
        },
        "get_partition_filter_violations": {
            "type": "function",
            "function": {
                "name": "get_partition_filter_violations",
                "description": (
                    "Find expensive queries (>= 1 GB scanned) that lack date/timestamp filters - "
                    "likely performing full table scans instead of partition-pruned reads. "
                    "Heuristic based on absence of date-like column patterns in WHERE clause."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "hours_back": {"type": "integer", "description": f"Hours to look back (default {h})"},
                        "limit":      {"type": "integer", "description": "Number of violations to return (default 10)"},
                    },
                    "required": []
                }
            }
        },
        "get_wildcard_scan_queries": {
            "type": "function",
            "function": {
                "name": "get_wildcard_scan_queries",
                "description": (
                    "Find queries using wildcard table patterns (e.g. `table_*`) that silently scan "
                    "all matching historical tables. These are often the most expensive surprise queries."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "hours_back": {"type": "integer", "description": f"Hours to look back (default {h})"},
                        "limit":      {"type": "integer", "description": "Number of queries to return (default 10)"},
                    },
                    "required": []
                }
            }
        },
    }


def run_anomaly_detector(project_id, bq_region, fuelix_key, model, recent_days, log_container, status_container):
    """Run the spend anomaly detection agentic loop and stream progress to the UI."""

    from datetime import datetime
    timestamp   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = os.path.join(os.path.dirname(__file__), f"anomaly_report_{timestamp}.html")
    tool_log    = []

    def append_log(icon, title, detail=""):
        tool_log.append({"icon": icon, "title": title, "detail": detail})
        with log_container:
            for entry in tool_log:
                with st.expander(f"{entry['icon']}  {entry['title']}", expanded=False):
                    if entry["detail"]:
                        st.code(entry["detail"], language="json")

    try:
        bq_client     = bigquery.Client(project=project_id)
        fuelix_client = OpenAI(api_key=fuelix_key, base_url="https://proxy.fuelix.ai/")
    except Exception as exc:
        status_container.error(f"Failed to initialize clients: {exc}")
        return None, None

    _s    = build_tool_schemas({"hours_back": recent_days * 24, "lookback_days": recent_days})
    TOOLS = [_s["get_spend_anomalies"], _s["get_user_top_queries"], _s["save_html_report"]]

    PROMPT = f"""\
Detect BigQuery spend anomalies for project '{project_id}', comparing the last {recent_days} days \
against the prior {recent_days}-day window.

Steps:
1. Call get_spend_anomalies to get the week-over-week breakdown for all users.
2. For every user with status RED, AMBER, NEW_USER, or NEW_ACTIVITY, \
call get_user_top_queries to see which specific queries drove the spike.
3. Generate a polished HTML report that includes:
   - A summary banner: total users analysed, how many are RED / AMBER / GREEN / NEW.
   - A main table with columns: Status (traffic-light emoji 🔴/🟡/🟢), User, \
Prior Cost (USD), Recent Cost (USD), Change, Queries (recent), Queries (prior).
   - For each RED / AMBER / NEW user: an expandable sub-section showing their top 3 queries \
(query preview, GB processed, estimated cost, job ID).
   - A "Recommendations" section with concrete actions: investigate full-table scans, \
review new ETL jobs, check partition filter regressions, consider quotas for repeat offenders.
4. Call save_html_report with the complete HTML.\
"""

    messages = [
        {"role": "system", "content": "You are a BigQuery cost anomaly analyst. Use the tools to detect unusual spend spikes and generate a clear, actionable HTML report."},
        {"role": "user",   "content": PROMPT},
    ]

    html_content = None
    iterations   = 0
    max_iter     = 25

    while iterations < max_iter:
        iterations += 1
        status_container.info(f"Thinking… (step {iterations})")

        try:
            response = fuelix_client.chat.completions.create(
                model=model,
                messages=_safe_messages(messages),
                tools=TOOLS,
                tool_choice="auto",
            )
        except Exception as exc:
            status_container.error(f"Fuel IX API error: {exc}")
            return None, None

        message = response.choices[0].message

        if message.content:
            append_log("💬", "Model response", message.content[:500])

        messages.append(_safe_messages([message])[0])

        if not message.tool_calls:
            status_container.success("Anomaly detection complete!")
            break

        for tool_call in message.tool_calls:
            name = tool_call.function.name
            raw  = tool_call.function.arguments

            try:
                inputs = json.loads(raw)
            except json.JSONDecodeError:
                import re
                match = re.search(r'"html_content"\s*:\s*"(.*)', raw, re.DOTALL)
                if match and name == "save_html_report":
                    html_str = match.group(1)
                    for suffix in ['"}', '"', '}']:
                        if html_str.endswith(suffix):
                            html_str = html_str[:-len(suffix)]
                            break
                    html_str = html_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
                    inputs = {"html_content": html_str}
                    append_log("⚠️", "JSON repair applied for save_html_report",
                               f"{len(html_str):,} chars recovered")
                else:
                    result = {"error": "JSON parse failed"}
                    append_log("❌", f"Could not parse tool arguments for {name}")
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(result),
                    })
                    continue

            if name == "get_spend_anomalies":
                r_days = inputs.get("recent_days", recent_days)
                status_container.info(f"Comparing last {r_days} days vs prior {r_days} days…")
                result = get_spend_anomalies(bq_client, bq_region, r_days)
                red_amber = [r for r in result.get("anomalies", []) if r["status"] in ("RED", "AMBER", "NEW_USER", "NEW_ACTIVITY")]
                append_log(
                    "🚨", f"get_spend_anomalies - {result.get('count', 0)} users, {len(red_amber)} flagged",
                    json.dumps(result, indent=2, default=str)[:3000]
                )

            elif name == "get_user_top_queries":
                user   = inputs.get("user_email", "")
                h_back = inputs.get("hours_back", recent_days * 24)
                limit  = inputs.get("limit", 3)
                status_container.info(f"Drilling into top queries for {user}…")
                result = get_user_top_queries(bq_client, bq_region, user, h_back, limit)
                append_log(
                    "🔎", f"get_user_top_queries - {user} ({result.get('count', 0)} queries)",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "save_html_report":
                status_container.info("Saving HTML report…")
                html_body = inputs.get("html_content", "")
                if not html_body.strip():
                    result = {"error": "html_content was empty - please regenerate the full HTML report"}
                    append_log("⚠️", "save_html_report - empty content, asking model to retry")
                else:
                    result       = save_html_report(html_body, output_file)
                    html_content = html_body
                    append_log(
                        "💾", f"save_html_report → {result.get('saved_to', 'unknown')} ({len(html_body):,} chars)",
                        json.dumps(result, indent=2)
                    )

            else:
                result = {"error": f"Unknown tool: {name}"}
                append_log("❌", f"Unknown tool: {name}")

            messages.append({
                "role":         "tool",
                "tool_call_id": tool_call.id,
                "content":      json.dumps(result, default=str),
            })

    return html_content, output_file


def run_cost_attribution(project_id, bq_region, fuelix_key, model, lookback_days, top_n_users, log_container, status_container):
    """Run the cost attribution agentic loop and stream progress to the UI."""

    from datetime import datetime
    hours_back  = lookback_days * 24
    timestamp   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = os.path.join(os.path.dirname(__file__), f"cost_attribution_{timestamp}.html")
    tool_log    = []

    def append_log(icon, title, detail=""):
        tool_log.append({"icon": icon, "title": title, "detail": detail})
        with log_container:
            for entry in tool_log:
                with st.expander(f"{entry['icon']}  {entry['title']}", expanded=False):
                    if entry["detail"]:
                        st.code(entry["detail"], language="json")

    try:
        bq_client     = bigquery.Client(project=project_id)
        fuelix_client = OpenAI(api_key=fuelix_key, base_url="https://proxy.fuelix.ai/")
    except Exception as exc:
        status_container.error(f"Failed to initialize clients: {exc}")
        return None, None

    _s    = build_tool_schemas({"hours_back": hours_back, "top_n_users": top_n_users})
    TOOLS = [_s["get_cost_attribution"], _s["get_user_top_queries"],
             _s["get_most_hit_tables"],  _s["save_html_report"]]

    PROMPT = f"""\
Analyse BigQuery cost attribution for project '{project_id}' over the last {lookback_days} days.

Steps:
1. Call get_cost_attribution to retrieve per-user spend data.
2. Call get_most_hit_tables to find the most frequently accessed tables.
3. Compute two independent user rankings from the attribution data:
   - COST RANK: order users by estimated_cost_usd descending (who spends the most).
   - OPT RANK: order users by avg_bytes_per_query_gb descending (whose individual queries \
are most wasteful, regardless of how many they run).
   Assign each user both rank numbers (1 = worst). A user ranked #1 in both is the \
highest-priority person to talk to.
4. Identify the top 3 users by OPT RANK and call get_user_top_queries for each to \
retrieve their actual SQL.
5. Generate a polished HTML report with the following structure:

   ── Legend box (top of page) ──────────────────────────────────────────────
   Explain all three sections briefly:
   • 💸 Cost Rank - who spends the most overall. Fix these to reduce the bill.
   • 🔧 Opt Rank - whose individual queries are most wasteful (full-table scans, \
missing partition filters, SELECT *). Fixing one query can have outsized impact.
   • 🗂️ Most Hit Tables - which tables are accessed most often. High-frequency tables \
are the best candidates for partitioning, clustering, and materialized views.
   ─────────────────────────────────────────────────────────────────────────

   Section A - Unified Attribution Table:
   - Summary bar: total estimated cost, period, project, pricing note ($5/TB).
   - One table with columns:
       User | 💸 Cost Rank | 🔧 Opt Rank | Queries | Total Cost (USD) | \
Avg GB/Query | Max GB/Query | Slot-ms
   - In the Cost Rank and Opt Rank columns show the numeric rank AND a small coloured \
badge: 🔴 for rank 1-3, 🟡 for 4-6, ⚪ for the rest.
   - If a user is in the top 3 of BOTH rankings, highlight their entire row in a warm \
amber background and append a "⚠️ High Priority" tag next to their name.
   - Sort the table by Cost Rank by default.

   Section B - Optimization Candidates:
   - Heading: "🔧 Users Most in Need of Query Optimization (by Avg GB/Query)".
   - Brief explanation: "These users may not be the biggest total spenders, but their \
individual queries process the most data on average - a sign of missing partition filters, \
SELECT *, or absent WHERE clauses."
   - For each of the top 3 OPT RANK users, a card showing:
       * OPT Rank badge, Cost Rank badge, avg GB/query, total estimated cost
       * Their top queries (preview, GB processed, cost per run, job ID)
       * Specific optimization suggestions drawn from the actual SQL text

   Section C - Most Hit Tables:
   - Heading: "🗂️ Most Frequently Accessed Tables".
   - One sentence noting that associated_cost_usd is a proxy (bytes are summed at the \
job level and may double-count multi-table queries).
   - A styled table with columns:
       Rank | Table (full_table_name) | Queries | Distinct Users | \
Total GB (proxy) | Associated Cost (USD, proxy)
   - Below the table, highlight any table that appears in both the top 5 by query_count \
AND the top 5 by associated_cost_usd - these are the hottest tables and the best \
candidates for partitioning, clustering, or a materialized view cache.
   - Include a short "Recommendations" callout: for each highlighted hot table suggest \
whether partitioning, clustering, or a scheduled materialized view would help most, \
based on its query count vs cost ratio.

6. Call save_html_report with the complete HTML.\
"""

    messages = [
        {"role": "system", "content": "You are a BigQuery cost analyst. You rank users on two independent axes - total spend and per-query wastefulness - and surface both clearly so engineers know whether to focus on cost reduction or query optimization first."},
        {"role": "user",   "content": PROMPT},
    ]

    html_content = None
    iterations   = 0
    max_iter     = 30

    while iterations < max_iter:
        iterations += 1
        status_container.info(f"Thinking… (step {iterations})")

        try:
            response = fuelix_client.chat.completions.create(
                model=model,
                messages=_safe_messages(messages),
                tools=TOOLS,
                tool_choice="auto",
            )
        except Exception as exc:
            status_container.error(f"Fuel IX API error: {exc}")
            return None, None

        message = response.choices[0].message

        if message.content:
            append_log("💬", "Model response", message.content[:500])

        messages.append(_safe_messages([message])[0])

        if not message.tool_calls:
            status_container.success("Cost attribution complete!")
            break

        for tool_call in message.tool_calls:
            name = tool_call.function.name
            raw  = tool_call.function.arguments

            try:
                inputs = json.loads(raw)
            except json.JSONDecodeError:
                import re
                match = re.search(r'"html_content"\s*:\s*"(.*)', raw, re.DOTALL)
                if match and name == "save_html_report":
                    html_str = match.group(1)
                    for suffix in ['"}', '"', '}']:
                        if html_str.endswith(suffix):
                            html_str = html_str[:-len(suffix)]
                            break
                    html_str = html_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
                    inputs = {"html_content": html_str}
                    append_log("⚠️", "JSON repair applied for save_html_report",
                               f"{len(html_str):,} chars recovered")
                else:
                    result = {"error": "JSON parse failed"}
                    append_log("❌", f"Could not parse tool arguments for {name}")
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(result),
                    })
                    continue

            if name == "get_cost_attribution":
                limit  = inputs.get("limit", top_n_users)
                h_back = inputs.get("hours_back", hours_back)
                status_container.info(f"Fetching cost attribution for top {limit} users…")
                result = get_cost_attribution(bq_client, bq_region, h_back, limit)
                append_log(
                    "💰", f"get_cost_attribution (found {result.get('count', 0)} users)",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "get_user_top_queries":
                user   = inputs.get("user_email", "")
                h_back = inputs.get("hours_back", hours_back)
                limit  = inputs.get("limit", 3)
                status_container.info(f"Fetching top queries for {user}…")
                result = get_user_top_queries(bq_client, bq_region, user, h_back, limit)
                append_log(
                    "🔎", f"get_user_top_queries - {user} ({result.get('count', 0)} queries)",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "get_most_hit_tables":
                limit  = inputs.get("limit", 15)
                h_back = inputs.get("hours_back", hours_back)
                status_container.info(f"Fetching top {limit} most accessed tables…")
                result = get_most_hit_tables(bq_client, bq_region, h_back, limit)
                append_log(
                    "🗂️", f"get_most_hit_tables (found {result.get('count', 0)} tables)",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "save_html_report":
                status_container.info("Saving HTML report…")
                html_body = inputs.get("html_content", "")
                if not html_body.strip():
                    result = {"error": "html_content was empty - please regenerate the full HTML report"}
                    append_log("⚠️", "save_html_report - empty content, asking model to retry")
                else:
                    result       = save_html_report(html_body, output_file)
                    html_content = html_body
                    append_log(
                        "💾", f"save_html_report → {result.get('saved_to', 'unknown')} ({len(html_body):,} chars)",
                        json.dumps(result, indent=2)
                    )

            else:
                result = {"error": f"Unknown tool: {name}"}
                append_log("❌", f"Unknown tool: {name}")

            messages.append({
                "role":         "tool",
                "tool_call_id": tool_call.id,
                "content":      json.dumps(result, default=str),
            })

    return html_content, output_file


def run_optimizer(project_id, bq_region, fuelix_key, model, top_n, lookback_days, log_container, status_container):
    """Run the full agentic optimization loop and stream progress to the UI."""

    from datetime import datetime
    hours_back  = lookback_days * 24
    timestamp   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = os.path.join(os.path.dirname(__file__), f"inefficient_queries_{timestamp}.html")
    tool_log    = []  # collected steps shown in UI

    def append_log(icon, title, detail=""):
        tool_log.append({"icon": icon, "title": title, "detail": detail})
        # Rebuild log display
        with log_container:
            for entry in tool_log:
                with st.expander(f"{entry['icon']}  {entry['title']}", expanded=False):
                    if entry["detail"]:
                        st.code(entry["detail"], language="json")

    # Init clients
    try:
        bq_client = bigquery.Client(project=project_id)
        fuelix_client = OpenAI(api_key=fuelix_key, base_url="https://proxy.fuelix.ai/")
    except Exception as exc:
        status_container.error(f"Failed to initialize clients: {exc}")
        return None, None

    _s    = build_tool_schemas({"hours_back": hours_back, "top_n": top_n})
    TOOLS = [_s["get_inefficient_queries"], _s["dry_run_query"], _s["save_html_report"]]

    PROMPT = f"""\
Find the top {top_n} most inefficient queries in my BigQuery project '{project_id}' \
and suggest specific SQL optimizations for each of them. \
Before finalizing, perform a dry run of each optimized query to validate it and \
retrieve the expected 'Bytes Processed' metrics.

Output the final report as an HTML file. The HTML must include:

1. A REPORT METADATA section at the top showing:
   - Report generated timestamp
   - Project ID and region analyzed
   - Optimizer job ID (from get_inefficient_queries result -> optimizer_stats)
   - Slot-seconds consumed by this optimizer run (optimizer_slot_seconds)
   - Duration of the INFORMATION_SCHEMA scan (optimizer_duration_ms)

2. For each inefficient query:
   - The original query and its execution metrics
   - The suggested optimized SQL
   - Dry run validation results (reduction in bytes processed, % saved)
   - The original Job ID with a 'Copy to Clipboard' button\
"""

    messages = [
        {"role": "system", "content": "You are a BigQuery optimization expert. Use the tools provided to find inefficient queries, optimize them, validate with dry runs, and generate a polished HTML report."},
        {"role": "user",   "content": PROMPT},
    ]

    html_content = None
    iterations   = 0
    max_iter     = 30

    while iterations < max_iter:
        iterations += 1
        status_container.info(f"Thinking... (step {iterations})")

        try:
            response = fuelix_client.chat.completions.create(
                model=model,
                messages=_safe_messages(messages),
                tools=TOOLS,
                tool_choice="auto",
            )
        except Exception as exc:
            status_container.error(f"Fuel IX API error: {exc}")
            return None, None

        message = response.choices[0].message

        if message.content:
            append_log("💬", "Model response", message.content[:500])

        messages.append(_safe_messages([message])[0])

        if not message.tool_calls:
            status_container.success("Optimization complete!")
            break

        for tool_call in message.tool_calls:
            name = tool_call.function.name
            raw  = tool_call.function.arguments

            # ── Parse tool arguments (with JSON repair for large HTML payloads) ─
            try:
                inputs = json.loads(raw)
            except json.JSONDecodeError:
                import re
                match = re.search(r'"html_content"\s*:\s*"(.*)', raw, re.DOTALL)
                if match and name == "save_html_report":
                    html_str = match.group(1)
                    for suffix in ['"}', '"', '}']:
                        if html_str.endswith(suffix):
                            html_str = html_str[:-len(suffix)]
                            break
                    html_str = html_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
                    inputs = {"html_content": html_str}
                    append_log("⚠️", "JSON repair applied for save_html_report",
                               f"{len(html_str):,} chars recovered")
                else:
                    result = {"error": "JSON parse failed"}
                    append_log("❌", f"Could not parse tool arguments for {name}")
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(result),
                    })
                    continue

            # ── Route tool calls ─────────────────────────────────────────
            if name == "get_inefficient_queries":
                limit      = inputs.get("limit", top_n)
                h_back     = inputs.get("hours_back", hours_back)
                status_container.info(f"Fetching top {limit} inefficient queries from BigQuery...")
                result = get_inefficient_queries(bq_client, bq_region, limit, h_back)
                append_log(
                    "🔍", f"get_inefficient_queries (found {result.get('count', 0)})",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "dry_run_query":
                status_container.info("Running dry-run validation...")
                result = dry_run_query(bq_client, inputs["sql"])
                label  = "valid" if result.get("valid") else "FAILED"
                gb     = result.get("estimated_gb", "?")
                append_log(
                    "🧪", f"dry_run_query → {gb} GB estimated [{label}]",
                    json.dumps(result, indent=2)
                )

            elif name == "save_html_report":
                status_container.info("Saving HTML report...")
                html_body = inputs.get("html_content", "")
                if not html_body.strip():
                    result = {"error": "html_content was empty - please regenerate the full HTML report"}
                    append_log("⚠️", "save_html_report - empty content, asking model to retry")
                else:
                    result       = save_html_report(html_body, output_file)
                    html_content = html_body
                    append_log(
                        "💾", f"save_html_report → {result.get('saved_to', 'unknown')} ({len(html_body):,} chars)",
                        json.dumps(result, indent=2)
                    )

            else:
                result = {"error": f"Unknown tool: {name}"}
                append_log("❌", f"Unknown tool: {name}")

            messages.append({
                "role":         "tool",
                "tool_call_id": tool_call.id,
                "content":      json.dumps(result, default=str),
            })

    return html_content, output_file


def get_user_health_scores(bq_client, region, hours_back=168, recent_days=7, limit=20):
    """
    Joins cost attribution and anomaly data in Python and computes a 0-100
    health score per user from three components:
      Cost      (0-40 pts): lower total spend relative to max spender = more pts
      Opt       (0-30 pts): lower avg GB/query relative to worst = more pts
      Anomaly   (0-30 pts): GREEN=30, AMBER=15, NEW=10, RED=0
    Score bands: 80+ Healthy | 60-79 Moderate | 40-59 Needs Attention | <40 Critical
    Users are returned sorted worst-first (lowest score first).
    """
    attribution = get_cost_attribution(bq_client, region, hours_back, limit)
    anomalies   = get_spend_anomalies(bq_client, region, recent_days)

    if attribution.get("error") or anomalies.get("error"):
        errors = [e for e in [attribution.get("error"), anomalies.get("error")] if e]
        return {"error": "; ".join(errors), "scores": [], "count": 0}

    anomaly_map = {a["user_email"]: a for a in anomalies.get("anomalies", [])}
    users       = attribution.get("attributions", [])

    if not users:
        return {"scores": [], "count": 0}

    max_cost   = max(u["estimated_cost_usd"]    for u in users) or 1
    max_avg_gb = max(u["avg_bytes_per_query_gb"] for u in users) or 1

    ANOMALY_PTS = {"GREEN": 30, "AMBER": 15, "RED": 0, "NEW_USER": 10, "NEW_ACTIVITY": 10}

    scored = []
    for u in users:
        anomaly   = anomaly_map.get(u["user_email"], {})
        status    = anomaly.get("status", "GREEN")
        ratio     = anomaly.get("change_ratio")

        cost_pts    = round(40.0 * (1 - u["estimated_cost_usd"]    / max_cost),   1)
        opt_pts     = round(30.0 * (1 - u["avg_bytes_per_query_gb"] / max_avg_gb), 1)
        anomaly_pts = ANOMALY_PTS.get(status, 30)
        score       = round(cost_pts + opt_pts + anomaly_pts, 1)

        if score >= 80:
            label, emoji = "Healthy",        "🟢"
        elif score >= 60:
            label, emoji = "Moderate",       "🟡"
        elif score >= 40:
            label, emoji = "Needs Attention","🟠"
        else:
            label, emoji = "Critical",       "🔴"

        scored.append({
            "user_email":        u["user_email"],
            "query_count":       u["query_count"],
            "cost_usd":          u["estimated_cost_usd"],
            "avg_gb_per_query":  u["avg_bytes_per_query_gb"],
            "anomaly_status":    status,
            "change_ratio":      ratio,
            "cost_component":    cost_pts,
            "opt_component":     opt_pts,
            "anomaly_component": anomaly_pts,
            "health_score":      score,
            "health_label":      label,
            "health_emoji":      emoji,
        })

    scored.sort(key=lambda x: x["health_score"])   # worst first
    return {"scores": scored, "count": len(scored), "score_range": {"max": 100, "healthy_threshold": 80, "moderate_threshold": 60, "attention_threshold": 40}}


def run_supervisor(project_id, bq_region, fuelix_key, model, lookback_days, top_n, top_n_users, log_container, status_container):
    """
    Supervisor agent: collects data from all three sub-domains, cross-references
    findings, and generates a unified BigQuery Health Dashboard report.
    """
    from datetime import datetime
    hours_back  = lookback_days * 24
    timestamp   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = os.path.join(os.path.dirname(__file__), f"bq_health_dashboard_{timestamp}.html")
    tool_log    = []

    def append_log(icon, title, detail=""):
        tool_log.append({"icon": icon, "title": title, "detail": detail})
        with log_container:
            for entry in tool_log:
                with st.expander(f"{entry['icon']}  {entry['title']}", expanded=False):
                    if entry["detail"]:
                        st.code(entry["detail"], language="json")

    try:
        bq_client     = bigquery.Client(project=project_id)
        fuelix_client = OpenAI(api_key=fuelix_key, base_url="https://proxy.fuelix.ai/")
    except Exception as exc:
        status_container.error(f"Failed to initialize clients: {exc}")
        return None, None

    _s    = build_tool_schemas({
        "hours_back":    hours_back,
        "top_n":         top_n,
        "top_n_users":   top_n_users,
        "lookback_days": lookback_days,
    })
    TOOLS = [_s[t] for t in [
        "get_inefficient_queries", "get_cost_attribution", "get_most_hit_tables",
        "get_spend_anomalies",     "get_user_top_queries", "get_user_health_scores",
        "save_html_report",
    ]]

    PROMPT = f"""\
You are the supervisor agent for BigQuery project '{project_id}'. \
Your job is to run a full health assessment by collecting data from all three \
sub-domains, cross-referencing findings, and producing a single unified \
"BigQuery Health Dashboard" report.

--- DATA COLLECTION PHASE ---
Call all five data tools to build a complete picture:
1. get_inefficient_queries    - find the top {top_n} most expensive queries
2. get_cost_attribution       - get per-user spend and efficiency metrics
3. get_most_hit_tables        - find the hottest tables
4. get_spend_anomalies        - detect week-over-week spend spikes
5. get_user_health_scores     - compute a 0-100 health score per user \
(joins attribution + anomaly automatically; returns users sorted worst-first)

--- CROSS-REFERENCE PHASE ---
After collecting all data, perform these cross-references:

A) HIGH-RISK USERS: Find users whose health_score < 40 (Critical) OR who \
appear in TWO OR MORE risk categories (top cost, top avg GB/query, RED/AMBER anomaly). \
For each high-risk user, call get_user_top_queries to retrieve their actual SQL.

B) HOT TABLE × EXPENSIVE QUERY OVERLAP: Find tables that appear in both \
get_most_hit_tables results AND in the text of the top expensive queries. \
These have the highest optimization leverage.

C) SILENT HEAVY USERS: Users with health_score 40-60 and GREEN anomaly status - \
consistently expensive but stable. Flag for flat-rate reservation consideration.

--- REPORT STRUCTURE ---
Generate a polished, styled HTML report with these sections:

1. HEALTH SCORECARD (top of page)
   A summary banner with FIVE metric cards:
   - Total estimated cost (USD) for the period
   - Number of users analysed
   - Average health score across all users (with colour: red if <60, amber if <80, green if 80+)
   - Number of Critical users (score < 40)
   - Number of hot tables identified
   Include the period analysed and project ID.

2. PRIORITY ACTIONS  🚨
   A ranked action list (highest impact first) based on cross-references.
   Each action item must include:
   - Severity badge (🔴 Critical / 🟡 High / 🔵 Medium)
   - Who or what is affected
   - Why it was flagged (cross-reference + health score)
   - Specific recommended action

3. USER LEADERBOARD  👥  ← health score is the primary column
   One table row per user, sorted by health_score ascending (worst first):
   🏥 Score | Label | User | Cost (USD) | Avg GB/Query | Anomaly | \
💸 Cost pts | 🔧 Opt pts | 📈 Anomaly pts
   - Render the score as a visual progress bar (HTML div with width = score%)
     coloured: red 0-39, orange 40-59, yellow 60-79, green 80-100
   - Add a tooltip or small breakdown showing the three component scores \
(e.g. "💸 18 + 🔧 22 + 📈 0 = 40")
   - Rows with score < 40: red background
   - Rows with score 40-59: orange background
   - Rows with score 60-79: yellow background
   - Rows with score 80+: green background
   - For Critical/Needs Attention users: show their top queries inline below the row

4. TABLE HEAT MAP  🗂️
   Rank | Table | Queries | Users | Assoc. Cost | In Expensive Queries?
   Flag tables found in both hot-tables and expensive query text.

5. SUB-AGENT SUMMARIES
   Three compact cards:
   - Optimizer: top query GB, potential savings
   - Attribution: biggest spender and cost share %
   - Anomaly: RED/AMBER count, highest spike ratio

6. RECOMMENDATIONS  💡
   Unified, prioritised list grouped by theme:
   Cost Reduction | Query Optimization | Table Design | Governance

Call save_html_report with the complete HTML when done.\
"""

    messages = [
        {
            "role": "system",
            "content": (
                "You are a BigQuery supervisor agent. You have access to data from three "
                "specialised sub-domains: query optimisation, cost attribution, and anomaly detection. "
                "Your value is in cross-referencing these domains to surface insights no single "
                "sub-agent can find alone. Be systematic: collect all data first, then cross-reference, "
                "then generate the report."
            )
        },
        {"role": "user", "content": PROMPT},
    ]

    html_content = None
    iterations   = 0
    max_iter     = 40

    while iterations < max_iter:
        iterations += 1
        status_container.info(f"Thinking… (step {iterations})")

        try:
            response = fuelix_client.chat.completions.create(
                model=model,
                messages=_safe_messages(messages),
                tools=TOOLS,
                tool_choice="auto",
            )
        except Exception as exc:
            status_container.error(f"Fuel IX API error: {exc}")
            return None, None

        message = response.choices[0].message

        if message.content:
            append_log("💬", "Model response", message.content[:500])

        messages.append(_safe_messages([message])[0])

        if not message.tool_calls:
            status_container.success("Health dashboard complete!")
            break

        for tool_call in message.tool_calls:
            name = tool_call.function.name
            raw  = tool_call.function.arguments

            try:
                inputs = json.loads(raw)
            except json.JSONDecodeError:
                import re
                match = re.search(r'"html_content"\s*:\s*"(.*)', raw, re.DOTALL)
                if match and name == "save_html_report":
                    html_str = match.group(1)
                    for suffix in ['"}', '"', '}']:
                        if html_str.endswith(suffix):
                            html_str = html_str[:-len(suffix)]
                            break
                    html_str = html_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
                    inputs = {"html_content": html_str}
                    append_log("⚠️", "JSON repair applied for save_html_report",
                               f"{len(html_str):,} chars recovered")
                else:
                    result = {"error": "JSON parse failed"}
                    append_log("❌", f"Could not parse tool arguments for {name}")
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(result),
                    })
                    continue

            if name == "get_inefficient_queries":
                limit  = inputs.get("limit", top_n)
                h_back = inputs.get("hours_back", hours_back)
                status_container.info(f"[Health Supervisor] Fetching top {limit} expensive queries…")
                result = get_inefficient_queries(bq_client, bq_region, limit, h_back)
                append_log(
                    "🔍", f"get_inefficient_queries - {result.get('count', 0)} queries",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "get_cost_attribution":
                limit  = inputs.get("limit", top_n_users)
                h_back = inputs.get("hours_back", hours_back)
                status_container.info(f"[Health Supervisor] Fetching cost attribution for top {limit} users…")
                result = get_cost_attribution(bq_client, bq_region, h_back, limit)
                append_log(
                    "💰", f"get_cost_attribution - {result.get('count', 0)} users",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "get_most_hit_tables":
                limit  = inputs.get("limit", 15)
                h_back = inputs.get("hours_back", hours_back)
                status_container.info(f"[Health Supervisor] Fetching top {limit} hot tables…")
                result = get_most_hit_tables(bq_client, bq_region, h_back, limit)
                append_log(
                    "🗂️", f"get_most_hit_tables - {result.get('count', 0)} tables",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "get_spend_anomalies":
                r_days = inputs.get("recent_days", lookback_days)
                status_container.info(f"[Health Supervisor] Comparing last {r_days} days vs prior {r_days} days…")
                result = get_spend_anomalies(bq_client, bq_region, r_days)
                flagged = [r for r in result.get("anomalies", []) if r["status"] in ("RED", "AMBER", "NEW_USER", "NEW_ACTIVITY")]
                append_log(
                    "🚨", f"get_spend_anomalies - {result.get('count', 0)} users, {len(flagged)} flagged",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "get_user_top_queries":
                user   = inputs.get("user_email", "")
                h_back = inputs.get("hours_back", hours_back)
                limit  = inputs.get("limit", 3)
                status_container.info(f"[Health Supervisor] Drilling into queries for {user}…")
                result = get_user_top_queries(bq_client, bq_region, user, h_back, limit)
                append_log(
                    "🔎", f"get_user_top_queries - {user} ({result.get('count', 0)} queries)",
                    json.dumps(result, indent=2, default=str)[:2000]
                )

            elif name == "get_user_health_scores":
                limit       = inputs.get("limit", top_n_users)
                h_back      = inputs.get("hours_back", hours_back)
                r_days      = inputs.get("recent_days", lookback_days)
                status_container.info(f"[Health Supervisor] Computing health scores for top {limit} users…")
                result = get_user_health_scores(bq_client, bq_region, h_back, r_days, limit)
                critical = sum(1 for s in result.get("scores", []) if s["health_label"] == "Critical")
                attention = sum(1 for s in result.get("scores", []) if s["health_label"] == "Needs Attention")
                append_log(
                    "🏥", f"get_user_health_scores - {result.get('count', 0)} users scored "
                          f"({critical} Critical, {attention} Needs Attention)",
                    json.dumps(result, indent=2, default=str)[:3000]
                )

            elif name == "save_html_report":
                status_container.info("[Health Supervisor] Saving health dashboard…")
                html_body = inputs.get("html_content", "")
                if not html_body.strip():
                    result = {"error": "html_content was empty - please regenerate the full HTML report"}
                    append_log("⚠️", "save_html_report - empty content, asking model to retry")
                else:
                    result       = save_html_report(html_body, output_file)
                    html_content = html_body
                    append_log(
                        "💾", f"save_html_report → {result.get('saved_to', 'unknown')} ({len(html_body):,} chars)",
                        json.dumps(result, indent=2)
                    )

            else:
                result = {"error": f"Unknown tool: {name}"}
                append_log("❌", f"Unknown tool: {name}")

            messages.append({
                "role":         "tool",
                "tool_call_id": tool_call.id,
                "content":      json.dumps(result, default=str),
            })

    return html_content, output_file


# ── Storage Advisor agent ──────────────────────────────────────────────────────
def run_storage_advisor(project_id, bq_region, fuelix_key, model, lookback_days, log_container, status_container):
    """Storage cost advisor: finds cold tables, partition violations, wildcard scans and recommends GCS archiving."""

    from datetime import datetime
    hours_back  = lookback_days * 24
    timestamp   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = os.path.join(os.path.dirname(__file__), f"storage_advisor_{timestamp}.html")
    tool_log    = []

    def append_log(icon, title, detail=""):
        tool_log.append({"icon": icon, "title": title, "detail": detail})
        with log_container:
            for entry in tool_log:
                with st.expander(f"{entry['icon']}  {entry['title']}", expanded=False):
                    if entry["detail"]:
                        st.code(entry["detail"], language="json")

    try:
        bq_client     = bigquery.Client(project=project_id)
        fuelix_client = OpenAI(api_key=fuelix_key, base_url="https://proxy.fuelix.ai/")
    except Exception as exc:
        status_container.error(f"Failed to initialize clients: {exc}")
        return None, None

    _s    = build_tool_schemas({"hours_back": hours_back, "lookback_days": lookback_days})
    TOOLS = [_s[t] for t in [
        "get_table_storage_stats", "get_cold_tables",
        "get_partition_filter_violations", "get_wildcard_scan_queries",
        "get_most_hit_tables", "save_html_report",
    ]]

    PROMPT = f"""\
You are a BigQuery storage cost advisor for project '{project_id}'.
Your job is to analyse storage costs and query anti-patterns, then produce
a Storage Optimisation Report showing where money is being wasted and how to fix it.

--- DATA COLLECTION ---
Call all four data tools:
1. get_table_storage_stats   - find the largest tables and their storage costs
2. get_cold_tables           - find tables not queried in {lookback_days}+ days (GCS archive candidates)
3. get_partition_filter_violations - find expensive queries missing date filters (full-table scans)
4. get_wildcard_scan_queries - find queries using table_* patterns scanning historical data
5. get_most_hit_tables       - find the most accessed tables (these should STAY in BigQuery)

--- REPORT STRUCTURE ---
Generate a polished HTML report with:

1. SAVINGS SUMMARY (top banner)
   - Total storage cost across analysed tables (USD/month)
   - Estimated annual savings if cold tables moved to GCS Archive
   - Number of partition filter violations found
   - Number of wildcard scan queries found
   - Project ID and analysis period

2. COLD TABLE ARCHIVE CANDIDATES 🧊
   - Table ranked by annual_savings_if_archived_usd descending
   - Columns: Table | Size (GB) | Days Since Last Query | Monthly Cost | Annual Savings -> Archive
   - For each table: ready-to-run bq extract command:
     bq extract --destination_format=PARQUET 'project:dataset.table' 'gs://your-archive-bucket/dataset/table/*.parquet'
   - Recommended GCS storage class per table:
     * Never queried in 180 days -> Archive ($0.0012/GB)
     * 90-180 days cold -> Coldline ($0.004/GB)
   - Total potential annual savings callout box

3. QUERY ANTI-PATTERNS 🚨
   Section A - Missing Partition Filters
   - Table of violations: User | GB Scanned | Cost | Query Preview
   - Explanation: "These queries scan the entire table because no date filter limits which partitions are read"
   - Fix: show example of adding WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

   Section B - Wildcard Table Scans
   - Table: User | Pattern | GB Scanned | Cost | Query Preview
   - Explanation: "This pattern scans ALL tables matching the wildcard, including years of history"
   - Fix: restrict with _TABLE_SUFFIX filter

4. STORAGE BREAKDOWN 💾
   - Top tables by size with active vs long-term storage class
   - Highlight tables already on long-term pricing (not modified in 90 days) - these are free wins
   - Cross-reference: hot tables (from get_most_hit_tables) should stay in BigQuery

5. RECOMMENDATIONS 💡
   Prioritised actions:
   Critical - largest cold tables (> $100/month wasted)
   High - partition filter violations (fix SQL, immediate savings)
   Medium - wildcard scans (restrict with _TABLE_SUFFIX)
   Low - tables already on long-term storage (no action needed)

Call save_html_report with the complete HTML when done.\
"""

    messages = [
        {
            "role": "system",
            "content": (
                "You are a BigQuery storage cost advisor. "
                "Identify wasted storage and query inefficiencies, quantify the savings, "
                "and give engineers specific, actionable steps to fix them. "
                "Always include ready-to-run commands."
            )
        },
        {"role": "user", "content": PROMPT},
    ]

    html_content = None
    iterations   = 0
    max_iter     = 25

    while iterations < max_iter:
        iterations += 1
        status_container.info(f"Thinking… (step {iterations})")

        try:
            response = fuelix_client.chat.completions.create(
                model=model,
                messages=_safe_messages(messages),
                tools=TOOLS,
                tool_choice="auto",
            )
        except Exception as exc:
            status_container.error(f"Fuel IX API error: {exc}")
            return None, None

        message = response.choices[0].message

        if message.content:
            append_log("💬", "Model response", message.content[:500])

        messages.append(_safe_messages([message])[0])

        if not message.tool_calls:
            status_container.success("Storage analysis complete!")
            break

        tool_calls_list = message.tool_calls if hasattr(message, 'tool_calls') else message.get('tool_calls', [])
        for tool_call in (tool_calls_list or []):
            name = tool_call.function.name if hasattr(tool_call, 'function') else tool_call['function']['name']
            raw  = tool_call.function.arguments if hasattr(tool_call, 'function') else tool_call['function']['arguments']
            tc_id = tool_call.id if hasattr(tool_call, 'id') else tool_call['id']

            try:
                inputs = json.loads(raw)
            except json.JSONDecodeError:
                import re
                match = re.search(r'"html_content"\s*:\s*"(.*)', raw, re.DOTALL)
                if match and name == "save_html_report":
                    html_str = match.group(1)
                    for suffix in ['"}', '"', '}']:
                        if html_str.endswith(suffix):
                            html_str = html_str[:-len(suffix)]
                            break
                    html_str = html_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
                    inputs = {"html_content": html_str}
                    append_log("⚠️", "JSON repair applied", f"{len(html_str):,} chars recovered")
                else:
                    result = {"error": "JSON parse failed"}
                    append_log("❌", f"Could not parse tool arguments for {name}")
                    messages.append({"role": "tool", "tool_call_id": tc_id, "content": json.dumps(result)})
                    continue

            try:
                if name == "get_table_storage_stats":
                    min_gb = inputs.get("min_gb", 0.1)
                    limit  = inputs.get("limit", 25)
                    status_container.info(f"Fetching storage stats (min {min_gb} GB)…")
                    result = get_table_storage_stats(bq_client, project_id, bq_region, min_gb, limit)
                    append_log("💾", f"get_table_storage_stats - {result.get('count', 0)} tables, "
                                     f"${result.get('total_monthly_cost_usd', 0):.2f}/month",
                               json.dumps(result, indent=2, default=str)[:2000])

                elif name == "get_cold_tables":
                    min_days = inputs.get("min_days_unqueried", lookback_days)
                    min_gb   = inputs.get("min_gb", 5)
                    limit    = inputs.get("limit", 20)
                    status_container.info(f"Finding tables cold for {min_days}+ days…")
                    result = get_cold_tables(bq_client, project_id, bq_region, min_days, min_gb, limit)
                    append_log("🧊", f"get_cold_tables - {result.get('count', 0)} cold tables, "
                                     f"${result.get('total_annual_savings_if_all_archived_usd', 0):,.2f} potential annual savings",
                               json.dumps(result, indent=2, default=str)[:2000])

                elif name == "get_partition_filter_violations":
                    h_back = inputs.get("hours_back", hours_back)
                    limit  = inputs.get("limit", 10)
                    status_container.info("Scanning for missing partition filters…")
                    result = get_partition_filter_violations(bq_client, bq_region, h_back, limit)
                    append_log("🚨", f"get_partition_filter_violations - {result.get('count', 0)} violations, "
                                     f"${result.get('total_wasted_cost_usd', 0):.4f} wasted",
                               json.dumps(result, indent=2, default=str)[:2000])

                elif name == "get_wildcard_scan_queries":
                    h_back = inputs.get("hours_back", hours_back)
                    limit  = inputs.get("limit", 10)
                    status_container.info("Scanning for wildcard table queries…")
                    result = get_wildcard_scan_queries(bq_client, bq_region, h_back, limit)
                    append_log("🃏", f"get_wildcard_scan_queries - {result.get('count', 0)} queries, "
                                     f"${result.get('total_cost_usd', 0):.4f} total cost",
                               json.dumps(result, indent=2, default=str)[:2000])

                elif name == "get_most_hit_tables":
                    limit  = inputs.get("limit", 15)
                    h_back = inputs.get("hours_back", hours_back)
                    status_container.info("Fetching most accessed tables…")
                    result = get_most_hit_tables(bq_client, bq_region, h_back, limit)
                    append_log("🗂️", f"get_most_hit_tables - {result.get('count', 0)} tables",
                               json.dumps(result, indent=2, default=str)[:2000])

                elif name == "save_html_report":
                    status_container.info("Saving storage report…")
                    html_body = inputs.get("html_content", "")
                    if not html_body.strip():
                        result = {"error": "html_content was empty - please regenerate"}
                        append_log("⚠️", "save_html_report - empty content, retrying")
                    else:
                        result       = save_html_report(html_body, output_file)
                        html_content = html_body
                        append_log("💾", f"save_html_report -> {result.get('saved_to', 'unknown')} ({len(html_body):,} chars)",
                                   json.dumps(result, indent=2))

                else:
                    result = {"error": f"Unknown tool: {name}"}
                    append_log("❌", f"Unknown tool: {name}")

            except Exception as tool_exc:
                result = {"error": f"Tool execution failed: {tool_exc}"}
                append_log("❌", f"{name} failed", str(tool_exc))
                status_container.error(f"Tool error ({name}): {tool_exc}")

            messages.append({
                "role":         "tool",
                "tool_call_id": tc_id,
                "content":      json.dumps(result, default=str),
            })

    if html_content is None:
        status_container.error(f"Loop ended after {iterations} steps without producing a report.")
    return html_content, output_file


# ── Access control helper ──────────────────────────────────────────────────────
def revoke_table_access(bq_client, project_id, dataset_id, table_id, user_email, dry_run=True):
    """
    Remove user_email from all IAM bindings on a specific BigQuery table.
    dry_run=True  → preview only, no changes applied.
    dry_run=False → changes applied immediately.
    """
    try:
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        policy    = bq_client.get_iam_policy(table_ref)

        removed_from = []
        new_bindings = []

        for binding in policy.bindings:
            members_before = set(binding.get("members", []))
            members_after  = members_before - {
                f"user:{user_email}",
                f"serviceAccount:{user_email}",
            }
            if members_before != members_after:
                removed_from.append(binding["role"])
            if members_after:
                new_bindings.append({
                    "role":    binding["role"],
                    "members": list(members_after),
                })

        if not removed_from:
            return {
                "status":  "no_change",
                "message": (
                    f"{user_email} has no direct table-level IAM access on "
                    f"{dataset_id}.{table_id}. "
                    "They may still have access via dataset or project-level roles."
                ),
                "dry_run": dry_run,
            }

        if not dry_run:
            policy.bindings = new_bindings
            bq_client.set_iam_policy(table_ref, policy)

        return {
            "status":        "revoked" if not dry_run else "preview",
            "user_email":    user_email,
            "dataset":       dataset_id,
            "table":         table_id,
            "roles_removed": removed_from,
            "dry_run":       dry_run,
            "message": (
                f"{'[PREVIEW] Would remove' if dry_run else '✅ Removed'} "
                f"{user_email} from {', '.join(removed_from)} "
                f"on {dataset_id}.{table_id}"
            ),
        }
    except Exception as exc:
        return {"error": str(exc), "status": "error"}


# ── Chat turn ─────────────────────────────────────────────────────────────────
def run_chat_turn(api_history, bq_client, fuelix_client,
                  project_id, bq_region, lookback_days, top_n, top_n_users):
    """
    Run one conversational turn against the BigQuery data tools.

    api_history  - OpenAI-format list of all prior turns (no system message).
                   The latest user message must already be appended before calling.
    Returns: (assistant_text, updated_api_history, tool_calls_log)
    """
    hours_back = lookback_days * 24

    _s         = build_tool_schemas({
        "hours_back":    hours_back,
        "top_n":         top_n,
        "top_n_users":   top_n_users,
        "lookback_days": lookback_days,
    })
    CHAT_TOOLS = [_s[t] for t in [
        "get_inefficient_queries", "get_cost_attribution",  "get_most_hit_tables",
        "get_spend_anomalies",     "get_user_top_queries",  "get_user_health_scores",
        "preview_revoke_access",   "execute_revoke_access",
        "get_table_storage_stats", "get_cold_tables",
        "get_partition_filter_violations", "get_wildcard_scan_queries",
    ]]

    system_msg = {
        "role": "system",
        "content": (
            f"You are a BigQuery analyst assistant for project '{project_id}' "
            f"(region: {bq_region}). "
            "Answer questions concisely using Markdown - tables, bold, bullet points. "
            "Call the minimum tools needed. Never generate HTML. "
            "Prices are $5/TB on-demand.\n\n"
            "ACCESS REVOCATION RULES (follow exactly):\n"
            "1. When asked to block/revoke/remove a user's table access, "
            "always call preview_revoke_access first.\n"
            "2. Present the preview result clearly, then ask: "
            "'Type **CONFIRM** to apply this change or **CANCEL** to abort.'\n"
            "3. Only call execute_revoke_access if the user's next message "
            "explicitly contains the word CONFIRM.\n"
            "4. If the user says CANCEL or anything else, acknowledge and do nothing."
        )
    }

    # Prepend system message for the API call (not stored in session state)
    messages = [system_msg] + list(api_history)

    tool_calls_log = []
    assistant_text  = ""

    for _ in range(15):
        try:
            response = fuelix_client.chat.completions.create(
                model=model,
                messages=_safe_messages(messages),
                tools=CHAT_TOOLS,
                tool_choice="auto",
            )
        except Exception as exc:
            return f"API error: {exc}", api_history, tool_calls_log

        message = response.choices[0].message

        # Serialise to dict so it can be stored in session state
        msg_dict: dict = {"role": message.role, "content": message.content or ""}
        if message.tool_calls:
            msg_dict["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                }
                for tc in message.tool_calls
            ]
        messages.append(_ascii_safe(msg_dict))

        if not message.tool_calls:
            assistant_text = message.content or ""
            break

        for tool_call in message.tool_calls:
            name = tool_call.function.name
            try:
                inputs = json.loads(tool_call.function.arguments)
            except json.JSONDecodeError:
                inputs = {}

            result: dict = {}
            icon    = "🔧"
            summary = name

            if name == "get_inefficient_queries":
                limit  = inputs.get("limit", top_n)
                h_back = inputs.get("hours_back", hours_back)
                result  = get_inefficient_queries(bq_client, bq_region, limit, h_back)
                icon, summary = "🔍", f"{result.get('count', 0)} queries"

            elif name == "get_cost_attribution":
                limit  = inputs.get("limit", top_n_users)
                h_back = inputs.get("hours_back", hours_back)
                result  = get_cost_attribution(bq_client, bq_region, h_back, limit)
                icon, summary = "💰", f"{result.get('count', 0)} users"

            elif name == "get_most_hit_tables":
                limit  = inputs.get("limit", 15)
                h_back = inputs.get("hours_back", hours_back)
                result  = get_most_hit_tables(bq_client, bq_region, h_back, limit)
                icon, summary = "🗂️", f"{result.get('count', 0)} tables"

            elif name == "get_spend_anomalies":
                r_days  = inputs.get("recent_days", lookback_days)
                result  = get_spend_anomalies(bq_client, bq_region, r_days)
                flagged = [r for r in result.get("anomalies", []) if r["status"] in ("RED", "AMBER", "NEW_USER")]
                icon, summary = "🚨", f"{result.get('count', 0)} users, {len(flagged)} flagged"

            elif name == "get_user_top_queries":
                user   = inputs.get("user_email", "")
                h_back = inputs.get("hours_back", hours_back)
                limit  = inputs.get("limit", 3)
                result  = get_user_top_queries(bq_client, bq_region, user, h_back, limit)
                icon, summary = "🔎", f"{user} ({result.get('count', 0)} queries)"

            elif name == "get_user_health_scores":
                limit  = inputs.get("limit", top_n_users)
                h_back = inputs.get("hours_back", hours_back)
                r_days = inputs.get("recent_days", lookback_days)
                result  = get_user_health_scores(bq_client, bq_region, h_back, r_days, limit)
                icon, summary = "🏥", f"{result.get('count', 0)} users scored"

            elif name == "preview_revoke_access":
                user    = inputs.get("user_email", "")
                dataset = inputs.get("dataset_id", "")
                table   = inputs.get("table_id", "")
                result  = revoke_table_access(bq_client, project_id, dataset, table, user, dry_run=True)
                icon, summary = "👁️", f"preview: {user} on {dataset}.{table}"

            elif name == "execute_revoke_access":
                user    = inputs.get("user_email", "")
                dataset = inputs.get("dataset_id", "")
                table   = inputs.get("table_id", "")
                result  = revoke_table_access(bq_client, project_id, dataset, table, user, dry_run=False)
                icon, summary = "🔒", f"revoked: {user} from {dataset}.{table}"

            elif name == "get_table_storage_stats":
                min_gb = inputs.get("min_gb", 0.1)
                limit  = inputs.get("limit", 25)
                result  = get_table_storage_stats(bq_client, project_id, bq_region, min_gb, limit)
                icon, summary = "💾", f"{result.get('count', 0)} tables, ${result.get('total_monthly_cost_usd', 0):.2f}/month"

            elif name == "get_cold_tables":
                min_days = inputs.get("min_days_unqueried", lookback_days)
                min_gb   = inputs.get("min_gb", 5)
                limit    = inputs.get("limit", 20)
                result   = get_cold_tables(bq_client, project_id, bq_region, min_days, min_gb, limit)
                icon, summary = "🧊", f"{result.get('count', 0)} cold tables, ${result.get('total_annual_savings_if_all_archived_usd', 0):,.2f} annual savings"

            elif name == "get_partition_filter_violations":
                h_back = inputs.get("hours_back", hours_back)
                limit  = inputs.get("limit", 10)
                result  = get_partition_filter_violations(bq_client, bq_region, h_back, limit)
                icon, summary = "🚨", f"{result.get('count', 0)} violations, ${result.get('total_wasted_cost_usd', 0):.4f} wasted"

            elif name == "get_wildcard_scan_queries":
                h_back = inputs.get("hours_back", hours_back)
                limit  = inputs.get("limit", 10)
                result  = get_wildcard_scan_queries(bq_client, bq_region, h_back, limit)
                icon, summary = "🃏", f"{result.get('count', 0)} wildcard queries"

            else:
                result = {"error": f"Unknown tool: {name}"}

            tool_calls_log.append({"icon": icon, "name": name, "summary": summary})

            messages.append({
                "role":         "tool",
                "tool_call_id": tool_call.id,
                "content":      json.dumps(result, default=str),
            })

    # Strip the system message we prepended - return only the new turns
    updated_api_history = messages[1:]
    return assistant_text, updated_api_history, tool_calls_log


# ── Main UI ───────────────────────────────────────────────────────────────────
st.title("BigQuery Intelligence Suite")
st.markdown(
    "AI-powered tools for BigQuery optimisation and cost attribution."
)

# Validation
ready = bool(project_id and bq_region and fuelix_key)
if not ready:
    st.warning("Fill in **GCP Project ID** and **Fuel IX API Key** in the sidebar to get started.")

col_main, col_chat = st.columns([3, 1])

with col_main:
    tab_opt, tab_cost, tab_anomaly, tab_super, tab_storage = st.tabs([
        "⚡ Query Optimizer", "💰 Cost Attribution",
        "🚨 Anomaly Detector", "🧠 Health Supervisor", "💾 Storage Advisor"
    ])

# ── Tab 1: Query Optimizer ─────────────────────────────────────────────────────
with tab_opt:
    st.subheader("⚡ Query Optimizer")
    st.markdown(
        "Finds your most expensive BigQuery queries, suggests SQL optimizations, "
        "validates them with dry runs, and produces a downloadable HTML report."
    )

    run_clicked = st.button("🚀 Run Optimization", disabled=not ready, use_container_width=False)

    if run_clicked:
        st.divider()
        status_box = st.empty()
        st.subheader("Live Progress")
        log_box = st.container()

        start = time.time()
        html, path = run_optimizer(
            project_id, bq_region, fuelix_key, model,
            top_n, lookback_days, log_box, status_box
        )
        elapsed = round(time.time() - start, 1)

        st.divider()

        if html and path:
            st.success(f"Done in {elapsed}s - report saved to `{path}`")
            st.download_button(
                label="📥 Download HTML Report",
                data=html,
                file_name="inefficient_queries.html",
                mime="text/html",
            )
            st.subheader("Report Preview")
            st.components.v1.html(html, height=900, scrolling=True)
        else:
            st.error("Optimization did not complete. Check the logs above.")

    else:
        import glob
        reports = sorted(
            glob.glob(os.path.join(os.path.dirname(__file__), "inefficient_queries_*.html")),
            reverse=True
        )
        if reports:
            latest = reports[0]
            st.info(f"Previous report found: `{os.path.basename(latest)}`. Run the optimizer to create a new one.")
            with open(latest, encoding="utf-8") as f:
                prev_html = f.read()
            st.download_button(
                label="📥 Download Previous Report",
                data=prev_html,
                file_name=os.path.basename(latest),
                mime="text/html",
            )
            st.subheader("Previous Report Preview")
            st.components.v1.html(prev_html, height=900, scrolling=True)

# ── Tab 2: Cost Attribution ───────────────────────────────────────────────────
with tab_cost:
    st.subheader("💰 Cost Attribution")
    st.markdown(
        "Breaks down BigQuery spend by user/team, identifying who runs the most "
        "expensive queries and their estimated on-demand costs ($5/TB)."
    )

    attr_clicked = st.button("💰 Run Cost Attribution", disabled=not ready, use_container_width=False)

    if attr_clicked:
        st.divider()
        attr_status_box = st.empty()
        st.subheader("Live Progress")
        attr_log_box = st.container()

        start = time.time()
        attr_html, attr_path = run_cost_attribution(
            project_id, bq_region, fuelix_key, model,
            lookback_days, top_n_users, attr_log_box, attr_status_box
        )
        elapsed = round(time.time() - start, 1)

        st.divider()

        if attr_html and attr_path:
            st.success(f"Done in {elapsed}s - report saved to `{attr_path}`")
            st.download_button(
                label="📥 Download Cost Attribution Report",
                data=attr_html,
                file_name="cost_attribution.html",
                mime="text/html",
            )
            st.subheader("Report Preview")
            st.components.v1.html(attr_html, height=900, scrolling=True)
        else:
            st.error("Cost attribution did not complete. Check the logs above.")

    else:
        import glob
        attr_reports = sorted(
            glob.glob(os.path.join(os.path.dirname(__file__), "cost_attribution_*.html")),
            reverse=True
        )
        if attr_reports:
            latest_attr = attr_reports[0]
            st.info(f"Previous report found: `{os.path.basename(latest_attr)}`. Run attribution to create a new one.")
            with open(latest_attr, encoding="utf-8") as f:
                prev_attr_html = f.read()
            st.download_button(
                label="📥 Download Previous Report",
                data=prev_attr_html,
                file_name=os.path.basename(latest_attr),
                mime="text/html",
            )
            st.subheader("Previous Report Preview")
            st.components.v1.html(prev_attr_html, height=900, scrolling=True)

# ── Tab 3: Anomaly Detector ───────────────────────────────────────────────────
with tab_anomaly:
    st.subheader("🚨 Anomaly Detector")
    st.markdown(
        "Compares each user's BigQuery spend over the last N days against the equal prior window. "
        "Flags users whose cost doubled (🟡 AMBER) or tripled (🔴 RED), then drills into their "
        "top queries to explain what drove the spike."
    )

    anomaly_clicked = st.button("🚨 Run Anomaly Detection", disabled=not ready, use_container_width=False)

    if anomaly_clicked:
        st.divider()
        anomaly_status_box = st.empty()
        st.subheader("Live Progress")
        anomaly_log_box = st.container()

        start = time.time()
        anomaly_html, anomaly_path = run_anomaly_detector(
            project_id, bq_region, fuelix_key, model,
            lookback_days, anomaly_log_box, anomaly_status_box
        )
        elapsed = round(time.time() - start, 1)

        st.divider()

        if anomaly_html and anomaly_path:
            st.success(f"Done in {elapsed}s - report saved to `{anomaly_path}`")
            st.download_button(
                label="📥 Download Anomaly Report",
                data=anomaly_html,
                file_name="anomaly_report.html",
                mime="text/html",
            )
            st.subheader("Report Preview")
            st.components.v1.html(anomaly_html, height=900, scrolling=True)
        else:
            st.error("Anomaly detection did not complete. Check the logs above.")

    else:
        import glob
        anomaly_reports = sorted(
            glob.glob(os.path.join(os.path.dirname(__file__), "anomaly_report_*.html")),
            reverse=True
        )
        if anomaly_reports:
            latest_anomaly = anomaly_reports[0]
            st.info(f"Previous report found: `{os.path.basename(latest_anomaly)}`. Run detection to create a new one.")
            with open(latest_anomaly, encoding="utf-8") as f:
                prev_anomaly_html = f.read()
            st.download_button(
                label="📥 Download Previous Report",
                data=prev_anomaly_html,
                file_name=os.path.basename(latest_anomaly),
                mime="text/html",
            )
            st.subheader("Previous Report Preview")
            st.components.v1.html(prev_anomaly_html, height=900, scrolling=True)

# ── Tab 4: Supervisor ─────────────────────────────────────────────────────────
with tab_super:
    st.subheader("🧠 Health Supervisor Agent")
    st.markdown(
        "Runs all three sub-agents, **cross-references their findings**, and produces a single "
        "unified **BigQuery Health Dashboard**. Surfaces insights no individual agent can find alone - "
        "users who are simultaneously the biggest spenders, worst optimizers, *and* anomaly spikes."
    )

    st.info(
        "**How it works:** The supervisor collects data from all four tool domains "
        "(expensive queries, cost attribution, hot tables, spend anomalies), cross-references "
        "users and tables across domains, drills into high-risk users, then generates a "
        "prioritised action report.\n\n"
        "⏱️ Expect 10–20 tool calls and 2–5 minutes to complete.",
        icon="🧠"
    )

    super_clicked = st.button("🧠 Run Health Supervisor Analysis", disabled=not ready, use_container_width=False)

    if super_clicked:
        st.divider()
        super_status_box = st.empty()
        st.subheader("Live Progress")
        super_log_box = st.container()

        start = time.time()
        super_html, super_path = run_supervisor(
            project_id, bq_region, fuelix_key, model,
            lookback_days, top_n, top_n_users,
            super_log_box, super_status_box
        )
        elapsed = round(time.time() - start, 1)

        st.divider()

        if super_html and super_path:
            st.success(f"Done in {elapsed}s - dashboard saved to `{super_path}`")
            st.download_button(
                label="📥 Download Health Dashboard",
                data=super_html,
                file_name="bq_health_dashboard.html",
                mime="text/html",
            )
            st.subheader("Dashboard Preview")
            st.components.v1.html(super_html, height=1000, scrolling=True)
        else:
            st.error("Health Supervisor analysis did not complete. Check the logs above.")

    else:
        import glob
        super_reports = sorted(
            glob.glob(os.path.join(os.path.dirname(__file__), "bq_health_dashboard_*.html")),
            reverse=True
        )
        if super_reports:
            latest_super = super_reports[0]
            st.info(f"Previous dashboard found: `{os.path.basename(latest_super)}`. Run supervisor to create a new one.")
            with open(latest_super, encoding="utf-8") as f:
                prev_super_html = f.read()
            st.download_button(
                label="📥 Download Previous Dashboard",
                data=prev_super_html,
                file_name=os.path.basename(latest_super),
                mime="text/html",
            )
            st.subheader("Previous Dashboard Preview")
            st.components.v1.html(prev_super_html, height=1000, scrolling=True)

# ── Tab 5: Storage Advisor ────────────────────────────────────────────────────
with tab_storage:
    st.subheader("💾 Storage Advisor")
    st.markdown(
        "Finds cold tables wasting money in active storage, query anti-patterns "
        "(missing partition filters, wildcard scans), and produces a prioritised "
        "cost-reduction report with ready-to-run `bq extract` commands."
    )

    st.info(
        "**How it works:** Analyses `INFORMATION_SCHEMA.TABLE_STORAGE` and `JOBS` "
        "to find tables not queried recently, queries scanning full tables due to missing "
        "date filters, and wildcard table scans. Estimates GCS Archive savings (up to 95% cheaper).\n\n"
        "⏱️ Expect 5–10 tool calls and 1–3 minutes to complete.",
        icon="💾"
    )

    storage_clicked = st.button("💾 Run Storage Analysis", disabled=not ready, use_container_width=False)

    if storage_clicked:
        st.divider()
        storage_status_box = st.empty()
        st.subheader("Live Progress")
        storage_log_box = st.container()

        start = time.time()
        storage_html, storage_path = run_storage_advisor(
            project_id, bq_region, fuelix_key, model,
            lookback_days, storage_log_box, storage_status_box
        )
        elapsed = round(time.time() - start, 1)

        st.divider()

        if storage_html and storage_path:
            st.success(f"Done in {elapsed}s - report saved to `{storage_path}`")
            st.download_button(
                label="📥 Download Storage Report",
                data=storage_html,
                file_name="storage_advisor.html",
                mime="text/html",
            )
            st.subheader("Report Preview")
            st.components.v1.html(storage_html, height=900, scrolling=True)
        else:
            st.error("Storage analysis did not complete. Check the logs above.")

    else:
        import glob
        storage_reports = sorted(
            glob.glob(os.path.join(os.path.dirname(__file__), "storage_advisor_*.html")),
            reverse=True
        )
        if storage_reports:
            latest_storage = storage_reports[0]
            st.info(f"Previous report found: `{os.path.basename(latest_storage)}`. Run analysis to create a new one.")
            with open(latest_storage, encoding="utf-8") as f:
                prev_storage_html = f.read()
            st.download_button(
                label="📥 Download Previous Report",
                data=prev_storage_html,
                file_name=os.path.basename(latest_storage),
                mime="text/html",
            )
            st.subheader("Previous Report Preview")
            st.components.v1.html(prev_storage_html, height=900, scrolling=True)

# ── Right panel: Chat ──────────────────────────────────────────────────────────
with col_chat:
    st.subheader("💬 Chat")
    st.caption("Ask anything about your BigQuery usage")

    # Initialise session state once per session
    st.session_state.setdefault("chat_messages", [])      # UI bubbles
    st.session_state.setdefault("chat_api_messages", [])  # Full API history

    if st.button("🗑️ Clear", use_container_width=True):
        st.session_state.chat_messages = []
        st.session_state.chat_api_messages = []
        st.rerun()

    # Render existing chat history
    for msg in st.session_state.chat_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg["role"] == "assistant" and msg.get("tool_calls_log"):
                with st.expander("🔧 Tool calls", expanded=False):
                    for tc in msg["tool_calls_log"]:
                        st.markdown(f"- {tc['icon']} **{tc['name']}** → {tc['summary']}")

    # Chat input
    if not ready:
        st.warning("Add credentials in the sidebar to enable chat.")
    else:
        if prompt := st.chat_input("Ask anything…"):
            # 1. Append user message to both lists
            st.session_state.chat_messages.append({"role": "user", "content": prompt})
            st.session_state.chat_api_messages.append({"role": "user", "content": prompt})

            # 2. Show user bubble immediately
            with st.chat_message("user"):
                st.markdown(prompt)

            # 3. Run agentic turn
            with st.spinner("Querying…"):
                try:
                    _bq  = bigquery.Client(project=project_id)
                    _llm = OpenAI(api_key=fuelix_key, base_url="https://proxy.fuelix.ai/")
                    reply, updated_history, tc_log = run_chat_turn(
                        st.session_state.chat_api_messages,
                        _bq, _llm,
                        project_id, bq_region, lookback_days, top_n, top_n_users,
                    )
                except Exception as exc:
                    reply           = f"Error initialising clients: {exc}"
                    updated_history = st.session_state.chat_api_messages
                    tc_log          = []

            # 4–5. Store results
            st.session_state.chat_messages.append({
                "role":           "assistant",
                "content":        reply,
                "tool_calls_log": tc_log,
            })
            st.session_state.chat_api_messages = updated_history

            # 6. Rerun to render new messages cleanly
            st.rerun()
