# BigQuery Intelligence Suite

An agentic AI-powered Streamlit application for BigQuery cost optimization, anomaly detection, and storage management. Built with multi-agent architecture using LLM orchestration via an OpenAI-compatible API.

---

## Features

### 5 Analysis Tabs

| Tab | Purpose |
|-----|---------|
| ⚡ **Query Optimizer** | Finds your most expensive queries, suggests SQL rewrites, validates savings with BigQuery dry runs, exports HTML report |
| 💰 **Cost Attribution** | Per-user spend breakdown, query counts, avg/max GB scanned, table heat map |
| 🚨 **Anomaly Detector** | Week-over-week spend spike detection per user — flags RED / AMBER / NEW_USER |
| 🧠 **Health Supervisor** | Unified dashboard combining cost, optimization, and anomaly signals into a 0–100 health score per user |
| 💾 **Storage Advisor** | Identifies cold/unqueried tables, partition filter violations, wildcard scans, and estimates GCS archiving savings |

### 💬 Persistent Chat Panel
Natural language interface available on every tab. Ask questions like:
- *"Who is the most expensive user this week?"*
- *"Show me their top 3 queries"*
- *"Any anomalies? Show me a summary"*
- *"Which tables should I archive?"*
- *"Revoke table access for user@example.com"*

Multi-turn conversation is preserved within the session. The chat agent has access to all 12 data tools and supports access revocation with a confirm/cancel safety flow.

---

## Architecture

```
col_main (3/4 width)                col_chat (1/4 width)
  ├─ ⚡ Query Optimizer               💬 Persistent chat panel
  ├─ 💰 Cost Attribution              Multi-turn, session state
  ├─ 🚨 Anomaly Detector              All 12 tools available
  ├─ 🧠 Health Supervisor
  └─ 💾 Storage Advisor
```

### Agents

Each tab runs an independent agentic loop against the Fuel IX LLM proxy, calling BigQuery tool functions and iterating until a final answer is produced.

| Agent | Tools | Max Iterations |
|-------|-------|----------------|
| `run_optimizer()` | get_inefficient_queries, dry_run_query, save_html_report | 25 |
| `run_cost_attribution()` | get_cost_attribution, get_user_top_queries, get_most_hit_tables, save_html_report | 25 |
| `run_anomaly_detector()` | get_spend_anomalies, get_user_top_queries, save_html_report | 25 |
| `run_supervisor()` | all 6 data tools + save_html_report | 25 |
| `run_storage_advisor()` | get_table_storage_stats, get_cold_tables, get_partition_filter_violations, get_wildcard_scan_queries, get_most_hit_tables, save_html_report | 25 |
| `run_chat_turn()` | all 12 tools | 15 |

### Data Sources

All data is queried live from BigQuery `INFORMATION_SCHEMA`:

- `INFORMATION_SCHEMA.JOBS` — query history, costs, users
- `INFORMATION_SCHEMA.TABLE_STORAGE` — table sizes, storage class, modification times

---

## Prerequisites

- Python 3.10+
- A GCP project with BigQuery enabled
- A [Fuel IX](https://dev.fuelix.ai) API key (OpenAI-compatible proxy)
- Application Default Credentials configured locally:
  ```bash
  gcloud auth application-default login
  ```

---

## Installation

```bash
git clone https://github.com/saviothara/bq-intelligence-suite.git
cd bq-intelligence-suite
pip install streamlit google-cloud-bigquery openai
```

---

## Running the App

```bash
streamlit run app.py --server.port 8509
```

Open [http://localhost:8509](http://localhost:8509) in your browser.

### Sidebar Configuration

| Setting | Description |
|---------|-------------|
| GCP Project ID | Your BigQuery project (e.g. `my-project-123`) |
| BQ Region | Dataset region (e.g. `northamerica-northeast1`) |
| Fuel IX API Key | From [dev.fuelix.ai](https://dev.fuelix.ai) |
| Model | LLM model to use (default: `gpt-4o`) |
| Lookback days | How far back to query job history |
| Top N queries | Number of queries to surface per analysis |

---

## BigQuery Tool Functions

### Data Tools

```python
get_inefficient_queries(bq_client, region, limit, hours_back)
get_cost_attribution(bq_client, region, hours_back, limit)
get_most_hit_tables(bq_client, region, hours_back, limit)
get_spend_anomalies(bq_client, region, recent_days)
get_user_top_queries(bq_client, region, user_email, hours_back, limit)
get_user_health_scores(bq_client, region, hours_back, recent_days, limit)
get_table_storage_stats(bq_client, project_id, region, min_gb, limit)
get_cold_tables(bq_client, project_id, region, min_days_unqueried, min_gb, limit)
get_partition_filter_violations(bq_client, region, hours_back, limit)
get_wildcard_scan_queries(bq_client, region, hours_back, limit)
```

### Action Tools

```python
dry_run_query(bq_client, sql)              # Estimate bytes without running
save_html_report(html_content, output_file) # Save report to disk
revoke_table_access(...)                   # IAM access removal (with confirm flow)
```

---

## Seed Scripts

Use these to populate your BigQuery project with test data:

```bash
# Set up base tables (sales, analytics, hr datasets)
python setup_bq.py

# Seed 20 intentionally inefficient queries (anti-patterns 1-20)
python seed_bad_queries.py

# Seed 15 more inefficient queries (anti-patterns 21-35)
python seed_more_bad_queries.py

# Create 17 cold/unqueried tables across archive, staging, finance, logs datasets
python seed_cold_tables.py
```

The seed scripts create realistic data volumes (3k–400k rows per table) and cover a wide range of anti-patterns including:

- `SELECT *` with no partition filter
- Correlated subqueries instead of window functions
- CAST on partition/cluster columns breaking pruning
- CROSS JOINs, large IN lists, repeated aggregation subqueries
- `REGEXP_CONTAINS` and `LIKE '%...%'` full table scans
- Unnecessary `DISTINCT` on primary keys

---

## GCS Archiving Reference

Storage Advisor recommendations are based on BigQuery on-demand pricing ($5/TB) and GCS storage tiers:

| Tier | Price/GB/month | Best for |
|------|---------------|----------|
| BigQuery Active | $0.023 | Hot data (0–90 days) |
| BigQuery Long-term | $0.016 | Unmodified 90+ days (auto) |
| GCS Nearline | $0.010 | Accessed monthly |
| GCS Coldline | $0.004 | Accessed quarterly |
| GCS Archive | $0.0012 | Accessed rarely |

Export to GCS with:
```bash
bq extract --destination_format=PARQUET \
  'project:dataset.table' \
  gs://your-bucket/path/table_*.parquet
```

---

## Known Constraints

- **Fuel IX ASCII encoding** — The proxy encodes request bodies as ASCII. All messages are sanitized through `_safe_messages()` before every API call to prevent encoding errors from model-generated Unicode characters.
- **TABLE_STORAGE region** — Must use `` `project`.`region-REGION`.INFORMATION_SCHEMA.TABLE_STORAGE `` with explicit region qualifier.
- **Cold table detection** — Uses two separate BigQuery queries (TABLE_STORAGE + JOBS) merged in Python, since cross-resource JOINs between these views are not supported.
- **New tables** — TABLE_STORAGE metadata can take ~10 minutes to reflect newly created tables.
- **Query truncation** — `INFORMATION_SCHEMA.JOBS` silently truncates the `query` column at ~1 MB (~900,000 characters). `get_inefficient_queries` detects truncation and recovers the full query text via the BigQuery Jobs API (`bq_client.get_job(job_id).query`), falling back to the truncated version if the API call fails. A `truncated: bool` flag is included in each returned row so the optimizer can acknowledge incomplete SQL rather than generating invalid rewrites.
