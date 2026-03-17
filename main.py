"""
BigQuery Query Optimization using Fuel IX (OpenAI-compatible proxy)
Project : tharalab1-lab-590b81
Region  : northamerica-northeast1
"""

import json
import os
from google.cloud import bigquery
from openai import OpenAI

# ── Configuration ─────────────────────────────────────────────────────────────
GCP_PROJECT    = os.environ.get("GCP_PROJECT", "tharalab1-lab-590b81")
BQ_REGION      = os.environ.get("BQ_REGION",   "northamerica-northeast1")
FUELIX_API_KEY = os.environ.get("FUELIX_API_KEY", "")
FUELIX_MODEL   = os.environ.get("FUELIX_MODEL",   "gpt-4o")
OUTPUT_FILE    = os.path.join(os.path.dirname(__file__), "inefficient_queries.html")

if not FUELIX_API_KEY:
    raise RuntimeError("FUELIX_API_KEY environment variable is not set.")

bq_client = bigquery.Client(project=GCP_PROJECT)

fuelix_client = OpenAI(
    api_key=FUELIX_API_KEY,
    base_url="https://proxy.fuelix.ai/"
)

# ── Tool definitions (OpenAI function-calling format) ─────────────────────────
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_inefficient_queries",
            "description": (
                "Query BigQuery INFORMATION_SCHEMA.JOBS to find the top N most "
                "inefficient queries ranked by total bytes processed. "
                "Returns job_id, query text, total_bytes_processed, total_slot_ms, "
                "creation_time, and user_email for each query."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of top inefficient queries to return (default: 4)"
                    },
                    "hours_back": {
                        "type": "integer",
                        "description": "How many hours of job history to look back (default: 168 = 7 days)"
                    }
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "dry_run_query",
            "description": (
                "Perform a BigQuery dry run to estimate bytes that would be processed "
                "by a SQL query without actually executing it. "
                "Returns estimated_bytes_processed and estimated_gb."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL query to validate via dry run"
                    }
                },
                "required": ["sql"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "save_html_report",
            "description": (
                "Save the final HTML report to inefficient_queries.html. "
                "The HTML must include for each query: original SQL + metrics, "
                "optimized SQL, dry-run byte reduction, and the Job ID with a "
                "Copy-to-Clipboard button."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "html_content": {
                        "type": "string",
                        "description": "Complete, self-contained HTML document to save"
                    }
                },
                "required": ["html_content"]
            }
        }
    }
]

# ── Tool implementations ───────────────────────────────────────────────────────
def get_inefficient_queries(limit: int = 4, hours_back: int = 168) -> dict:
    from datetime import datetime, timezone
    sql = f"""
    SELECT
      job_id,
      query,
      total_bytes_processed,
      total_slot_ms,
      creation_time,
      user_email
    FROM `region-{BQ_REGION}`.INFORMATION_SCHEMA.JOBS
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
        job    = bq_client.query(sql)
        result = job.result()
        rows   = []
        for row in result:
            rows.append({
                "job_id":                   row.job_id,
                "query":                    row.query,
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


def dry_run_query(sql: str) -> dict:
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = bq_client.query(sql, job_config=job_config)
        bytes_processed = job.total_bytes_processed
        return {
            "estimated_bytes_processed": bytes_processed,
            "estimated_gb":              round(bytes_processed / (1024 ** 3), 4),
            "valid":                     True,
        }
    except Exception as exc:
        return {
            "error":                     str(exc),
            "estimated_bytes_processed": None,
            "valid":                     False,
        }


def save_html_report(html_content: str) -> dict:
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
            fh.write(html_content)
        return {"saved_to": OUTPUT_FILE, "success": True}
    except Exception as exc:
        return {"error": str(exc), "success": False}


def execute_tool(name: str, inputs: dict) -> dict:
    dispatch = {
        "get_inefficient_queries": get_inefficient_queries,
        "dry_run_query":           dry_run_query,
        "save_html_report":        save_html_report,
    }
    fn = dispatch.get(name)
    if fn is None:
        return {"error": f"Unknown tool: {name}"}
    return fn(**inputs)


# ── Prompt ────────────────────────────────────────────────────────────────────
PROMPT = """\
Find the top 4 most inefficient queries in my BigQuery environment and suggest \
specific SQL optimizations for each of them. Before finalizing your suggestions, \
perform a dry run of the optimized queries to validate them and retrieve their \
expected 'Bytes Processed' metrics.

Output the final report as an HTML file (inefficient_queries.html). The HTML must include:

1. A REPORT METADATA section at the top showing:
   - Report generated timestamp
   - Project ID and region analyzed
   - Optimizer job ID (from get_inefficient_queries result -> optimizer_stats)
   - Slot-seconds consumed by this optimizer run (optimizer_slot_seconds)
   - Duration of the INFORMATION_SCHEMA scan (optimizer_duration_ms)

2. For each inefficient query:
   - The original query and its execution metrics
   - The suggested optimized SQL
   - The validation results from the dry run (reduction in bytes processed, % saved)
   - The original Job ID with a 'Copy to Clipboard' button\
"""


# ── Agentic loop ──────────────────────────────────────────────────────────────
def main() -> None:
    print(f"BigQuery Query Optimizer")
    print(f"Project : {GCP_PROJECT}  |  Region: {BQ_REGION}  |  Model: {FUELIX_MODEL}")
    print("=" * 70)

    messages = [
        {"role": "system", "content": "You are a BigQuery optimization expert. Use the available tools to find inefficient queries, optimize them, validate with dry runs, and generate an HTML report."},
        {"role": "user",   "content": PROMPT}
    ]

    while True:
        response = fuelix_client.chat.completions.create(
            model=FUELIX_MODEL,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto"
        )

        message = response.choices[0].message

        # Print any text response from the model
        if message.content:
            print(message.content)

        # Append assistant message to history
        messages.append(message)

        # No tool calls -> we are done
        if not message.tool_calls:
            print("\n" + "=" * 70)
            print(f"Report saved -> {OUTPUT_FILE}")
            break

        # Execute each tool call
        for tool_call in message.tool_calls:
            name = tool_call.function.name
            try:
                inputs = json.loads(tool_call.function.arguments)
            except json.JSONDecodeError:
                # Large HTML payloads sometimes arrive with a truncated closing quote/brace.
                # Attempt a simple repair: extract html_content via regex fallback.
                import re
                raw = tool_call.function.arguments
                match = re.search(r'"html_content"\s*:\s*"(.*)', raw, re.DOTALL)
                if match and name == "save_html_report":
                    html_str = match.group(1)
                    # Strip trailing incomplete JSON artifacts
                    for suffix in ['"}', '"', '}']:
                        if html_str.endswith(suffix):
                            html_str = html_str[:-len(suffix)]
                            break
                    # Unescape JSON string sequences
                    html_str = html_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
                    inputs = {"html_content": html_str}
                    print(f"\n[Warn] JSON repair applied for save_html_report ({len(html_str):,} chars)")
                else:
                    print(f"\n[Error] Could not parse tool arguments for {name} — skipping.")
                    messages.append({"role": "tool", "tool_call_id": tool_call.id,
                                     "content": json.dumps({"error": "JSON parse failed"})})
                    continue

            print(f"\n[Tool] {name}")
            print(f"       input : {json.dumps(inputs)[:200]}")

            result  = execute_tool(name, inputs)
            preview = json.dumps(result)
            print(f"       result: {preview[:300]}{'...' if len(preview) > 300 else ''}")

            # Return tool result in OpenAI format
            messages.append({
                "role":         "tool",
                "tool_call_id": tool_call.id,
                "content":      json.dumps(result)
            })


if __name__ == "__main__":
    main()
