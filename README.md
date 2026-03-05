LLM SQL Agent

This project implements a deterministic natural language → SQL pipeline
for querying an analytics database containing fund holdings and returns.

The system converts a user question into a structured Intent, builds
deterministic SQL, executes it safely against Postgres, and returns
structured results.

Key rule: LLMs never generate SQL.

  --------------------------------------------------
                 SYSTEM ARCHITECTURE
  --------------------------------------------------
                  EXECUTION PIPELINE

    User Question │ ▼ POST /intent Intent Resolver
    (rule engine + optional LLM clarification) │ ▼
     Intent │ ▼ POST /build-sql Deterministic SQL
      Builder │ ▼ SQLPlan {sql, params} │ ▼ POST
       /run-sql Query Runner │ ▼ Query Results
  --------------------------------------------------

1.  INTENT RESOLUTION (Intent 2.0)

Location: intent_clarifier/

Purpose: Convert natural language question into structured Intent
object.

Example question: top 10 funds

Resolved intent example:

{ “entity”: “fund”, “metric”: “market_value”, “analysis_type”: “rank”,
“scope”: “universe”, “time_axis”: “report_date”, “time_window”: {
“mode”: “between_dates”, “start_date”: “LAST_COMPLETED_QUARTER_START”,
“end_date”: “LAST_COMPLETED_QUARTER_END” }, “ranking”: { “top_n”: 10 } }

Strategy: Rules first → LLM fallback

Rules detect: - entity - metric - ranking - identifiers - scope - time
window

  --------------------------------------------------
  2. TIME WINDOW POLICY
  --------------------------------------------------
  3. SQL BUILDER

  Location: sql_builder/

  Purpose: Convert Intent → SQLPlan
  deterministically.

  Entry point: build_sql(intent)

  Output:

  SQLPlan ├─ sql └─ params

  Example:

  { “sql”: “SELECT … LIMIT :limit”, “params”:
  {“limit”: 10} }

  Example generated SQL:

  WITH fh_src AS ( SELECT * FROM
  analytics.fact_holding fh )

  SELECT fh.fund_key, df.fund_name,
  SUM(fh.market_value) AS value FROM fh_src fh JOIN
  analytics.dim_fund df ON df.fund_key = fh.fund_key
  WHERE fh.report_date BETWEEN quarter_start AND
  quarter_end GROUP BY fh.fund_key, df.fund_name
  ORDER BY value DESC LIMIT :limit
  --------------------------------------------------

SQL VALIDATION

Before execution SQL is validated.

Checks: - SELECT only - single statement - required LIMIT - allowed
schemas (analytics) - trailing semicolon allowed

Validator: sql_builder/sql_validator.py

  --------------------------------------------------
  4. QUERY RUNNER
  --------------------------------------------------
  DATABASE SCHEMA

  analytics.fact_holding analytics.fact_fund_return
  analytics.dim_fund analytics.dim_security

  fact_holding: fund_key security_key report_date
  market_value weight_pct shares

  fact_fund_return: fund_key month_end total_return

  dim_fund: fund_key fund_name registrant_cik
  series_id class_id

  dim_security: security_key ticker cusip isin
  --------------------------------------------------

LOGGING

Logging exists across the entire pipeline:

intent_clarifier.resolver sql_builder.build sql_builder.time_window
sql_builder.sql_validator api.routes_db

Example logs:

intent.resolve.start sql_builder.start sql_builder.sql db.run_sql.start
db.run_sql.done rows=…

Traceability: question → intent → sql → execution

  --------------------------------------------------
  CURRENT CAPABILITIES
  --------------------------------------------------
  PLANNED FEATURES

  - security ranking - trend queries - peer groups -
  fund comparison - delta analysis - result
  explanations - chart selection - query caching
  --------------------------------------------------

DESIGN PRINCIPLES

Deterministic SQL LLM never generates SQL.

Structured Intent Layer Intent is the semantic bridge between language
and SQL.

Safety SQL validator prevents dangerous queries.

Reproducibility All stages are logged and deterministic.
