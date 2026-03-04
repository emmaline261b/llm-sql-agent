-- sql/analytics_build.sql
-- Build analytics layer from raw_nport (SEC N-PORT)
-- Notes:
-- - raw tables are TEXT-typed; we normalize with NULLIF and casts
-- - we filter out rows that cannot form stable keys (cik/series_id, report_date)
-- - we dedupe dim_security by security_key (DISTINCT ON) because many rows map to same identifier
-- - this is ETL/ELT script (not Alembic migration)

BEGIN;

-- Safety: clean target tables (POC)
TRUNCATE TABLE analytics.fact_holding;
TRUNCATE TABLE analytics.dim_security;
TRUNCATE TABLE analytics.dim_fund;
TRUNCATE TABLE analytics.fact_fund_return;

-- -----------------------
-- 1) dim_fund
-- -----------------------
WITH sub AS (
  SELECT
    accession_number,
    NULLIF(report_date, '')::date AS report_date
  FROM raw_nport.submission
  WHERE NULLIF(report_date, '') IS NOT NULL
),
fund_base AS (
  SELECT
    NULLIF(r.cik, '') AS cik,
    NULLIF(r.registrant_name, '') AS registrant_name,
    NULLIF(r.country, '') AS domicile,
    NULLIF(fri.series_id, '') AS series_id,
    NULLIF(fri.series_name, '') AS series_name,
    s.report_date
  FROM raw_nport.registrant r
  JOIN raw_nport.fund_reported_info fri
    ON fri.accession_number = r.accession_number
  JOIN sub s
    ON s.accession_number = r.accession_number
  WHERE NULLIF(r.cik,'') IS NOT NULL
    AND NULLIF(fri.series_id,'') IS NOT NULL
)
INSERT INTO analytics.dim_fund (
  fund_key, registrant_cik, series_id, fund_name, domicile
)
SELECT
  fb.cik || ':' || fb.series_id AS fund_key,
  fb.cik AS registrant_cik,
  fb.series_id,
  MAX(fb.series_name) AS fund_name,
  MAX(fb.domicile) AS domicile
FROM fund_base fb
GROUP BY fb.cik, fb.series_id;

-- -----------------------
-- 2) dim_security
-- -----------------------
-- Treat placeholder identifiers as NULL (e.g. CUSIP '000000000')
-- Build a stable security_key using priority:
--   ISIN -> CUSIP -> Ticker -> Other identifier -> Holding_id fallback
WITH sec_src AS (
  SELECT
    -- normalized identifiers
    NULLIF(i.identifier_isin,'') AS isin,
    CASE
      WHEN NULLIF(h.issuer_cusip,'') IN ('000000000') THEN NULL
      ELSE NULLIF(h.issuer_cusip,'')
    END AS cusip,
    NULLIF(i.identifier_ticker,'') AS ticker,
    NULLIF(i.other_identifier,'') AS other_identifier,
    NULLIF(h.holding_id,'') AS holding_id,

    NULLIF(h.issuer_title,'') AS security_name,
    NULLIF(h.asset_cat,'') AS asset_category,
    NULLIF(h.currency_code,'') AS currency,
    NULLIF(h.issuer_name,'') AS issuer_name,

    -- issuer country: use investment_country; treat N/A and XX as NULL
    CASE
      WHEN NULLIF(h.investment_country,'') IN ('N/A','XX') THEN NULL
      ELSE NULLIF(h.investment_country,'')
    END AS issuer_country_code

  FROM raw_nport.fund_reported_holding h
  LEFT JOIN raw_nport.identifiers i
    ON i.holding_id = h.holding_id
  WHERE NULLIF(h.holding_id,'') IS NOT NULL
),
sec_keyed AS (
  SELECT
    COALESCE(isin, cusip, ticker, other_identifier, holding_id) AS security_key,
    cusip, isin, ticker, other_identifier,
    security_name, asset_category, currency, issuer_name,
    issuer_country_code,
    -- ranking to choose best representative row for a given security_key
    (CASE WHEN isin IS NOT NULL THEN 4 ELSE 0 END) +
    (CASE WHEN cusip IS NOT NULL THEN 3 ELSE 0 END) +
    (CASE WHEN ticker IS NOT NULL THEN 2 ELSE 0 END) +
    (CASE WHEN other_identifier IS NOT NULL THEN 1 ELSE 0 END) AS key_quality
  FROM sec_src
  WHERE COALESCE(isin, cusip, ticker, other_identifier, holding_id) IS NOT NULL
)
INSERT INTO analytics.dim_security (
  security_key,
  cusip,
  isin,
  ticker,
  security_name,
  asset_category,
  currency,
  issuer_name,
  issuer_country_code
)
SELECT DISTINCT ON (security_key)
  security_key,
  cusip,
  isin,
  ticker,
  security_name,
  asset_category,
  currency,
  issuer_name,
  issuer_country_code
FROM sec_keyed
ORDER BY security_key, key_quality DESC
ON CONFLICT (security_key) DO NOTHING;

-- -----------------------
-- 3) fact_holding
-- -----------------------
WITH fact_src AS (
  SELECT
    (NULLIF(r.cik,'') || ':' || NULLIF(fri.series_id,'')) AS fund_key,
    NULLIF(s.report_date,'')::date AS report_date,
    COALESCE(
      NULLIF(i.identifier_isin,''),
      CASE
        WHEN NULLIF(h.issuer_cusip,'') IN ('000000000') THEN NULL
        ELSE NULLIF(h.issuer_cusip,'')
      END,
      NULLIF(i.identifier_ticker,''),
      NULLIF(i.other_identifier,''),
      NULLIF(h.holding_id,'')
    ) AS security_key,
    NULLIF(h.percentage,'')::double precision AS weight_pct,
    NULLIF(h.currency_value,'')::double precision AS market_value,
    NULLIF(h.balance,'')::double precision AS shares
  FROM raw_nport.fund_reported_holding h
  JOIN raw_nport.registrant r
    ON r.accession_number = h.accession_number
  JOIN raw_nport.fund_reported_info fri
    ON fri.accession_number = h.accession_number
  JOIN raw_nport.submission s
    ON s.accession_number = h.accession_number
  LEFT JOIN raw_nport.identifiers i
    ON i.holding_id = h.holding_id
  WHERE NULLIF(s.report_date,'') IS NOT NULL
    AND NULLIF(r.cik,'') IS NOT NULL
    AND NULLIF(fri.series_id,'') IS NOT NULL
    AND NULLIF(h.holding_id,'') IS NOT NULL
    AND COALESCE(
          NULLIF(i.identifier_isin,''),
          CASE
            WHEN NULLIF(h.issuer_cusip,'') IN ('000000000') THEN NULL
            ELSE NULLIF(h.issuer_cusip,'')
          END,
          NULLIF(i.identifier_ticker,''),
          NULLIF(i.other_identifier,''),
          NULLIF(h.holding_id,'')
        ) IS NOT NULL
)
INSERT INTO analytics.fact_holding (
  fund_key, report_date, security_key, weight_pct, market_value, shares
)
SELECT
  fund_key,
  report_date,
  security_key,
  SUM(weight_pct),
  SUM(market_value),
  SUM(shares)
FROM fact_src
GROUP BY fund_key, report_date, security_key;

-- -----------------------
-- 4) fact_fund_return (deferred)
-- -----------------------
-- monthly_total_return is keyed by class_id; we'll load it after we add mapping class_id -> series_id (or switch to class_key).

COMMIT;