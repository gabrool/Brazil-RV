# ANP Raw-To-Research Spine

This layer converts the current ANP source-specific silver tables into ANP gold
panels under `data/gold/anp/{panel}/`. It does not run ingestion, call ANP,
mutate silver data, or add new ANP datasets.

## Inputs And Outputs

Model-usable inputs are limited to:

- `data/silver/anp_fuel_prices_weekly/`
- `data/silver/anp_fuel_sales_monthly/`
- `data/silver/anp_oil_gas_production_monthly/`

Gold outputs are:

- `fuel_price_station_observation`
- `fuel_price_group_observation`
- `fuel_sales_observation`
- `fuel_sales_group_observation`
- `oil_gas_production_observation`
- `oil_gas_group_observation`
- `state_asof_daily`
- `daily_long`

Deferred ANP datasets from the source map remain excluded.

## Point-In-Time Policy

Observation panels keep ANP `ref_date` as the source observation date and
preserve the silver `available_date`. As-of panels use `ref_date` as the model
date, set `available_date = ref_date`, and only use observations with
`observation_available_date <= ref_date`.

ANP has official/conservative first-release timing for the live datasets. Source
last-modified and first-seen timestamps are preserved for revision audit and
snapshot IDs, but they do not delay old observations inside large historical
files when the official lag policy is defensible.

| dataset_id | official timing source | availability_policy | availability_basis | revision_policy | model_usable default | what happens for current snapshots | what happens for first-seen snapshots |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `anp_fuel_prices_weekly` | ANP weekly fuel-price survey CSV page | `anp_weekly_price_survey_conservative_7d_next_business_day` | `conservative_heuristic` | `official_lag_no_revisions` | true | Stored with source snapshot lineage; official 7-calendar-day lag remains the model-ready timing. | `first_seen_timestamp_utc` is preserved in silver/research for audit and `vintage_id` construction. |
| `anp_fuel_sales_monthly` | ANP page states monthly updates up to the last day after the reference month | `anp_monthly_official_next_month_end_next_business_day` | `official_lag_policy` | `official_lag_no_revisions` | true | Stored with source snapshot lineage; last-day-following-month timing remains model-ready. | `first_seen_timestamp_utc` is preserved for revision audit without over-delaying official-lag rows. |
| `anp_oil_gas_production_monthly` | ANP page states monthly updates up to the last day after the reference month | `anp_monthly_official_next_month_end_next_business_day` | `official_lag_policy` | `official_lag_no_revisions` | true | Stored with source snapshot lineage; last-day-following-month timing remains model-ready. | `first_seen_timestamp_utc` is preserved for revision audit without over-delaying official-lag rows. |

The pipeline reads observation history through the requested `end` date and
emits as-of rows only within `[start, end]`. Daily-long outputs keep only rows
where `model_usable = true` and exclude `current_snapshot_no_vintage`.

## Aggregation Rules

ANP has no hourly data in the current silver scope, so there is no hourly-to-daily
aggregation here.

Fuel-price silver is station/product microdata. Station rows are preserved in an
audit panel, but model-facing panels aggregate only to low-cardinality
`all`, `region`, and `state` groups by product. Sale and purchase prices are
averaged over non-null official station values and count fields record
contributing rows.

Fuel-sales rows are monthly state/product observations. Group panels sum
official `sales_volume_m3` by configured geography/product and preserve counts.

Oil/gas production rows stay long by official metric. Group panels sum official
metric values by configured geography, location, product, and metric type.

## Exclusions

This spine preserves official values and structural counts only. It intentionally
does not compute price changes, spreads, pass-through, inflation proxies, growth
rates, shares, ratios, per-capita values, rolling features, z-scores, seasonal
anomalies, BOE conversions, flaring shares, Petrobras labels, stress labels, or
portfolio/backtest fields.
