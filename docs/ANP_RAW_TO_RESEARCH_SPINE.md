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

`download_timestamp_utc` is never used as historical availability. The pipeline
reads observation history through the requested `end` date and emits as-of rows
only within `[start, end]`.

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
