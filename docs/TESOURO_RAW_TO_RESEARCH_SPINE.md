# Tesouro Raw-To-Research Spine

This layer converts Tesouro source-specific silver tables into research-ready
gold panels under `data/gold/tesouro/`. It reads existing silver Parquet files
only; it does not run ingestion, call CKAN, or mutate silver data.

## Inputs And Outputs

Inputs:

- `data/silver/tesouro_direto_prices_rates/`
- `data/silver/tesouro_direto_sales/`
- `data/silver/tesouro_direto_redemptions/`
- `data/silver/tesouro_direto_stock/`
- `data/silver/tesouro_dpf_stock/`

Outputs:

- `data/gold/tesouro/direto_prices_rates_observation/`
- `data/gold/tesouro/direto_prices_rates_asof_daily/`
- `data/gold/tesouro/direto_flows_daily/`
- `data/gold/tesouro/direto_stock_observation/`
- `data/gold/tesouro/direto_stock_asof_daily/`
- `data/gold/tesouro/dpf_stock_observation/`
- `data/gold/tesouro/dpf_stock_asof_daily/`
- `data/gold/tesouro/daily_long/`

## Point-In-Time Policy

Every model-usable row has `ref_date` and `available_date`. Observation panels
keep the source observation date or period-end date as `ref_date`. As-of panels
use the model date as `ref_date`, set `available_date = ref_date`, and retain
the original silver dates as `observation_ref_date` and
`observation_available_date`.

As-of panels only use observations where
`observation_available_date <= ref_date`. Download timestamps are never used as
historical availability.

## State Panels Versus Flow Panels

Prices/rates, Tesouro Direto stock, and DPF stock are state panels. Their daily
as-of panels forward-fill the latest available official observation by
`feature_id` and emit rows only after the first availability date.

Sales and redemptions are flow/event observations. They are aligned to the date
when the official observation becomes usable:

```text
ref_date = observation_available_date
available_date = ref_date
```

Flow rows are never forward-filled.

## Transformer-Aware Feature Rule

The gold panels preserve official values and add only structural fields needed
for daily sequence models: identifiers, observation dates, availability dates,
missingness flags, `staleness_days`, and `feature_id`.

The spine intentionally excludes returns, rate changes, spreads, curve slopes,
curve curvature, breakevens, term premia, carry, roll-down, net flow, flow
ratios, rolling features, z-scores, stationarity transforms, classification
labels, portfolio fields, and backtest logic.

## Known Limitations

- RTN is not included until the official API schema is fixture-verified.
- DPF emissions/redemptions XLSX resources are not included.
- CAPAG, auctions, large Tesouro Direto operation files, and investor files are
  not included in this spine.
- Tesouro Direto retail rates/prices are retail cash-bond proxies, not B3
  tradable futures prices.
