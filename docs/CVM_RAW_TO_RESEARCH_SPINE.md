# CVM Raw-To-Research Spine

This layer converts CVM source-specific silver tables into research-ready gold
panels under `data/gold/cvm/`. It reads existing silver Parquet files only; it
does not run ingestion, call CVM, read raw or bronze files, or mutate silver
data.

## Inputs And Outputs

Inputs:

- `data/silver/cvm_fund_daily_reports/`
- `data/silver/cvm_fund_registry_current/`

Outputs:

- `data/gold/cvm/fund_daily_observation/`
- `data/gold/cvm/fund_group_observation/`
- `data/gold/cvm/fund_flows_daily/`
- `data/gold/cvm/fund_state_asof_daily/`
- `data/gold/cvm/fund_registry_current_reference/`
- `data/gold/cvm/daily_long/`

## Point-In-Time Policy

Daily fund report observation rows keep the CVM report date as `ref_date` and
preserve the silver `available_date`, which is the historical model-usable
decision date. Group observations aggregate only rows available from the
configured daily-report silver contract, and the group `available_date` is the
maximum contributing fund-row availability date.

As-of rows use the model date as `ref_date`, set `available_date = ref_date`,
and retain the original group observation dates as `observation_ref_date` and
`observation_available_date`.

As-of panels only use observations where
`observation_available_date <= ref_date`. Download timestamps are never used as
historical availability.

## Panel Semantics

`fund_daily_observation` is an audit panel. It preserves official CVM fund-level
values and raw silver fields, adding only missingness flags.

`fund_group_observation` is the low-cardinality aggregate observation layer used
by model-facing panels. It emits `all` and `fund_type` groups, applies
null-aware sums, counts contributing non-null values, preserves fund counts, and
uses deterministic `feature_id` values.

`fund_flows_daily` aligns subscriptions and redemptions to
`observation_available_date`. Flow rows are never forward-filled and retain
`observation_ref_date` in the primary key.

`fund_state_asof_daily` carries the latest available aggregate state by business
day. It forward-fills only state and count fields and exposes `staleness_days`.

`fund_registry_current_reference` is a current metadata reference only. It is not
joined into historical flow or state panels.

`daily_long` is aggregate-only. It is built from the flow and state panels,
drops null values, and excludes fund-level rows and registry metadata.

## Transformer-Aware Feature Rule

The gold panels preserve official values and add only structural fields needed
for daily sequence models: identifiers, observation dates, availability dates,
missingness flags, counts, `staleness_days`, and deterministic `feature_id`
values.

The spine intentionally excludes net flows, ratios, returns, rolling features,
z-scores, stationarity transforms, stress labels, portfolio fields, and backtest
logic.

## Known Limitations

- CVM registry history and class registry silver outputs are not included in
  model-ready gold panels.
- CDA, IPE, FII/FIDC, ITR, DFP, public-offering, sanctions, and regulatory-event
  datasets remain out of scope for this spine.
- `fund_type` from daily-report silver is the only low-cardinality fund
  classification used in historical aggregate panels. Current registry
  classifications remain reference-only to avoid point-in-time leakage.
