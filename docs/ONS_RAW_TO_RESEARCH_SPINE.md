# ONS Raw-To-Research Spine

This layer converts current ONS source-specific silver tables into daily,
point-in-time gold panels under `data/gold/ons/{panel}/`. It reads only:

- `data/silver/ons_ear_subsystem_daily/`
- `data/silver/ons_ena_subsystem_daily/`
- `data/silver/ons_load_daily/`
- `data/silver/ons_cmo_weekly/`
- `data/silver/ons_energy_balance_subsystem/`
- `data/silver/ons_interchange_subsystem_hourly/`

It does not run ingestion, call ONS, mutate silver, or use deferred
reservoir/REE/basin, plant, constrained-off, dispatch, capacity, reliability,
or transmission datasets.

## Gold Panels

Observation panels preserve official silver values and add only structural
fields such as deterministic `feature_id` values and missingness flags:

- `ear_subsystem_observation`
- `ena_subsystem_observation`
- `load_daily_observation`
- `cmo_weekly_observation`
- `energy_balance_daily_observation`
- `interchange_daily_observation`

`state_asof_daily` is the long latest-available daily state panel. `daily_long`
is a selected long copy of non-null as-of values for later cross-source
assembly.

## Point-In-Time Policy

Observation panels keep the ONS observation `ref_date` and the silver
`available_date`. As-of panels use `ref_date` as the model date,
`available_date = ref_date`, and preserve the source row dates as
`observation_ref_date` and `observation_available_date`.

As-of rows use only observations where:

```text
observation_available_date <= ref_date
```

`download_timestamp_utc` is never used as historical availability.

## Hourly-To-Daily Rule

ONS energy-balance and interchange silver inputs are hourly, but the Brazil-RV
model spine is daily. This layer aggregates them to daily observation panels
before any as-of or daily-long output.

Daily aggregation is intentionally structural:

- group by official date and key,
- mean over non-null official hourly values,
- count contributing hourly rows and non-null metric observations,
- set `available_date` to the maximum contributing hourly `available_date`.

No hourly model-ready panels are written.

## Exclusions

This spine does not compute changes, returns, ratios, source shares, net
interchange, thermal gaps, drought indicators, scarcity labels, intraday ranges,
volatility, rolling features, z-scores, stationarity transforms, portfolio
fields, or backtest fields.

The first pass is subsystem-level only. Higher-cardinality reservoir, plant,
capacity, reliability, and transmission data remain source-mapped for later
dedicated work.
