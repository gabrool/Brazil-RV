# FRED Raw-To-Research Spine

This layer converts source-specific FRED silver observations into research-ready
gold panels under `data/gold/fred/`. It reads existing silver Parquet files and
FRED series config only; it does not run ingestion, call FRED, or mutate silver.

## Inputs And Outputs

Input:

- `data/silver/fred_series_observations/`
- `configs/series/fred.yaml`

Outputs:

- `data/gold/fred/observation/`
- `data/gold/fred/asof_daily/`
- `data/gold/fred/series_reference/`
- `data/gold/fred/daily_long/`

## Point-In-Time Policy

Observation rows keep the FRED observation date as `ref_date` and preserve the
silver `available_date`, which is the model-usable decision date. As-of rows use
the model date as `ref_date`, set `available_date = ref_date`, and retain the
original observation dates as `observation_ref_date` and
`observation_available_date`.

As-of panels only use observations where
`observation_available_date <= ref_date` and the series is model usable.
Download timestamps are never used as historical availability.

## State-Variable Treatment

FRED global market series in this source are treated as state variables or
slower state proxies. The daily as-of panel carries the latest available
official observation by series and exposes `staleness_days`, so daily market
series and monthly proxies such as copper can coexist without pretending they
have the same freshness.

If the latest official observation is missing, the as-of panel preserves that
missing row. It does not fall back to an older numeric value.

## Transformer-Aware Feature Rule

Gold panels preserve official values and add only structural fields needed by
daily sequence models: identifiers, observation dates, availability dates,
missingness flags, `staleness_days`, and deterministic `feature_id` values.

The spine intentionally excludes returns, differences, rate changes, spreads,
yield-curve slopes, real-rate spreads, credit-spread changes, oil returns,
equity returns, volatility changes, rolling means, rolling z-scores, rolling
volatility, PCA or factor estimates, surprises, revisions, stationarity
transforms, classification labels, portfolio fields, and backtest logic.

## Known Limitations

- FRED redistributes data from multiple underlying sources, so St. Louis Fed
  terms and underlying source restrictions still apply.
- Some series have limited history or licensing caveats. `SP500` is useful as a
  FRED equity proxy, but it is not complete long-history equity coverage.
- `PCOPPUSDM` is monthly and will usually have larger `staleness_days` than
  daily market series.
- This FRED-only source does not cover EWZ, EEM, EMB, HYG, LQD, iron ore,
  long-history equity EOD, commodity futures EOD, China equities or PMI, IMF,
  World Bank, OECD, or BIS datasets.
