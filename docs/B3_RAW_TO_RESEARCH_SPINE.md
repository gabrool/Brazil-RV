# B3 raw-to-research spine

This layer converts source-specific B3 silver tables into research-ready gold
panels under `data/gold/b3/`. It does not run live downloads, does not mutate
source-specific silver inputs, and does not create shared canonical merge tables
under `data/silver/`.

## Inputs and outputs

Silver inputs are read from `data/silver/{dataset_id}/` when present:

- Futures: `b3_futures_settlements`, `b3_derivatives_open_interest`,
  `b3_derivatives_trade_summary`, optional `b3_futures_contract_master`, and
  optional `b3_holiday_calendar`.
- Listed markets and indexes: `b3_cotahist_yearly`, optional
  `b3_cotahist_daily`, `b3_traded_securities`, `b3_isin_database`,
  `b3_indexes_historical_data`, `b3_indexes_composition`,
  `b3_indexes_current_portfolio`, and `b3_indexes_theoretical_portfolio`.

Gold outputs are written to:

- `data/gold/b3/futures_contract_daily/`
- `data/gold/b3/continuous_futures_daily/`
- `data/gold/b3/di_curve_contract_daily/`
- `data/gold/b3/di_curve_grid_daily/`
- `data/gold/b3/listed_market_daily/`
- `data/gold/b3/index_daily/`
- `data/gold/b3/index_composition_daily/`
- `data/gold/b3/targets_daily/`

All date-indexed outputs are partitioned by year and are idempotent upserts by
their panel primary key.

## Point-in-time policy

Every feature or research panel row has `ref_date` and `available_date`.
Derived rows that use multiple sources set `available_date` to the maximum
contributing input availability. Contract selection for continuous futures uses
only same-row, same-date point-in-time contract fields. Targets are separate
label rows and use `label_available_date` from the future endpoint row.

## Continuous futures

The first roll policy is `maturity_rank`. For configured roots, the pipeline
selects `ROOT_R1`, `ROOT_R2`, and `ROOT_R3` from usable contracts with enough
days to maturity. Liquidity filters use only volume/open-interest already
present on the same contract row. Roll dates are explicit: `is_roll_date` marks
selected-contract changes, `previous_contract_id` records the prior selection,
and same-contract one-day changes are null on roll dates.

## DI curve grid

The DI contract curve uses DI1 observed contracts and preserves raw settlement
or quote values as `curve_value`; it does not assume those values are rates
unless source conventions later make that explicit. The fixed-tenor grid uses
simple linear interpolation by `days_to_maturity_calendar`. Unsupported tenors
remain null instead of being silently extrapolated.

## Transformer-aware feature rule

Included fields are structural or preprocessing fields: contract identifiers,
maturity dates, days to maturity, maturity rank, roll flags, selected contract,
raw settlement/volume/open-interest values, one-step differences/returns,
fixed-tenor curve values, missingness masks, liquidity/tradeability masks, and
labels in the target panel.

The spine intentionally excludes hand-crafted alpha and technical features such
as moving averages, RSI, MACD, Bollinger bands, rolling correlations, rolling
volatility, z-scores, PCA factors, slope/curvature/butterflies, and arbitrary
momentum windows.

## Known limitations

- No corporate-action adjustment or inflation adjustment is attempted.
- No canonical B3 market merge table is created beyond the requested research
  panels.
- No advanced alpha, carry, roll-down, volatility, or portfolio features are
  produced in this PR.
- No live B3 Daily Bulletin endpoints are used unless separately confirmed in
  ingestion.
- Root-specific futures P&L conventions are not finalized here.

## CLI

```bash
python -m bralpha.pipelines.b3_research_spine \
  --repo-root . \
  --start 2010-01-01 \
  --end 2026-01-01
```

Use repeated `--panel` flags to build selected panels only. Missing optional
inputs are skipped in full-pipeline runs and raise clear errors when explicitly
selected.
