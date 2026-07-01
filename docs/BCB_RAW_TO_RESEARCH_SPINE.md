# BCB raw-to-research spine

This layer converts source-specific BCB silver tables into research-ready gold
panels under `data/gold/bcb/`. It reads existing silver Parquet only, does not
run live BCB downloads, and does not mutate source-specific silver inputs.

## Inputs and outputs

Silver inputs are read from `data/silver/{dataset_id}/` when present:

- `bcb_sgs_series`
- `bcb_ptax_exchange_rates`
- `bcb_focus_expectations`
- `bcb_focus_top5_expectations`
- `bcb_focus_top5_reference_dates`

Gold outputs are written to:

- `data/gold/bcb/sgs_observation_daily/`
- `data/gold/bcb/sgs_asof_daily/`
- `data/gold/bcb/sgs_feature_daily/`
- `data/gold/bcb/ptax_selected_daily/`
- `data/gold/bcb/focus_expectation_observation_daily/`
- `data/gold/bcb/focus_expectation_asof_daily/`
- `data/gold/bcb/focus_reference_dates/`
- `data/gold/bcb/daily_long/`

Date-indexed outputs are partitioned by `year` and are idempotent upserts by
each panel's primary key.

## Point-in-time policy

Every model-usable daily row has `ref_date` and `available_date`.
`available_date` is the model-usable daily decision date. Observation panels
preserve the source observation date and source availability date. As-of panels
use `ref_date` as the model date, set `available_date = ref_date`, and carry the
original source dates as `observation_ref_date` and `observation_available_date`.
SGS rows also carry `availability_basis`, `revision_policy`, source reference
URL, reference-only reasons, alternate source families, and notes from
`configs/series/bcb_sgs.yaml`.

As-of panels only use source rows where:

```text
observation_available_date <= ref_date
```

The first pass emits as-of rows only after an observation is available. It does
not create unavailable placeholder rows before first availability.

SGS series with `availability_policy = unknown` or
`revision_policy = current_snapshot_reference_only` are retained in source
metadata but have `model_usable = false`, so they do not enter model-ready SGS
observation, as-of, feature, or daily-long panels when `include_model_usable_only`
is on. The only SGS series promoted to model-ready after the original
Selic/IPCA subset is `13982` international reserves liquidity, using
date-only next-business-day availability.

The `sgs_feature_daily` panel is built from `sgs_asof_daily`, not raw silver, so
the feature rows inherit point-in-time availability. It emits model-ready long
features for Selic levels/spreads/steps, IPCA trailing sums/annualized values,
and international reserves levels/log changes/drawdowns. The pipeline builds
SGS features from a pre-window as-of history before filtering back to the
requested output dates, so rolling IPCA and reserves features remain populated
for sliced backfills and live inference windows when enough historical
observations are available.

## Focus availability limitation

BCB Focus silver uses a conservative date-only next-business-day policy from
`Data`. BCB documents that Focus statistics are calculated daily but normally
published on the first business day of the week, so model-grade Focus usage
later needs a publication-calendar rule. The gold Focus panels carry
`availability_basis = official_weekly_publication_date_assumed_pre_eod_cutoff` and an
explicit Focus availability note to
keep this caveat visible.

## Transformer-aware feature rule

The BCB research spine includes structural preprocessing fields and the
explicit first-model SGS feature set documented in
`docs/BCB_SGS_MODEL_READY_DECISIONS.md`:

- source, series, currency, indicator, and expectation identifiers
- observation and availability dates
- raw official values from SGS, PTAX, and Focus
- SGS model-ready Selic/IPCA/reserves features built from as-of panels
- selected PTAX bulletin flags
- missingness and availability flags
- as-of latest observation values
- `staleness_days`

It intentionally excludes broader hand-crafted alpha features and model-stage
transformations, including real rates, surprises, revision factors, z-scores,
PTAX mid/spread/basis features, policy text or NLP fields, stationarity
transforms, classification labels, and portfolio/backtest logic.

## CLI

```bash
python -m bralpha.pipelines.bcb_research_spine \
  --repo-root . \
  --start 2010-01-01 \
  --end 2026-01-01
```

Use repeated `--panel` flags to build selected panels only. Missing optional
inputs are skipped in full-pipeline runs and raise clear errors when an
explicitly selected panel has no required input.
