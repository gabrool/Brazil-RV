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
URL, and notes from `configs/series/bcb_sgs.yaml`.

As-of panels only use source rows where:

```text
observation_available_date <= ref_date
```

The first pass emits as-of rows only after an observation is available. It does
not create unavailable placeholder rows before first availability.

SGS series with `availability_policy = unknown` or
`revision_policy = current_snapshot_reference_only` are retained in source
metadata but have `model_usable = false`, so they do not enter model-ready SGS
observation, as-of, or daily-long panels when `include_model_usable_only` is on.
The SGS expansion in PR #53 is therefore a reference/source-map coverage
expansion. It does not claim to close the remaining model-ready SGS coverage
blocker, which is tracked in #54.

## Focus availability limitation

BCB Focus silver uses a conservative date-only next-business-day policy from
`Data`. BCB documents that Focus statistics are calculated daily but normally
published on the first business day of the week, so model-grade Focus usage
later needs a publication-calendar rule. The gold Focus panels carry
`availability_note = date_only_next_business_day_until_publication_calendar` to
keep this caveat visible.

## Transformer-aware feature rule

The BCB research spine includes only structural preprocessing fields:

- source, series, currency, indicator, and expectation identifiers
- observation and availability dates
- raw official values from SGS, PTAX, and Focus
- selected PTAX bulletin flags
- missingness and availability flags
- as-of latest observation values
- `staleness_days`

It intentionally excludes hand-crafted alpha features and model-stage
transformations, including real rates, surprises, revisions, moving or rolling
features, z-scores, PTAX mid/spread/basis features, policy text or NLP fields,
stationarity transforms, classification labels, and portfolio/backtest logic.

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
