# Agent instructions for Brazil RV

This repository is the source of truth for the **Brazil RV systematic macro** project.

## Project objective

Build a free-data, daily-frequency, multi-asset futures research pipeline for Brazilian markets. The first tradable universe is:

- DI/rates futures and related B3 rates products.
- USD/BRL futures.
- Ibovespa futures.

The model should use information available by end of day `t` to forecast risk-adjusted returns over `t+1`, `t+5`, and `t+20`.

## Current standing plan

Assume the plan in `docs/PROJECT_PLAN.md` is active unless explicitly superseded by a later committed document.

The immediate engineering target is `docs/B3_INGESTION_SETUP.md`: create a robust B3 data-ingestion spine before model research.

## Non-negotiable rules

1. **Point-in-time correctness is mandatory.** Every model input must have an `available_date`; no feature can use observations with `available_date > asof_date`. `available_date` is the model-usable daily decision date, not merely a source release date.
2. **Raw data is immutable.** Files in `data/raw/` are append-only and are identified by source, dataset id, download timestamp, and content hash.
3. **Do not commit downloaded market data.** Commit code, configs, schemas, docs, small fixtures, and tests only.
4. **Keep layers separate.** Downloaders download, parsers parse, normalizers canonicalize, feature builders create features, target builders create labels, models forecast, portfolios size positions, and backtests evaluate P&L.
5. **Free data first.** Do not add paid data dependencies unless explicitly approved and documented.
6. **Broad ingestion, narrow rejection.** Include free and plausibly useful daily datasets unless they are truly impractical or require a dedicated NLP/modeling project just to convert into basic features.
7. **No notebook-only logic.** Notebooks may explore, but production logic must live under `src/bralpha/` and be covered by tests.
8. **No silent data fixes.** Data-quality failures should be logged, surfaced, and either corrected in an explicit transform or quarantined.

## Preferred stack

- Python 3.11+.
- `uv` or standard virtualenv/pip for environment management.
- `httpx` + `tenacity` for HTTP clients.
- `pydantic` for configs and schemas.
- `pandas`, `polars`, `pyarrow`, and `duckdb` for tabular work.
- `pytest` for tests.
- `ruff` for linting/formatting.

## Repository layers

```text
data/raw/       # immutable source files, ignored by Git
data/bronze/    # parsed source-specific tables, ignored by Git
data/silver/    # canonical point-in-time tables, ignored by Git
data/gold/      # feature/label matrices, ignored by Git
data/manifests/ # download manifests and hashes, ignored by Git
```

Code should use the package namespace `bralpha`.

## Canonical dates

Use these consistently:

- `ref_date`: market date or observation date.
- `ref_period_start` / `ref_period_end`: economic period being measured.
- `release_date`: date the source first published the observation.
- `available_date`: daily decision date on which the row is allowed to enter a model.
- `download_timestamp_utc`: when this copy was downloaded.
- `vintage_id`: source version when data can be revised.

The default daily decision profile is EOD daily in `America/Sao_Paulo`: exact timestamps at or before the configured cutoff are usable that decision date, exact timestamps after the cutoff are usable the next business day, and date-only releases default to next business day. See `docs/TIMING_AND_AVAILABILITY_POLICY.md`.

For first-pass B3 market data, if exact publication time is not modeled, use a conservative daily rule: data for `ref_date = t` becomes usable on the next business day for next-session execution.

## B3 ingestion principles

The B3 pipeline should initially ingest all useful free B3 datasets listed in `docs/B3_INGESTION_SETUP.md`, including futures settlements, COTAHIST, reference rates, index data, open interest, trade summaries, investor participation, traded securities, and daily-bulletin-derived public reports.

It is acceptable to use the public `rb3` package documentation as a source map for B3 endpoints and templates, but the Python pipeline should own its raw downloads, manifests, parsed tables, and canonical schemas.

## Testing expectations

At minimum, add tests for:

- URL/config construction.
- Raw download manifest creation.
- Hash stability.
- Fixed-width parsing for COTAHIST.
- Schema validation for bronze and silver tables.
- Contract maturity parsing.
- No duplicate primary keys.
- No `available_date` leakage.
- Idempotent reruns.

## Completion criteria for ingestion tasks

A data-ingestion task is not complete until it has:

- Dataset config.
- Raw downloader.
- Parser into bronze table.
- Normalizer into silver table if applicable.
- Manifest logging.
- Basic quality checks.
- Tests with at least one small fixture or mocked response.
- Documentation of source URL, expected frequency, known limitations, and primary keys.
