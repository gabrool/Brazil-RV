# IBGE raw-to-research spine

This spine converts source-specific IBGE silver tables into structural gold
panels under `data/gold/ibge/{panel}/`. It reads existing silver data only and
does not run IBGE ingestion or live downloads.

## Inputs

- `data/silver/ibge_sidra_series/`
- `data/silver/ibge_release_calendar/`
- `data/silver/ibge_products_metadata/`
- `data/silver/ibge_news_releases_metadata/`

Source-specific silver remains immutable. Gold writes are idempotent upserts by
each panel primary key.

## Gold panels

- `sidra_observation`: long-form SIDRA observations filtered by configured
  dataset slugs and model-usability policy.
- `sidra_asof_daily`: daily latest-available SIDRA observations for model dates.
- `release_calendar_reference`: release-calendar timing metadata for audit.
- `products_reference`: product and category metadata.
- `news_release_metadata`: metadata-only release/news rows.
- `daily_long`: SIDRA-only long model input rows with non-null values.

## Point-in-time policy

Observation panels preserve silver `ref_date` and `available_date`. For SIDRA,
`ref_date` is the reference-period end date and `available_date` is the
model-usable decision date computed during ingestion from matched release
calendar timing.

As-of panels use:

- `ref_date`: model/as-of date.
- `available_date`: same as `ref_date`.
- `observation_ref_date`: original SIDRA `ref_date`.
- `observation_available_date`: original SIDRA `available_date`.

As-of rows only use observations where
`observation_available_date <= ref_date` and `model_usable == true`. Period end
dates, release dates, and download timestamps are not availability substitutes.

## SIDRA release calendar dependency

SIDRA observations without matched release-calendar availability remain
non-model-usable in silver and are excluded from default gold observation and
as-of panels. This keeps historical macro actuals unavailable until their
configured product and reference period have a valid release-calendar match.

## Feature policy

The spine is transformer-aware and structural. It preserves official SIDRA
variables such as index levels, monthly variation, 12-month variation, weights,
or volume indices when they appear as raw official variables. It does not compute
new macro transforms.

Intentionally excluded:

- inflation breadth or services/administered/food aggregate features
- surprise or revision features
- rolling means, z-scores, volatility, correlations, PCA, or factors
- stationarity transforms or real-rate features
- calendar event-risk overlays or event windows
- text, NLP, article body, image, PDF, or scraping-heavy features

## CLI

```bash
python -m bralpha.pipelines.ibge_research_spine \
  --repo-root . \
  --start 2010-01-01 \
  --end 2026-01-01
```

Use repeated `--panel` flags to build a subset:

```bash
python -m bralpha.pipelines.ibge_research_spine \
  --repo-root . \
  --start 2024-01-01 \
  --end 2024-12-31 \
  --panel sidra_observation \
  --panel sidra_asof_daily
```
