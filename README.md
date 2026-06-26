# Brazil RV

Brazil RV is a systematic macro research project focused on Brazilian asset classes, starting with a **free-data, daily-frequency, multi-asset futures model**.

The initial target is to build a point-in-time daily research pipeline for:

- **Rates:** DI futures and related B3 rates products.
- **FX:** USD/BRL futures.
- **Equity beta:** Ibovespa futures.

The project intentionally begins with free or freely attainable data, broad ingestion, strict point-in-time discipline, and modular feature attribution.

## Source-of-truth docs

Codex and other agents should read these first:

1. [`AGENTS.md`](AGENTS.md) — working rules, architecture, and non-negotiable project assumptions.
2. [`docs/PROJECT_PLAN.md`](docs/PROJECT_PLAN.md) — full strategy and data plan.
3. [`docs/B3_INGESTION_SETUP.md`](docs/B3_INGESTION_SETUP.md) — immediate setup plan for downloading and normalizing B3 data.

## Current phase

The current phase is **B3 ingestion setup**. Do not start model research before the B3 market-data spine is in place:

- Futures settlements.
- Contract metadata.
- Volume and open interest.
- Reference rates and curves.
- COTAHIST.
- Index levels and composition.
- Investor participation and related public B3 reports.

## Storage convention

Local data files are not committed to Git. Use this local layout:

```text
data/raw/       # immutable downloaded files
data/bronze/    # parsed source-specific tables
data/silver/    # canonical point-in-time tables
data/gold/      # model-ready features and labels
data/manifests/ # download manifests, hashes, lineage metadata
```

## Core principle

Every observation used by a model must have an `available_date`. A model trained or evaluated as of date `t` may only use rows with:

```text
available_date <= t
```
