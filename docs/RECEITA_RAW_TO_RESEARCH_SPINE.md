# Receita Raw-To-Research Spine

This layer converts the source-specific Receita silver table into long,
point-in-time gold panels for daily model assembly.

Input:

- `data/silver/receita_tax_collection_monthly/`

Outputs:

- `data/gold/receita/tax_collection_observation/`
- `data/gold/receita/tax_collection_feature_observation/`
- `data/gold/receita/state_asof_daily/`
- `data/gold/receita/daily_long/`

The pipeline does not run Receita ingestion, call Receita, call dados.gov.br,
or read raw/bronze data. Source-specific silver remains immutable.

## Point-In-Time Policy

Observation panels use Receita collection month-end as `ref_date` and inherit
`available_date` from silver. That silver date is produced under
`receita_monthly_collection_conservative_next_month_end_plus_5bd`; download
timestamps are never used as historical availability.

Daily as-of rows use the model date as `ref_date`, set `available_date =
ref_date`, and carry the source observation dates as:

- `observation_ref_date`
- `observation_available_date`

As-of panels only use observations where `observation_available_date <=
ref_date`. They emit rows only after the first available observation and carry
monthly observations forward with `staleness_days`.

## Panel Semantics

`tax_collection_observation` preserves official nominal Receita rows and adds
only `has_collection_amount_brl`. It does not aggregate, infer totals, or
combine category/code/table rows. If the official file includes a total row,
that row is carried as an ordinary official row.

`tax_collection_feature_observation` adds deterministic feature identities:

```text
receita_tax_collection|{collection_scope}|{table_kind}|{revenue_key}
```

Tokens use the same normalization style as source columns; blank or null values
become `unknown`. Feature IDs exclude `ref_date`, `raw_path`, `sha256`, and
`download_timestamp_utc`. Same-date feature ID collisions raise instead of being
silently deduplicated.

`state_asof_daily` is long-format latest-available daily state. It preserves the
latest missing official observation when that row is the latest available row;
it does not fall back to an older numeric value.

`daily_long` is a selected long copy of non-null Receita state rows for later
cross-source assembly. It does not pivot wide.

## Explicit Exclusions

This spine intentionally does not compute:

- real values or inflation adjustment
- month-over-month or year-over-year changes
- growth rates, shares, ratios, or revenue surprises
- rolling features, rolling z-scores, trend-cycle components, or seasonal adjustment
- fiscal labels, fiscal-stress labels, tax-burden ratios, or collection-to-GDP ratios
- portfolio, target, or backtest fields

Deferred Receita datasets remain out of scope: state-level collection, tax
expenditures, fiscal-benefit reports, tax-burden data, monthly PDF reports, CNPJ
reference data, Simples Nacional, REFIS/installment programs, and customs or
specialty tax datasets.
