# Novo CAGED Raw-To-Research Spine

This layer converts source-specific Novo CAGED silver data into low-cardinality
gold panels for daily model assembly. It reads only:

- `data/silver/novo_caged_movements_monthly/`
- `data/silver/novo_caged_release_calendar/`

Gold outputs are written only under `data/gold/novo_caged/{panel}/`. The pipeline
does not run ingestion, call MTE/PDET, read raw or bronze files, mutate silver, or
commit generated data.

## Panels

| panel | purpose |
|---|---|
| `movement_record_observation` | Row-level audit panel preserving official movement silver fields with wage/hour missingness flags. |
| `release_calendar_reference` | Official release-date reference by competence month. |
| `movement_group_observation` | Monthly low-cardinality aggregates by `all`, `region`, `state`, and `cnae_section`, crossed with `movement_sign`. |
| `state_asof_daily` | Long latest-available daily state using observations with `observation_available_date <= ref_date`. |
| `daily_long` | Selected long copy of as-of rows with null values removed. |

## Point-In-Time Policy

Movement observations keep the silver `available_date` as
`silver_available_date`. When the official release-calendar reference has the
same `ref_date`, `movement_group_observation` uses that calendar
`available_date` instead and marks `availability_source = official_calendar`.
When no calendar row exists, it falls back to the movement silver date and marks
`availability_source = conservative_fallback`.

Daily as-of panels use:

- `ref_date`: model/as-of date.
- `available_date`: the as-of date itself.
- `observation_ref_date`: original Novo CAGED competence month-end.
- `observation_available_date`: selected monthly observation availability.

Rows are emitted only after first availability, and no row may use an observation
with `observation_available_date > ref_date`.

## Feature Discipline

Row-level movement records are preserved for audit but excluded from
`daily_long`. Model-facing panels use only low-cardinality aggregate groups:
`all`, `region`, `state`, and `cnae_section`, crossed with official
`movement_sign`.

The gold layer preserves official movement counts, wage means, contract-hour
means, non-null counts, group identifiers, staleness, and lineage version. It
does not add admissions/dismissals labels, net jobs, count differences, wage or
employment growth, shares, ratios, per-capita values, rolling features,
z-scores, seasonal adjustment, surprise measures, late-declaration/exclusion
revision logic, XLSX tables, RAIS, legacy CAGED, PowerBI/online query data, or
portfolio/backtest fields.
