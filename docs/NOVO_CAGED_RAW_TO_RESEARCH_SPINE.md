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

Movement silver keeps the old month-end-plus-2BD heuristic only as
`novo_caged_conservative_next_month_end_plus_2bd_reference_only`. Model-ready
movement groups require both the official calendar row for the competence and a
source/first-seen snapshot timestamp. When both exist,
`movement_group_observation` uses:

```text
available_date = max(calendar_available_date, snapshot_available_date)
```

When either side of that gate is missing, the row remains reference-only.

| dataset_id | official timing source | availability_policy | availability_basis | revision_policy | model_usable default | what happens for current snapshots | what happens for first-seen snapshots |
|---|---|---|---|---|---|---|---|
| `novo_caged_release_calendar` | MTE Novo CAGED official release-calendar page | `novo_caged_official_release_calendar` | `official_release_calendar` | `unrevised` | true | Calendar metadata remains usable as release metadata, not as movement values. | First-seen is retained as audit lineage for the calendar page. |
| `novo_caged_movements_monthly` | Official calendar plus source/first-seen movement snapshot timestamp | `novo_caged_official_calendar_plus_snapshot_first_seen` | `official_release_calendar+first_seen_download_timestamp` | `revised_use_first_seen_snapshots` | true only after both gates | Without calendar match or snapshot timestamp, rows are reference-only and excluded from daily-long. | The movement group becomes usable on `max(calendar usable date, snapshot usable date)`. |
| `novo_caged_movements_monthly` fallback | Conservative heuristic only | `novo_caged_conservative_next_month_end_plus_2bd_reference_only` | `conservative_heuristic` | `current_snapshot_reference_only` | false | Heuristic-only movement rows are retained for audit and excluded from model-ready panels. | Not applicable without calendar matching. |

Daily as-of panels use:

- `ref_date`: model/as-of date.
- `available_date`: the as-of date itself.
- `observation_ref_date`: original Novo CAGED competence month-end.
- `observation_available_date`: selected monthly observation availability.

Rows are emitted only after first availability, and no row may use an observation
with `observation_available_date > ref_date`. `daily_long` keeps only
`model_usable = true` rows and excludes `current_snapshot_no_vintage`.

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
