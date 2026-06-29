# CVM Raw-To-Research Spine

This layer converts CVM source-specific silver tables into research-ready gold
panels under `data/gold/cvm/`. It reads existing silver Parquet files only; it
does not run ingestion, call CVM, read raw or bronze files, or mutate silver
data.

## Inputs And Outputs

Inputs:

- `data/silver/cvm_fund_daily_reports/`
- `data/silver/cvm_fund_registry_current/`

Outputs:

- `data/gold/cvm/fund_daily_observation/`
- `data/gold/cvm/fund_group_observation/`
- `data/gold/cvm/fund_flows_daily/`
- `data/gold/cvm/fund_state_asof_daily/`
- `data/gold/cvm/fund_registry_current_reference/`
- `data/gold/cvm/daily_long/`

## Timing Policy

| dataset_id | official timing source | availability_policy | availability_basis | revision_policy | model_usable default | what happens for current snapshots | what happens for first-seen snapshots |
|---|---|---|---|---|---:|---|---|
| `cvm_fund_daily_reports` | CVM `fi-doc-entrega` delivery metadata when exactly matched; otherwise persisted pipeline first-seen snapshots | `cvm_delivery_metadata`, `cvm_first_seen_snapshot`, or `cvm_fund_daily_conservative_2bd_reference_only` | `exact_source_timestamp`, `source_date_only`, `first_seen_download_timestamp`, or `conservative_heuristic` | `revised_use_vintages`, `revised_use_first_seen_snapshots`, or `current_snapshot_reference_only` | true only for delivery/first-seen rows | Two-business-day fallback remains reference-only and is excluded from model-ready daily-long rows. | Rows become usable from the first-seen usable date and keep their `vintage_id`. |
| `cvm_fund_delivery_metadata` | Official CVM `Fundos de Investimento: Documentos: Entrega` metadata | `cvm_delivery_metadata` | `exact_source_timestamp` or `source_date_only` | `revised_use_vintages` | false until the INF_DIARIO join is fixture-documented | Audit/source-map only. | Delivery metadata can make matched daily rows model-usable once the join is unambiguous. |
| `cvm_fund_registry_current` | Current CVM `cad_fi.csv` snapshot | `cvm_fund_registry_current_reference_only` | `current_snapshot_no_vintage` | `current_snapshot_reference_only` | false | Kept as reference metadata only. | Historical registry first-seen modeling is deferred. |

## Point-In-Time Policy

Daily fund report observation rows keep the CVM report date as `ref_date` and
preserve the silver `available_date`, `availability_basis`, `revision_policy`,
`vintage_id`, and `model_usable` fields. Daily rows are model-usable only when
they are backed by matched delivery metadata or by a persisted first-seen
snapshot. Group observations aggregate only rows available from the configured
daily-report silver contract, and the group `available_date` is the maximum
contributing fund-row availability date.

As-of rows use the model date as `ref_date`, set `available_date = ref_date`,
and retain the original group observation dates as `observation_ref_date` and
`observation_available_date`.

As-of panels only use observations where
`observation_available_date <= ref_date`. When multiple revisions exist, the
latest source/first-seen snapshot that is available by the model date wins.

## Panel Semantics

`fund_daily_observation` is an audit panel. It preserves official CVM fund-level
values and raw silver fields, adding only missingness flags.

`fund_group_observation` is the low-cardinality aggregate observation layer used
by model-facing panels. It emits `all` and `fund_type` groups, applies
null-aware sums, counts contributing non-null values, preserves fund counts, and
uses deterministic `feature_id` values.

`fund_flows_daily` aligns subscriptions and redemptions to
`observation_available_date`. Flow rows are never forward-filled and retain
`observation_ref_date` in the primary key.

`fund_state_asof_daily` carries the latest available aggregate state by business
day. It forward-fills only state and count fields and exposes `staleness_days`.

`fund_registry_current_reference` is a current metadata reference only. It is not
joined into historical flow or state panels.

`daily_long` is aggregate-only. It is built from the flow and state panels,
drops null values, excludes `model_usable = false` and
`current_snapshot_no_vintage` rows, and excludes fund-level rows and registry
metadata.

## Transformer-Aware Feature Rule

The gold panels preserve official values and add only structural fields needed
for daily sequence models: identifiers, observation dates, availability dates,
missingness flags, counts, `staleness_days`, and deterministic `feature_id`
values.

The spine intentionally excludes net flows, ratios, returns, rolling features,
z-scores, stationarity transforms, stress labels, portfolio fields, and backtest
logic.

## Known Limitations

- CVM registry history and class registry silver outputs are not included in
  model-ready gold panels.
- CDA, IPE, FII/FIDC, ITR, DFP, public-offering, sanctions, and regulatory-event
  datasets remain out of scope for this spine.
- `fund_type` from daily-report silver is the only low-cardinality fund
  classification used in historical aggregate panels. Current registry
  classifications remain reference-only to avoid point-in-time leakage.
