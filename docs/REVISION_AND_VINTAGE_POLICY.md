# Revision and Vintage Policy

This project treats every model feature as point-in-time data. A row is model usable only when the repository can identify when the value was knowable and whether later revisions can overwrite it.

## Required Lineage Fields

Model-ready or audit-ready panels should carry these fields whenever the source can revise or republish history:

- `available_date`: first model decision date when the row can be used.
- `availability_basis`: how `available_date` was established.
- `availability_policy`: source-specific timing rule, such as an official release calendar or conservative lag.
- `revision_policy`: how later source revisions are handled.
- `vintage_id`: stable identifier for a publication vintage or first-seen snapshot.
- `first_seen_timestamp_utc`: download timestamp used when the source does not publish vintages.
- `source_publication_datetime_utc`: official timestamp when the source publishes one.
- `model_usable`: explicit gate used by research panels.

## Availability Basis

Use the shared values in `bralpha.timing.vintages`:

- `exact_source_timestamp`: the source publishes an exact release timestamp.
- `source_date_only`: the source publishes only a release or vintage date, so the project applies the configured decision cutoff.
- `first_seen_download_timestamp`: the source lacks official vintage timing, and the project has persisted first-seen snapshots.
- `current_snapshot_no_vintage`: the source is a mutable current snapshot with no vintage or first-seen lineage.
- `unknown`: timing is not established.

## Revision Policy

- `unrevised`: official history is treated as final for model purposes after its availability rule.
- `official_lag_no_revisions`: source is usable after a documented lag and has no revision handling requirement.
- `revised_use_vintages`: rows are usable only when official vintages or realtime dates are available.
- `revised_use_first_seen_snapshots`: rows are usable only from the stored first-seen download timestamp.
- `current_snapshot_reference_only`: current snapshot data is allowed for reference tables but not model features.

Current-snapshot revised data must stay out of model-ready panels unless it has either official vintages or stored first-seen snapshots. Unknown timing also means `model_usable = false`.

## Source Notes

FRED revised macro series use `revised_use_vintages` and FRED realtime dates. Current FRED snapshots of revised series remain `model_usable = false`.

IBGE SIDRA model-ready rows use matched IBGE release-calendar events as publication vintages. Rows with verified release-calendar product IDs carry `availability_basis`, `revision_policy`, `vintage_id`, `first_seen_timestamp_utc`, and `source_publication_datetime_utc` through the silver, observation, as-of, and daily-long panels. SIDRA current snapshots without matched release-calendar lineage remain `current_snapshot_reference_only` and are not model usable.

Tesouro Direto sales use the official two-business-day lag from CKAN resource metadata. Redemptions use a conservative two-business-day lag until official metadata documents a more precise rule. Prices/rates keep their existing date-only next-business-day rule.

ONS data should follow the existing docs-grounded ONS contract: prefer official ONS metadata where available, use first-seen snapshots when implemented, and keep mutable current-snapshot revised data out of model-ready panels.

BCB SGS series must carry source references, timing notes, and revision metadata. Series with unknown timing or unverified identifiers are configuration candidates only and must remain `model_usable = false`.
