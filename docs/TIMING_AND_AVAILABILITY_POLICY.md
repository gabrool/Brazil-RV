# Timing and Availability Policy

Brazil-RV uses a strict daily decision policy. A model-ready daily `ref_date`
row may enter model inputs only when the source observation was available at or
before the configured daily decision cutoff for that date.

## Calendar

Daily model grids use the canonical B3 trading calendar, not a weekday-only
calendar. The current manual calendar lives at
`configs/calendars/b3_trading_holidays.yaml` and covers 2000-2035. Production
code fails outside this coverage instead of silently falling back to Mon-Fri
logic.

The calendar is a versioned manual file because the repo does not yet wire a
verified machine-readable B3 source. It lists dates explicitly, including B3
year-end no-trading dates and exchange closures that are not simple national
holiday rules. Historical additions should be validated against official B3
holiday bulletins or another audited exchange-calendar source before dates are
removed or extended.

## Default Profile

The default profile is `eod_daily` in `configs/timing.yaml`:

- timezone: `America/Sao_Paulo`
- decision cutoff: `18:30:00` local time
- execution lag: `next_session`
- business calendar: canonical B3 trading calendar

If an exact local timestamp is known and it is at or before the cutoff on a B3
business date `D`, the row is usable on `D`. If the timestamp is after cutoff,
or falls on a non-business date, the row is usable on the next B3 business day.

Date-only releases are source-specific. Conservative date-only releases use the
next B3 business day. Official same-day EOD release-calendar dates are usable on
the release date when it is a B3 business day, otherwise the next B3 business
day.

## Field Meanings

- `observation_ref_date`: date the observation describes.
- `release_date`: source-published release date, if known.
- `available_datetime_local`: exact or inferred local availability timestamp.
- `available_datetime_utc`: UTC equivalent when known.
- `available_date`: model-usable daily decision date.
- `ref_date`: observation date in observation panels, as-of date in daily
  as-of panels, and label start date in target panels.
- `label_available_date`: availability date of label endpoint data.
- `availability_policy`: named rule used to compute availability.
- `availability_basis`: evidence class behind the timing rule.
- `availability_note`: human-readable caveat for incomplete timing knowledge.
- `model_usable`: whether a row is allowed into model-facing panels.

Observation panels may have `available_date` after `ref_date`, because a source
can publish an observation later than the period it describes. Model-ready as-of
panels must have `available_date <= ref_date` and, when present,
`observation_available_date <= ref_date`.

## Current Source Policies

- B3 daily market observations, including settlements, open interest, trade
  summary, COTAHIST, and index daily market data for observation date `D`, are
  usable no earlier than the next B3 business day unless exact publication
  timestamps are modeled later.
- B3 futures maturities, DI tenors, rolls, targets, and as-of grids use the same
  canonical B3 calendar.
- BCB SGS daily Selic and external-reserves series use conservative next-B3-day
  timing. SGS IPCA is reference-only until matched to the official IBGE release
  calendar.
- BCB PTAX uses `dataHoraCotacao` as the source timestamp when present and
  applies the local cutoff rule. Date-only fallback remains conservative.
- BCB Focus and Top5 use the official weekly publication date in `Data` as a
  same-day EOD release-calendar date.
- IBGE SIDRA is model-usable only when matched to the official IBGE release
  calendar. Exact timestamps use cutoff timing; official date-only releases use
  same-day EOD timing.
- Novo CAGED movement records carry only reference-only heuristic availability
  until joined to the official release calendar. Model-facing movement panels
  require an official calendar date for the competence month.
- ONS timestampless snapshots are reference-only. Rows with a source publication
  timestamp, resource last-modified timestamp, HTTP last-modified timestamp, or
  explicit first-seen timestamp use cutoff timing and may be model-usable.
  Missing PIT metadata defaults fail-closed.
- Tesouro Direto prices/rates use next B3 business day. Sales and redemptions
  use 2 B3 business days. Tesouro Direto stock uses 30 B3 business days, and DPF
  stock uses 45 B3 business days.
- FRED date-only vintages remain conservative next-B3-day unless exact vintage
  timestamps are available.
