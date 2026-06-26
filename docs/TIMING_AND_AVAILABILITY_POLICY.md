# Timing and availability policy

Brazil-RV uses a strict daily decision policy. A daily `ref_date` row is model
usable only at the configured daily decision cutoff for that date, normally for
execution in the next trading session. `available_date` is therefore the daily
decision date on which a row may enter model inputs, not merely the source
release date.

## Default profile

The default profile is `eod_daily` in `configs/timing.yaml`:

- timezone: `America/Sao_Paulo`
- decision cutoff: `18:30:00` local time
- execution lag: `next_session`
- date-only releases: next business day
- unknown release time: next business day

If an exact local timestamp is known and it is at or before the cutoff on date
`D`, the row is usable on `D`. If the timestamp is after the cutoff, the row is
usable on the next business day. If only a release date is known, the row is
also usable on the next business day.

## Field meanings

- `observation_ref_date`: date the observation describes.
- `release_date`: source-published release date, if known.
- `available_datetime_local`: exact or inferred local availability timestamp.
- `available_datetime_utc`: UTC equivalent when known.
- `available_date`: model-usable daily decision date.
- `ref_date`: observation date in observation panels, as-of date in daily
  as-of panels, and label start date in target panels.
- `label_available_date`: availability date of label endpoint data.
- `availability_policy`: named rule used to compute availability.
- `availability_note`: human-readable caveat for incomplete timing knowledge.

Intraday releases are not usable for earlier same-day decisions. A release after
the daily cutoff on `D` is first usable for the next business-day decision.

## Scheduled events and outcomes

Scheduled-event metadata, such as a known meeting date, can be available before
the event. Event outcomes, such as a decision, statement, or observed statistic,
must use the outcome's own release timing. This PR does not add event calendars
or policy-text ingestion.

## Current source policies

- B3 daily market observations remain conservative: settlements, open interest,
  trade summary, COTAHIST, and index daily market data for observation date `D`
  are usable no earlier than the next business day unless exact publication
  timestamps are modeled later.
- BCB SGS uses series-level policies: daily financial series use
  `next_business_day`, lagged macro series use configured lags, and unknown
  series remain not model usable or null-available until configured.
- BCB PTAX uses `quote_datetime` as the local availability timestamp when
  present. Date-only rows use the next-business-day policy.
- BCB Focus expectations use a conservative date-only next-business-day policy.
  BCB publication timing is more complex and will need a publication-calendar
  rule before model-grade Focus timing is final.
