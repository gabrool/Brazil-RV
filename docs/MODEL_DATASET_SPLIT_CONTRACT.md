# Model Dataset Split Contract

This contract defines the first model-sample date policy for Brazil-RV. It is
only about sample dates, warmup, lookback, target horizons, and chronological
split assignment. It does not define feature transforms, tensors, labels, or
training.

## Defaults

| field | value |
|---|---|
| model start date | `2013-01-02` |
| lookback | `256` business days |
| feature warmup | `504` business days |
| target horizons | `1`, `5`, and `21` business days |
| split assignment date | `asof_date` |
| embargo | exactly `0` business days |

The canonical config lives at `configs/modeling/dataset.yaml`.

## Splits

| split | start | end |
|---|---|---|
| train | `2013-01-02` | `2021-12-31` |
| validation | `2022-01-03` | `2023-12-29` |
| test | `2024-01-02` | latest available date unless an explicit end is supplied |

Split assignment is based only on the sample `asof_date`. It does not use target
end dates, target horizons, or future target windows.

## No Embargo

There is no embargo, purge, or intentional gap between splits. Any missing dates
between the configured split boundaries are calendar weekends or non-business
days. Samples near a boundary may have target windows that cross into the next
split. That behavior is accepted intentionally to preserve post-Covid data
density.

## Target Dates

`target_end_date(asof_date, horizon_business_days)` is metadata for future
sample builders. It advances by business days from the sample `asof_date`, but it
must not be used for split assignment and must not drop boundary samples.
