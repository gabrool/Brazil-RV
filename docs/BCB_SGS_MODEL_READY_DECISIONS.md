# BCB SGS Model-Ready Decisions

This document records the conservative issue #54 decision for SGS model-ready
coverage. The goal is to broaden BCB SGS beyond Selic/IPCA without allowing
current snapshots or unknown timing into model-ready panels.

## Official Sources

BCB Open Data SGS resources document the `bcdata.sgs.{codigo_serie}/dados`
JSON endpoint with `formato`, `dataInicial`, and `dataFinal` parameters, plus a
latest-values route. Each configured series keeps its official BCB catalog or
API URL in `configs/series/bcb_sgs.yaml`.

Official BCB catalog metadata checked for this decision includes:

- `13982` Reservas internacionais - Conceito liquidez - Total - Diaria:
  official BCB daily SGS reserves series, USD millions.
- `22701` Transacoes correntes - mensal - saldo and `22707` Balanca comercial -
  Balanco de Pagamentos - mensal - saldo: official BCB monthly SGS series with
  Tempestividade up to four weeks after the reference period.
- `27810`, `27813`, and `27815` broad money M2/M3/M4: official BCB monthly SGS
  series with Tempestividade marked not applicable and methodology-revision
  notes.
- `24363`, `24364`, `20539`, and `21082`: official BCB monthly activity and
  credit SGS series, but without sufficient public vintage or historical
  release-calendar evidence for strict point-in-time model training.

## Model-Ready SGS Series

The model-ready SGS set is intentionally narrow:

| series_id | slug | family | decision |
|---:|---|---|---|
| 11 | selic_over | rates | Keep model-ready with date-only next-business-day availability. |
| 432 | selic_target | rates | Keep model-ready with date-only next-business-day availability. |
| 433 | ipca | inflation | Keep model-ready with the existing conservative 15-day lag. |
| 13982 | international_reserves_liquidity | external_reserves | Promote to model-ready using date-only next-business-day availability. |

`13982` is the only newly model-ready SGS series in this issue. Date-only rows
are usable no earlier than the next business day; same-day use is not allowed.

## Reference-Only SGS Families

The following families stay available for source-map/reference use but remain
out of model-ready daily-long output. Each configured row carries
`non_model_usable_reason`, `reference_feature_family`, and
`alternate_source_family`.

| family | SGS candidates | reason | alternate source family |
|---|---|---|---|
| activity | `24363`, `24364` | SGS current snapshots lack sufficient historical vintage or release-calendar evidence. | IBGE SIDRA release-calendar datasets. |
| credit | `20539`, `20631`, `21082` | SGS current snapshots lack sufficient historical vintage or release-calendar evidence. | CVM fund flows and market-liquidity features. |
| fiscal/debt | `13762`, `4513`, `4478`, `4649`, `4583` | SGS current snapshots lack sufficient historical vintage or release-calendar evidence. | Tesouro and Receita families. |
| external monthly | `22701`, `22707` | Official four-week Tempestividade is useful metadata, but SGS does not provide historical vintages. | BCB Focus external expectations until a first-seen/vintage strategy exists. |
| monetary/liquidity | `27810`, `27813`, `27815` | Official metadata marks Tempestividade as not applicable and records methodology revision history. | Market liquidity, CVM flows, and B3 volume/open-interest features. |

## Derived Model-Ready Features

`sgs_feature_daily` is built from `sgs_asof_daily`, so the source values have
already passed availability filtering. Feature rows use
`source_family = bcb_sgs_feature` and stable feature IDs such as:

- `bcb_sgs_feature:rates:selic_over_level_pa`
- `bcb_sgs_feature:rates:selic_over_minus_target_bp`
- `bcb_sgs_feature:inflation:ipca_12m_sum_pct`
- `bcb_sgs_feature:external_reserves:reserves_log_change_5bd`

Default BCB `daily_long` includes only rows with `model_usable = true`.
Reference-only SGS rows remain available in config/source metadata and are
excluded from model-ready daily-long output.
