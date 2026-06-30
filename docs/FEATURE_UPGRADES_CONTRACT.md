# Feature Upgrades Contract

Issue #67 adds deterministic, point-in-time feature builders on top of existing
research panels. These builders emit daily-long-compatible rows, preserve
available PIT metadata, and leave values unpreprocessed. They do not fit
scalers, build tensors, generate targets, read new raw sources, or remove
existing research outputs. Emitted feature values remain unpreprocessed until a
later train-only preprocessing layer consumes the metadata contract.

## PIT And Warmup Policy

Feature builders consume only as-of or already-normalized research panels.
Lagged, yoy, and rolling formulas use observation sequence order inside the
feature/entity, not calendar-day gaps. Builders accept warmup history before the
requested `start`, then filter emitted rows back to `[start, end]`.

When a feature combines multiple inputs, `available_date`,
`observation_available_date`, `observation_ref_date`, `staleness_days`,
`source_version`, and available PIT policy metadata are derived from the
contributing rows. Upgraded feature panels preserve `availability_policy`,
`availability_basis`, `revision_policy`, `vintage_id`, `model_usable`, and
`model_usable_reason` when upstream rows carry them. Missing denominators,
missing lag history, or non-positive log inputs produce null feature values
rather than stale filled values.

Common formulas:

- `log_return_nobs = log(value_t / value_t-n)` when both values are positive.
- `signed_log_level = sign(value) * log1p(abs(value))` for oil-price series
  that may become non-positive.
- `signed_log_change_nobs = signed_log_level_t - signed_log_level_t-n`.
- `pct_change_nobs = 100 * (value_t / value_t-n - 1)` when the lag denominator
  is nonzero.
- `bp_change_nobs = value_bp_t - value_bp_t-n`.
- `realized_vol_nobs_ann = stdev(log_return_1obs over last n observations) *
  sqrt(252)`.

## Canonical Upgraded Families

The canonical model-facing upgraded families are:

- `b3_di_curve_feature`
- `b3_futures_feature`
- `b3_index_feature`
- `b3_index_composition_feature`
- `bcb_sgs_feature`
- `bcb_ptax_feature`
- `bcb_focus_feature`
- `fred_rate_feature`
- `fred_market_feature`
- `tesouro_feature`
- `br_rv_cross_feature`
- `ibge_sidra_feature`
- `anp_fuel_feature`
- `ons_power_feature`
- `cvm_fund_feature`
- `novo_caged_feature`
- `receita_feature`

## Formula Scope

B3 feature panels cover DI curve levels/changes/rolldown/shapes/forwards,
continuous futures log levels/returns/volatility/liquidity/roll flags, index
returns/volatility/drawdown/liquidity, and index-composition concentration.
DI forward rates use log discount factors:

```text
forward_bp = (exp((log_df_T1 - log_df_T2) * 252 / (T2 - T1)) - 1) * 10000
```

Rates and global RV panels add real policy rates, PTAX mid-rate returns and
bid/ask spreads in basis points, FRED rate/market levels and changes, signed-safe
WTI/Brent oil levels and changes, Tesouro price/flow/stock features, and
Brazil/global cross features such as DI-minus-UST spreads, 5bd/21bd BRL
idiosyncratic FX returns, and 21bd IBOV-minus-S&P 500 relative returns.

Macro/source panels add:

- Focus levels, `std_dev_log1p`, log respondent counts, revisions, dispersion
  ratios using `std_dev / max(abs(median), 1.0)`, and matched Top5-minus-general
  spreads.
- IBGE SIDRA inflation trailing sums, quarterly/monthly yoy changes, signed GDP
  current-value logs, and PNAD/activity log changes.
- ANP fuel price logs, price changes, sale-purchase spreads, ethanol/gasoline
  parity, diesel/gasoline spread, sales volume yoy, production yoy, and coverage
  logs.
- ONS stored-energy percent levels/changes/seasonal z-scores, ENA percent-MLT
  features when the unit is percent-MLT, physical ENA log features otherwise,
  CMO/load logs and changes, generation shares, interchange-to-load,
  interchange daily signed-log flows, and hour-count logs. Seasonal z-scores use
  only prior observations in the same calendar month and require at least 24
  prior observations.
- CVM fund flow log1p rows, net/gross flows, NAV-scaled ratios, redemption
  share, balance log1p rows, count logs, and shareholder-count changes.
- Novo CAGED combined admissions/dismissals, net/gross jobs, ratios, yoy changes,
  wage logs, contract hours, and state/sector diffusion shares.
- Receita signed-log collections, yoy and real-yoy percent changes, rolling
  signed-log sums, and category shares. Category-share denominators prefer an
  explicit total-collection row for the compatible scope/table group and fall
  back to same-group category sums only when no explicit total exists.

## Metadata And Demotions

`configs/modeling/feature_preprocessing.yaml` contains explicit rules for every
upgraded family. There is no catch-all rule. Unknown feature rows fail metadata
coverage until a reviewed selector is added.

Once upgraded replacements exist, superseded raw rows are marked
`model_default: false` while remaining available for audit and diagnostics. This
applies to raw B3 DI/continuous/index/composition rows, raw PTAX/Focus/FRED/IBGE
/Tesouro rows, raw ANP/ONS/CVM/Novo CAGED/Receita daily-long rows, redundant DI
compatibility columns, listed-market individual-security rows, and pure
diagnostics without predictive numeric content.

The emitted feature values remain deterministic raw feature values. The
preprocessing metadata records later train-only transform/scaler intent; it does
not fit scalers and does not apply transforms in the feature builders.
