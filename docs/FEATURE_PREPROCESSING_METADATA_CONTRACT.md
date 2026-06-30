# Feature Preprocessing Metadata Contract

This contract records preprocessing intent for model-facing features that the repo
currently emits. It is metadata only: it does not transform values, fit scalers,
build tensors, create targets, or run deterministic feature builders. The
deterministic feature formulas live in
`docs/FEATURE_UPGRADES_CONTRACT.md`; this document describes how emitted rows are
annotated for a later preprocessing layer.

## Rule Matching

Rules live in `configs/modeling/feature_preprocessing.yaml` under `rules`.
Selectors may use `panel`, `source_family`, `feature_id_regex`, `value_name`,
`value_name_regex`, `unit`, and `unit_regex`.

Each feature row must match exactly one rule. A row with no matching rule is an
error. A row with multiple matching rules is also an error; priority values are
unique for stable inspection, but priority does not resolve overlaps.

B3 research panels do not use the common daily-long shape, so their rules use
`panel` plus the emitted numeric column name in `value_name`. Daily-long families
use `source_family`, `feature_id`, `value_name`, and `unit` selectors.

## Preprocessing Fields

All fitted statistics are train-only. Real-valued financial and macro features
preserve missing values for a later mask-adding preprocessing layer.

Common conventions:

- Binary flags use `transform: binary`, no winsorization, no scaler, and
  `missing_policy: false_fill`.
- Count and coverage features use `count_log1p`, train quantile winsorization,
  and train robust z-score scaling.
- Positive level features use `log_positive` when zero is invalid and
  `log1p_positive` when zero is plausible.
- Signed flow and amount features use `signed_log1p`.
- Returns, log changes, and basis-point changes use `identity` or
  `already_return` with train-only robust treatment.
- Values already emitted as logs or signed logs use `already_log`; values already
  emitted as returns use `already_return`. The metadata must not request a second
  log transform for upgraded feature rows.
- Percent/rate levels use `percent_to_bp` when raw units are percent and
  `identity` when already basis points.
- Bounded percent/share features use hard clipping before train quantile
  winsorization.
- Staleness features use `clip_only` from 0 to 756 days and are diagnostic
  age features, not count/log features.

## Model Defaults

`model_default` means "include in the first model after preprocessing." It does
not stop raw/research rows from being emitted.

Rows marked `model_default: false` are current features that are redundant,
structurally unsafe, or too noisy for the first model. Current examples include:

- raw BCB SGS rows when engineered `bcb_sgs_feature` replacements exist;
- B3 COTAHIST individual-security price and liquidity fields until corporate
  actions and security-master cleanup are handled;
- redundant DI columns such as `curve_value`, `implied_annual_rate`,
  `discount_factor`, raw PU, and redundant curve-value percentage change;
- Focus `min_value` and `max_value`;
- raw reserves level when the log-reserves level is the canonical feature;
- security-specific index theoretical quantity.
- superseded raw daily-long rows for ANP, ONS, CVM, Novo CAGED, and Receita once
  upgraded source-family feature rows exist.

## Source-Family Decisions

The metadata config follows the issue #65 source-family contract and the issue
#67 feature-upgrade contract:

- B3 rules cover raw research panels and the upgraded
  `b3_di_curve_feature`, `b3_futures_feature`, `b3_index_feature`, and
  `b3_index_composition_feature` families. Superseded raw DI, futures, index,
  and composition rows are defaults-off.
- BCB uses engineered `bcb_sgs_feature` rows as canonical first-model inputs.
  PTAX and Focus raw rows are defaults-off when `bcb_ptax_feature` and
  `bcb_focus_feature` replacements exist.
- FRED and IBGE use feature-id regexes because their emitted identifiers are
  dynamic across series, datasets, variables, and classifications. Upgraded
  `fred_rate_feature`, `fred_market_feature`, and `ibge_sidra_feature` rows are
  preferred over raw values.
- Tesouro, ANP, ONS, CVM, Novo CAGED, and Receita rules are keyed by emitted
  daily-long `source_family` and `value_name`. The upgraded
  `tesouro_feature`, `anp_fuel_feature`, `ons_power_feature`,
  `cvm_fund_feature`, `novo_caged_feature`, and `receita_feature` families are
  the model-default rows; raw daily-long rows remain available for audit.
- `br_rv_cross_feature` rows combine same-date upgraded/as-of inputs and carry
  their own explicit basis-point and return-style preprocessing rules.

Upgraded feature rows may also carry PIT policy metadata from their upstream
as-of inputs: `availability_policy`, `availability_basis`, `revision_policy`,
`vintage_id`, `model_usable`, and `model_usable_reason`. These fields document
when and why a deterministic feature row is usable; they are not preprocessing
instructions and do not relax the exact-one preprocessing rule.

Selected issue #67 source-family details are encoded directly in the rules:
PTAX spread rows are emitted in basis points, FRED WTI/Brent oil feature rows
are emitted as signed-log levels and signed-log changes, Focus standard
deviation is emitted as `std_dev_log1p`, physical ONS ENA rows use physical
log/log-change rules while percent-MLT rows use bounded percent rules, ONS
interchange feature flows are signed-log rows, CVM zero-capable flow/balance
logs are already log1p-safe, and Receita category-share rows use explicit total
denominators where available.

Unknown new feature patterns should fail coverage tests until a reviewed rule is
added.
