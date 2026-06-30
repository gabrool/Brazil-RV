# Feature Preprocessing Metadata Contract

This contract records preprocessing intent for model-facing features that the repo
currently emits. It is metadata only: it does not transform values, fit scalers,
build tensors, create targets, or add new deterministic features.

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

## Source-Family Decisions

The metadata config follows the issue #65 source-family contract:

- B3 rules cover current research panels only; no B3 features are created here.
- BCB uses engineered SGS features as canonical first-model inputs, while PTAX
  and Focus rows receive source-family-specific preprocessing metadata.
- FRED and IBGE use feature-id regexes because their emitted identifiers are
  dynamic across series, datasets, variables, and classifications.
- Tesouro, ANP, ONS, CVM, Novo CAGED, and Receita rules are keyed by emitted
  daily-long `source_family` and `value_name`, with unit-specific handling for
  ONS ENA percent-MLT rows.

Unknown new feature patterns should fail coverage tests until a reviewed rule is
added.
