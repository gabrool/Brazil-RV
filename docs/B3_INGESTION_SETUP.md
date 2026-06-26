# B3 ingestion setup plan

This is the immediate setup plan for the B3 data spine. The goal is to support a free-data, daily-frequency, multi-asset Brazilian futures model before any model research begins.

## 1. Scope

The B3 pipeline must download, parse, normalize, and quality-check all useful free B3 daily datasets for:

- Futures labels and returns.
- Contract rolls and maturity metadata.
- DI/rates curve features.
- FX and equity-index futures features.
- Liquidity, open interest, volume, and participation features.
- Equity/index context for Ibovespa futures.

No intraday or paid B3 data is in scope for this phase.

## 2. Engineering constraints

The implementation must follow `docs/ENGINEERING_GUIDELINES.md`.

Key constraints:

- Hardware constrained by default.
- Chunk large files.
- Store parsed outputs as Parquet.
- Prefer `polars`, `pyarrow`, and `duckdb` for large tabular work.
- Keep code lean; no speculative abstractions.
- Make downloads and parsing idempotent.
- Do not commit downloaded data.

## 3. B3 datasets to cover

### P0: required before first model

| Dataset id | Purpose |
|---|---|
| `b3_futures_settlements` | Daily futures settlement prices for DI1, DOL/WDO, IND/WIN, DAP, DDI, FRC, and other liquid futures where freely available. |
| `b3_futures_contract_master` | Contract codes, maturity-code parsing, expiry dates, multipliers, tick sizes, quote conventions, and settlement conventions. |
| `b3_holiday_calendar` | Trading-day alignment, roll logic, carry, and forward returns. |
| `b3_derivatives_open_interest` | Liquidity filters, crowding proxy, roll selection. |
| `b3_derivatives_trade_summary` | Daily derivative product activity and volume. |
| `b3_reference_rates` | B3 reference rates and curves for PRE/DI and related rates features. |
| `b3_cotahist_yearly` | Listed-market daily quote history for equities, ETFs, FIIs, BDRs, options, and related instruments. |
| `b3_indexes_historical_data` | Historical index levels for Ibovespa, IBrX, SMLL, IFIX, and sector/style indices. |
| `b3_indexes_composition` | Index composition and portfolio metadata for Ibovespa exposure context. |
| `b3_traded_securities` | Security master and identifier mapping. |

### P1: add immediately after P0 spine works

| Dataset id | Purpose |
|---|---|
| `b3_cotahist_daily` | Incremental daily listed-market updates. |
| `b3_indexes_current_portfolio` | Current index weights and constituents. |
| `b3_indexes_theoretical_portfolio` | Historical/theoretical index composition where freely available. |
| `b3_equities_investor_participation` | Local/foreign/retail/institutional participation features. |
| `b3_foreign_investor_movement` | Foreign-flow features. |
| `b3_daily_bulletin_chapters` | Cross-checks and additional public market statistics. |
| `b3_isin_database` | Identifier crosswalk enrichment. |
| `b3_trading_parameters` | Trading parameter metadata. |
| `b3_fee_schedules` | Cost assumptions and fee metadata. |

### P2: include if simple after P0/P1

| Dataset id | Purpose |
|---|---|
| `b3_market_data_public_reports` | Any other public daily B3 reports that are clearly useful and easy to parse. |
| `b3_derivatives_reference_prices` | Additional derivative reference-price cross-checks if freely accessible. |
| `b3_product_specs_pages` | Static product-rule snapshots for documentation and validation. |

## 4. Repo setup before first downloader

Create these files before implementation:

```text
pyproject.toml
.gitignore
configs/project.yaml
configs/paths.yaml
configs/instruments.yaml
configs/datasets/b3.yaml
```

Create code folders only when they receive real code:

```text
src/bralpha/infra/
src/bralpha/metadata/
src/bralpha/domain/
src/bralpha/ingestion/b3/
src/bralpha/parsing/
src/bralpha/normalization/
src/bralpha/quality/
```

Do not create empty packages just to create structure.

## 5. Local data layout

All local data paths should come from `configs/paths.yaml`.

```text
data/raw/b3/{dataset_id}/{download_date}/...
data/bronze/b3/{dataset_id}/...
data/silver/{canonical_table}/...
data/gold/...
data/manifests/b3/...
```

Large outputs should be partitioned by natural keys, usually year or reference date.

## 6. Manifest requirements

Every raw download must write a manifest row with:

```text
dataset_id
source
source_url
request_params
download_timestamp_utc
http_status
content_type
file_size_bytes
sha256
raw_path
license_note
success
error_message
```

The manifest is required even for failed downloads.

## 7. Canonical B3 silver schemas

### `market_daily`

```text
ref_date
available_date
source
source_dataset
asset_id
contract_id
symbol
commodity
maturity_code
asset_class
open
high
low
close
settlement
previous_settlement
price_change
settlement_value
volume
financial_volume
number_of_trades
open_interest
currency
unit
source_version
```

### `curve_daily`

```text
ref_date
available_date
source
source_dataset
curve_id
tenor_days
forward_date
rate
rate_type
compounding
business_day_basis
currency
source_version
```

### `reference_contract`

```text
contract_id
symbol_root
commodity
asset_class
maturity_code
maturity_date
first_trade_date
last_trade_date
expiry_date
contract_multiplier
tick_size
currency
quote_convention
settlement_method
source
source_version
```

### `reference_security`

```text
security_id
symbol
isin
name
market_type
asset_class
issuer
currency
source
source_version
```

## 8. Dataset config contract

Each B3 dataset entry in `configs/datasets/b3.yaml` must include:

```yaml
dataset_id: b3_futures_settlements
source: b3
priority: P0
frequency: daily
free_access: true
requires_auth: false
raw_format: html_or_zip_or_csv_or_json
canonical_table: market_daily
point_in_time_required: true
primary_keys: []
source_urls: []
quality_checks: []
notes: ""
```

## 9. First implementation order

1. Infrastructure: paths, config loading, HTTP client, hashing, manifest writer.
2. Domain: B3 calendar, maturity-code parser, instrument registry.
3. First downloader: `b3_futures_settlements`.
4. First parser and normalizer into `market_daily`.
5. Quality checks and tests.
6. Add open interest and trade summary.
7. Add COTAHIST parser.
8. Add index data and composition.
9. Add investor participation and other B3 reports.

## 10. Testing checklist

Minimum tests:

- Config files parse.
- Paths resolve without creating data in the repo.
- URL construction is deterministic.
- Hashing is stable.
- Manifests are written for success and failure.
- Downloads are idempotent.
- COTAHIST fixed-width parsing works on a tiny fixture.
- Contract maturity parsing works for all B3 month codes.
- Silver tables have no duplicate primary keys.
- `available_date` is never later than the model `asof_date` in feature-building tests.

## 11. Definition of done for a B3 dataset

A B3 dataset is not done until it has:

- A config entry.
- A downloader or documented manual source step.
- Raw manifest logging.
- A parser into bronze Parquet where applicable.
- A normalizer into silver Parquet where applicable.
- Data-quality checks.
- Tests.
- Source notes and known limitations.
