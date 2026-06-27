# CVM Source Map

This PR adds CVM fund-flow-first structured ingestion for Brazil-RV. The live
datasets use deterministic direct files from the Portal Dados Abertos CVM. The
implementation keeps raw files immutable, parses source-specific bronze tables,
and normalizes only fixture-tested fund daily report and fund registry fields to
source-specific silver.

| dataset_id | priority | status | source_page_or_endpoint | raw_format | expected_frequency | silver_output | known_limitations |
|---|---:|---|---|---|---|---|---|
| `cvm_fund_daily_reports` | P0 | `live_download` | `fi-doc-inf_diario`; `INF_DIARIO/DADOS/` monthly ZIPs and `INF_DIARIO/DADOS/HIST/` annual ZIPs | zip_csv | daily | `data/silver/cvm_fund_daily_reports/` | Administrators have one business day to send reports; first-pass availability is conservative two business days after `ref_date`. |
| `cvm_fund_registry_current` | P0 | `live_download` | `fi-cad`; direct `cad_fi.csv` | csv | daily_snapshot | `data/silver/cvm_fund_registry_current/` | Current snapshot/reference data, not a historical model feature by itself. |
| `cvm_fund_registry_history` | P0 | `raw_bronze_only_pending_normalizer` | `fi-cad`; direct `cad_fi_hist.zip` | zip_csv | daily | `data/bronze/cvm/cvm_fund_registry_history/` | Live raw/bronze download only; silver deferred until the official multi-file historical schema is fixture-verified. |
| `cvm_fund_class_registry` | P0 | `raw_bronze_only_pending_normalizer` | `fi-cad`; direct `registro_fundo_classe.zip` | zip_csv | daily_snapshot | `data/bronze/cvm/cvm_fund_class_registry/` | Live raw/bronze download only; silver deferred until the `registro_fundo.csv` / `registro_classe.csv` / `registro_subclasse.csv` layout is handled explicitly. |
| `cvm_fund_portfolio_cda` | P1 | `source_map_only_feature_gated_large_files` | `fi-doc-cda` | zip_csv_large | monthly | none | Large monthly holdings files with confidentiality mechanics; future feature-gated PR. |
| `cvm_company_ipe_metadata` | P1 | `source_map_only_metadata` | `cia_aberta-doc-ipe` | zip_csv_metadata_pending | event | none | Future metadata-only event source; no document body or free-text parsing here. |
| `cvm_fii_reports` | P1 | `not_implemented_pending_endpoint` | pending dedicated endpoint verification | pending_endpoint | monthly_or_event | none | Useful yield-product stress source; future dedicated PR. |
| `cvm_fidc_reports` | P1 | `not_implemented_pending_endpoint` | pending dedicated endpoint verification | pending_endpoint | monthly | none | Useful private-credit stress source; future dedicated PR. |
| `cvm_company_itr` | P2 | `source_map_only_slow_company_fundamentals` | `cia_aberta-doc-itr` | zip_csv_large | quarterly | none | Slower company fundamentals, not first-pass daily RV inputs. |
| `cvm_company_dfp` | P2 | `source_map_only_slow_company_fundamentals` | `cia_aberta-doc-dfp` | zip_csv_large | annual | none | Annual company fundamentals; defer. |
| `cvm_public_offerings` | P2 | `source_map_only_feature_gated` | pending dedicated endpoint verification | pending_endpoint | event | none | Capital-market risk appetite source; defer. |
| `cvm_sanctions_regulatory_events` | P3 | `source_map_only_low_priority` | pending dedicated endpoint verification | pending_endpoint | event | none | Regulatory stress source; lower priority. |

## Official Source Facts

- CVM's Informe Diario page states the daily fund report contains portfolio
  value, net assets, quota value, daily subscriptions, daily redemptions, and
  number of shareholders.
- CVM states current and previous month daily report files are updated daily,
  older recent files are updated weekly, and administrators have one business
  day to send the report to CVM.
- Direct daily report files are available as annual historical ZIPs for
  2000-2020 and monthly ZIPs from 2021 onward.
- CVM's fund registry page covers fund CNPJ, registration date, status,
  fund/class/subclass data, and historical ICVM 555 changes. The direct registry
  directory contains `cad_fi.csv`, `cad_fi_hist.zip`, and
  `registro_fundo_classe.zip`. In this PR, `cad_fi_hist.zip` and
  `registro_fundo_classe.zip` are raw/bronze-only pending a fixture-verified
  silver normalizer for their multi-file official layouts.

## Availability Policy

`cvm_fund_daily_reports` uses `cvm_fund_daily_conservative_2bd`:

```text
available_date = add_business_days(ref_date, 2)
```

This rule reflects the official one-business-day administrator submission
window plus a conservative portal processing lag. Download timestamps are never
used as historical availability.

## Deferred Scope

This PR does not add CDA holdings ingestion, IPE document parsing, FII/FIDC
report parsing, DFP/ITR fundamentals, offerings, sanctions, PDF parsing, text
features, NLP, gold panels, modeling, backtesting, or portfolio logic.
No fake endpoints are configured for deferred datasets.
