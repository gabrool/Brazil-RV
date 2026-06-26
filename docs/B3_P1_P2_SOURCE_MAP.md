# B3 P1/P2 Source Map

This source map extends the P0 B3 ingestion spine. It records which P1/P2 datasets are live-downloadable now, which are raw-only snapshots, and which remain pending until a stable free B3 URL template is confirmed.

Statuses:

- `live_download`: config owns a stable free URL template and code may download it.
- `manual_source_map`: manual/source-map input is expected before live download.
- `raw_only`: raw snapshot and manifest are useful, parsing is intentionally minimal.
- `not_implemented_pending_url`: no live downloader may create an HTTP client until a stable URL is confirmed.

| dataset_id | priority | status | source_url_or_page | raw_format | expected_frequency | canonical_or_silver_output | known_limitations |
|---|---|---|---|---|---|---|---|
| b3_cotahist_daily | P1 | not_implemented_pending_url | https://www.b3.com.br/en_us/market-data-and-indices/data-services/market-data/historical-data/equities/historical-quote-data/ documents historical quote files and layout; no official stable daily `COTAHIST_D{ddmmyyyy}.ZIP` endpoint evidence is recorded. | fixed_width_zip | daily | data/silver/b3_cotahist_daily/ | Parser fixtures use the COTAHIST fixed-width layout, but live daily download remains disabled until a stable public endpoint is confirmed. |
| b3_indexes_current_portfolio | P1 | not_implemented_pending_url | https://www.b3.com.br/pt_br/market-data-e-indices/indices/indices-amplos/ibovespa.htm and index composition pages | csv_or_html | periodic | data/silver/b3_indexes_current_portfolio/ | Dynamic index pages exist, but no stable direct free file template is confirmed. |
| b3_indexes_theoretical_portfolio | P1 | not_implemented_pending_url | B3 index/theoretical portfolio pages under Market Data e Indices | csv_or_html | periodic | data/silver/b3_indexes_theoretical_portfolio/ | Keep separate from current portfolio; pending stable direct free URL. |
| b3_equities_investor_participation | P1 | not_implemented_pending_url | B3 public participation/statistics pages and daily bulletin search | csv_or_xlsx_or_html | daily_or_monthly | data/silver/b3_equities_investor_participation/ | Stable bulk report URL not confirmed; use fixtures/manual rows only until confirmed. |
| b3_foreign_investor_movement | P1 | not_implemented_pending_url | B3 public foreign-investor statistics pages and daily bulletin search | csv_or_xlsx_or_html | daily_or_monthly | data/silver/b3_foreign_investor_movement/ | Keep separate from general participation; stable bulk report URL not confirmed. |
| b3_daily_bulletin_chapters | P1 | not_implemented_pending_url | https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/boletins-diarios/pesquisa-por-pregao/pesquisa-por-pregao/ | pdf_or_html_or_xlsx_or_xml | daily | data/bronze/b3/b3_daily_bulletin_chapters/ | Page lists useful price, instrument, fee, risk, FX, derivatives, swap, government-securities, and scenario/risk-matrix files; stable direct download URLs are not confirmed. |
| b3_isin_database | P1 | not_implemented_pending_url | B3 public ISIN search/data hub references | csv_or_xlsx_or_html | periodic | data/silver/b3_isin_database/ | Stable free bulk URL not confirmed. |
| b3_trading_parameters | P1 | not_implemented_pending_url | Daily bulletin files such as Tradable Security List and Instrument Group Parameters | csv_or_xlsx_or_html_or_xml | periodic | data/silver/b3_trading_parameters/ | Bulletin lists candidate files, but stable direct URL template is not confirmed. |
| b3_fee_schedules | P1 | raw_only | https://www.b3.com.br/pt_br/produtos-e-servicos/tarifas/ | html_or_pdf | periodic | data/bronze/b3/b3_fee_schedules/ and data/silver/b3_fee_schedules/ where simple tables parse | Raw snapshots are primary; parse only reliable HTML tables from stable public pages. |
| b3_market_data_public_reports | P2 | not_implemented_pending_url | B3 daily bulletin search and Market Data historical pages | mixed | mixed | data/bronze/b3/b3_market_data_public_reports/ | Source-map subreports include cash, derivatives, FX, securities lending, risk/scenario, swap, government-securities, and fee/instrument public files; do not enable live downloads without direct URL templates. |
| b3_derivatives_reference_prices | P2 | not_implemented_pending_url | Daily bulletin option/reference premium and reference price scenario reports | csv_or_html_or_xml | daily | data/silver/b3_derivatives_reference_prices/ | Stable direct free URL not confirmed. |
| b3_product_specs_pages | P2 | raw_only | B3 product/spec/methodology pages for DI1, DAP, DDI, FRC, DOL, WDO, IND, WIN, relevant options, COTAHIST, and index methodology. | html_or_pdf | static_or_periodic | data/bronze/b3/b3_product_specs_pages/ and data/silver/b3_product_specs_pages/ | Raw snapshots only; fee/tariff pages belong under `b3_fee_schedules`; parse simple metadata, no OCR. |
| b3_corporate_actions_public | P2_candidate | not_implemented_pending_url | B3 issuer/corporate-actions public pages | csv_or_html_or_pdf | periodic | data/silver/b3_corporate_actions_public/ | Candidate only; free bulk/history URL not confirmed. |
| b3_securities_lending_public | P2_candidate | not_implemented_pending_url | https://www.b3.com.br/pt_br/produtos-e-servicos/emprestimo-de-ativos/informacoes.htm | csv_or_html | daily_or_periodic | data/silver/b3_securities_lending_public/ | Candidate only; free bulk/history URL not confirmed. |
| b3_margin_and_risk_parameters | P2_candidate | not_implemented_pending_url | B3 daily bulletin risk, margin, primitive risk factor, and scenario files | csv_or_xlsx_or_xml | daily_or_periodic | data/silver/b3_margin_and_risk_parameters/ | Candidate only; stable direct URL not confirmed. |
| b3_index_methodology_and_divisors | P2_candidate | raw_only | B3 index methodology and index pages under Market Data e Indices | html_or_pdf | static_or_periodic | data/silver/b3_index_methodology_and_divisors/ | Candidate only; raw metadata snapshots are acceptable if later added to registry. |

## Product/Spec Raw Page Audit

The `b3_product_specs_pages` registry intentionally points only at product,
contract, specification, or methodology pages. It excludes fee/tariff pages,
which belong under `b3_fee_schedules`.

Stable public pages currently included:

- DI1: `Futuro de Taxa Média de Depósitos Interfinanceiros de Um Dia`
- DAP: `Futuro de Cupom de IPCA`
- DDI: `Futuro de Cupom Cambial de Depósitos Interfinanceiros de Um Dia`
- FRC: `Operação Estruturada de FRA de FRC (PU Neutro)`
- DOL/WDO: `Futuro de Taxa de Câmbio de Reais por Dólar Comercial`
- IND/WIN: `Futuro de Ibovespa`
- D11-D19 options: `Opções sobre Futuro de DI`
- WDO options: `Mini Opções sobre Taxa de Câmbio de Reais por Dólar Comercial`
- IBOV options: `Opções Semanais sobre Ibovespa`
- COTAHIST: B3 Historical Quote Data page
- Ibovespa methodology/index page: recorded as index-methodology metadata, not
  as an IND/WIN futures product spec.

## Daily Bulletin Subreport Audit

The B3 daily bulletin search page currently exposes report names including:

- `BVBG.087.01 IndexReport`
- `BVBG.086.01 PriceReport`
- `BVBG.186.01 Simplified Price Report - Equities`
- `BVBG.187.01 Simplified Price Report - Derivatives`
- `BVBG.028.02 Instruments File`
- `BVBG.029.02 Instruments File`
- `BVBG.044.01 Fee Daily Unit Cost`
- `BVBG.043.01 Fee Unit Cost`
- `Standardized Instrument Groups`
- `Primitive Risk Factors`
- `Risk Formulas`
- `Fee Variables`
- `Daily Liquidity Limit`
- `Maximum Theoretical Margin`
- `Tradable Security List`
- `Instrument Group Parameters`
- `Equities Market - Equities Option Reference Premiums`
- `FX Market - Traded Rates, Opening Parameters and Contracted Transactions`
- `FX Market - Volume Settled on a Net Basis`
- `Derivatives Market - Option Reference Premiums`
- `Derivatives Market - Margin Scenarios`
- `Derivatives Market - Economic Indicators`
- `Derivatives Market - Agricultural Indicators`
- `Derivatives Market - Swap Mark-to-Market`
- `Derivatives Market - Swap Market Rates`
- `Securities Market - Government Securities Reference Prices`
- `Scenario and risk-matrix files`

These are useful P1/P2 candidates, but the ingestion code must keep them `not_implemented_pending_url` until a repeatable direct download endpoint is confirmed and owned by dataset config.
