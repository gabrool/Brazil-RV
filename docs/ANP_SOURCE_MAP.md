# ANP Source Map

ANP is used here as official-source energy and fuel context for the Brazil-RV
macro pipeline. This PR implements only raw -> bronze -> source-specific silver
ingestion. It does not build research panels, model features, derived price
indices, spreads, pass-through measures, inflation proxies, returns, ratios,
rolling features, stress labels, BOE conversions, or portfolio/backtest fields.

Fuel-price ingestion intentionally uses the non-overlapping historical split:
semiannual automotive/GLP files through 2022 and monthly fuel-family files from
2023 onward. The official page also lists recent semiannual groupings; those are
not ingested here to avoid duplicate observations. The four-latest-weeks files
are also excluded because they overlap with monthly files.

| dataset_id | priority | status | source_page_or_endpoint | raw_format | expected_frequency | silver_output | known_limitations |
|---|---:|---|---|---|---|---|---|
| anp_fuel_prices_weekly | P0 | live_download | https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis | mixed_csv_zip | weekly survey, delivered as semiannual/monthly files | data/silver/anp_fuel_prices_weekly/ | Station/product microdata only; purchase price is available only through August 2020; no four-latest-weeks overlap files. |
| anp_fuel_sales_monthly | P0 | live_download | https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/vendas-de-derivados-de-petroleo-e-biocombustiveis | csv | monthly | data/silver/anp_fuel_sales_monthly/ | Uses the current official linked historical CSV for distributor sales volumes in cubic meters. |
| anp_oil_gas_production_monthly | P1 | live_download | https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-petroleo-e-gas-natural-por-estado-e-localizacao | csv_multi_resource | monthly | data/silver/anp_oil_gas_production_monthly/ | Production/gas resource families stay long; no BOE conversion, rates, shares, or Petrobras labels. |
| anp_downstream_movements | P1 | source_map_only_high_dimensional | ANP downstream movements open-data page | zip_csv | monthly | none | Broader/high-dimensional; fuel sales is the first-pass demand signal. |
| anp_government_take_royalties | P2 | source_map_only_slow_fiscal | https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/participacoes-governamentais | csv_or_xlsx_by_year | annual | none | Fiscal context is fragmented and slower; deferred. |
| anp_government_take_special_participation | P2 | source_map_only_slow_fiscal | https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/participacoes-governamentais | mixed_pending | annual_or_quarterly | none | Special participation files need a dedicated PR. |
| anp_reference_oil_price | P2 | source_map_only_petroleum_reference | https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/participacoes-governamentais | csv_or_xlsx_by_year | monthly_or_annual | none | Useful later for Petrobras/fiscal context; source-map only. |
| anp_reference_gas_price | P2 | source_map_only_petroleum_reference | https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/participacoes-governamentais | csv_or_xlsx_by_year | monthly_or_annual | none | Useful later for Petrobras/fiscal context; source-map only. |
| anp_fuel_station_registry | P2 | source_map_only_reference | ANP fuel station registry | mixed_pending | snapshot | none | Reference data; not a first-pass model feature. |
| anp_glp_reseller_registry | P2 | source_map_only_reference | ANP GLP reseller registry | mixed_pending | snapshot | none | Reference data; deferred. |
| anp_distributor_registry | P2 | source_map_only_reference | ANP distributor registry | mixed_pending | snapshot | none | Reference data; deferred. |
| anp_storage_capacity | P2 | source_map_only_reference | ANP storage/tankage pages | mixed_pending | snapshot | none | Specialized reference data; deferred. |
| anp_import_export_authorizations | P3 | source_map_only_specialized | ANP import/export authorization pages | mixed_pending | event_or_snapshot | none | Specialized authorization data; deferred. |
| anp_quality_monitoring_pmqc | P3 | source_map_only_specialized | ANP PMQC pages | mixed_pending | mixed | none | Needs dedicated quality-monitoring parser/normalizer. |
| anp_fuel_stock_data | P3 | source_map_only_specialized | ANP fuel stock pages | mixed_pending | mixed | none | Specialized stock data; deferred. |
| anp_renovabio_cbio | P3 | source_map_only_specialized | ANP RenovaBio/CBIO pages | mixed_pending | mixed | none | Specialized environmental-credit data; deferred. |

Official source basis:

- The fuel-price page says ANP monitors automotive fuel and GLP P13 reseller
  prices through a weekly survey and publishes the historical series as CSV
  files. Metadata lists gasoline C, hydrated ethanol, diesel B, GNV, and GLP
  P13; `Valor de Compra` exists only until August 2020.
- The fuel-sales page says sales data are updated monthly through the last day
  of the month after the reference month and metadata defines `VENDAS` as cubic
  meters.
- The production page lists petroleum, LGN, natural gas, reinjection, flaring
  and losses, own consumption, and available gas resources, updated monthly
  through the last day of the month after the reference month.
- The government-take page describes royalties, special participation, and
  reference oil/gas price files as financial/fiscal context; these stay deferred.
