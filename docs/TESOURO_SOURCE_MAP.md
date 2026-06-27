# Tesouro source map

This source map covers the first Tesouro Nacional / Tesouro Transparente
structured ingestion scaffold for Brazil-RV. The implementation uses the
Tesouro Transparente CKAN API as the source of package and resource metadata,
keeps raw downloads immutable, and only normalizes fixture-tested CSV resources
to source-specific silver tables.

| dataset_id | priority | status | source_page_or_api | raw_format | expected_frequency | silver_output | known_limitations |
|---|---:|---|---|---|---|---|---|
| `tesouro_direto_prices_rates` | P0 | `live_download` | `taxas-dos-titulos-ofertados-pelo-tesouro-direto` via CKAN `package_show` | csv | daily | `data/silver/tesouro_direto_prices_rates/` | Historical CSV starts Jan 2002. Rows are date-only; available on next business day. |
| `tesouro_direto_sales` | P0 | `live_download` | `vendas-do-tesouro-direto` via CKAN `package_show` | csv | daily | `data/silver/tesouro_direto_sales/` | Historical CSV starts Jan 2002. Preserve official sales amounts and quantities only. |
| `tesouro_direto_redemptions` | P0 | `live_download` | `resgates-do-tesouro-direto` via CKAN `package_show` | csv_multi_resource | daily | `data/silver/tesouro_direto_redemptions/` | Three CSV resources are selected: maturities, early repurchases, and coupon payments. |
| `tesouro_direto_stock` | P0 | `live_download` | `estoque-do-tesouro-direto` via CKAN `package_show` | csv | monthly | `data/silver/tesouro_direto_stock/` | Monthly stock uses a conservative 30-calendar-day lag, then next business day. Stock is calculated by issuance rates, not market prices. |
| `tesouro_dpf_stock` | P0 | `live_download` | `estoque-da-divida-publica-federal` via CKAN `package_show` | csv | monthly | `data/silver/tesouro_dpf_stock/` | Monthly DPF stock uses a conservative 45-calendar-day lag, then next business day. |
| `tesouro_rtn_series` | P0 | `live_download_if_api_verified` | `resultado-do-tesouro-nacional` CKAN package; API resource points to Tesouro series-temporais docs | api_json_or_xlsx_pending | monthly | none | Kept pending until the API response schema is fixture-verified. XLSX parsing is not implemented in this PR. |
| `tesouro_dpf_emissions_redemptions` | P1 | `not_implemented_pending_xlsx_parser` | `emissoes-e-resgates-divida-publica-federal` | xlsx_pending_parser | monthly | none | Observed primary data resource is XLSX. No XLSX parser is added here. |
| `tesouro_direto_operations` | P1 | `feature_gated_large_files` | `operacoes-do-tesouro-direto` | zip_or_gz_csv_large | monthly | none | Huge annual operation files from 2014. Not default-ingested; future explicit-only backfill should aggregate before modeling. |
| `tesouro_direto_investors` | P2 | `source_map_only_large_privacy_sensitive` | `investidores-do-tesouro-direto` | zip_or_gz_csv_large | annual | none | Very large investor/profile files. Source-map only until aggregation and privacy review. |
| `tesouro_capag_states` | P1 | `source_map_only_p1` | `capag-estados` | csv_multi_resource | quadrimestral | none | CSV resources exist, but this first PR keeps CAPAG Estados source-map only. |
| `tesouro_capag_municipalities` | P2 | `not_implemented_pending_xlsx_parser` | `capag-municipios` | xlsx_pending_parser | quadrimestral | none | Observed resources are XLSX; no XLSX parser is added here. |
| `tesouro_auction_calendar_results` | P1 | `not_implemented_pending_endpoint` | pending stable Tesouro/BCB endpoint | pending_endpoint | event | none | Source-map only until a stable public auction calendar/results source is confirmed. |

## Official source facts

- Tesouro Transparente is a CKAN-backed open-data portal. Dataset resources are
  discovered through `https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id={package_id}`.
- Tesouro Direto prices/rates, sales, redemptions, Tesouro Direto stock, and DPF
  stock expose CKAN CSV resources suitable for deterministic raw downloads.
- The RTN CKAN package exposes an API documentation resource and XLSX resources.
  This PR keeps RTN pending because the API response schema is not fixture-verified
  and XLSX parsing is intentionally out of scope.
- DPF emissions/redemptions and CAPAG municipalities are XLSX-only for this first
  pass. Investor and operation files are large ZIP/GZ/CSV resources and are not
  default-ingested.

## Availability policy

- Daily Tesouro Direto market/flow observations use
  `available_date = next_business_day(ref_date)` because the source rows are
  date-only.
- Monthly Tesouro Direto stock uses
  `available_date = next_business_day(ref_date + 30 calendar days)`.
- Monthly DPF stock uses
  `available_date = next_business_day(ref_date + 45 calendar days)`.
- The downloader never uses download date as historical availability and never
  assumes same-day availability for month-end observations.
