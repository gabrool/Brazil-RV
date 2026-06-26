# BCB source map

This source map covers structured Banco Central do Brasil inputs for the
raw -> bronze -> source-specific silver spine. It intentionally excludes NLP,
PDF parsing, and scraping-heavy policy text work.

| dataset_id | priority | status | source | documented_endpoint_or_page | raw_format | expected_frequency | silver_output | known_limitations |
|---|---:|---|---|---|---|---|---|---|
| `bcb_sgs_series` | P0 | `live_download` | BCB SGS | `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados` | JSON | mixed | `data/silver/bcb_sgs_series/` | Only configured, verified SGS IDs are active. Unknown availability policies make rows not model-usable. |
| `bcb_ptax_exchange_rates` | P0 | `live_download` | BCB PTAX OData | `Currencies`, `DollarRatePeriod`, `ExchangeRatePeriod` | OData JSON | daily | `data/silver/bcb_ptax_exchange_rates/` | Uses JSON only. Multiple daily bulletins are retained with a selected/latest bulletin flag. |
| `bcb_focus_expectations` | P0 | `live_download` | BCB Focus OData | `ExpectativaMercadoMensais`, `ExpectativasMercadoSelic`, `ExpectativasMercadoTrimestrais`, `ExpectativasMercadoAnuais`, `ExpectativasMercadoInflacao12Meses`, `ExpectativasMercadoInflacao24Meses` | OData JSON | daily_or_weekly_publication | `data/silver/bcb_focus_expectations/` | First pass sets `available_date = Data`; no institution-level expectations resource is used. |
| `bcb_focus_top5_expectations` | P0 | `live_download` | BCB Focus OData | `ExpectativasMercadoTop5Mensais`, `ExpectativasMercadoTop5Selic`, `ExpectativaMercadoTop5Trimestral`, `ExpectativasMercadoTop5Anuais`, `ExpectativasMercadoTop5Inflacao12Meses`, `ExpectativasMercadoTop5Inflacao24Meses` | OData JSON | daily_or_weekly_publication | `data/silver/bcb_focus_top5_expectations/` | First pass preserves Top5 calculation type and does not infer institution identities. |
| `bcb_focus_top5_reference_dates` | P0 | `live_download` | BCB Focus OData | `DatasReferencia` | OData JSON | periodic | `data/silver/bcb_focus_top5_reference_dates/` | Normalizes only explicit reference-date fields. |
| `bcb_copom_calendar` | P1 | `not_implemented_pending_url` | BCB | pending stable non-JS endpoint | html_or_json_pending | event | none | Source-map only until a stable structured endpoint is confirmed. |
| `bcb_copom_decisions` | P1 | `not_implemented_pending_url` | BCB | pending stable endpoint | html_or_json_pending | event | none | Source-map only until a stable structured endpoint is confirmed. |
| `bcb_copom_documents` | P2 | `raw_only_or_pending_url` | BCB | Copom statements/minutes pages | html_or_pdf | event | none | No NLP and no PDF parsing in this PR. |
| `bcb_monetary_policy_reports` | P2 | `raw_only_or_pending_url` | BCB | monetary policy report pages | pdf_or_html | quarterly | none | No NLP and no PDF parsing in this PR. |
| `bcb_speeches_press_releases` | P2 | `not_implemented_pending_url` | BCB | speeches and press-release pages | html | event | none | Metadata/source-map only for now. |

Official BCB references used for the P0 endpoints:

- SGS JSON resources document the `bcdata.sgs.{codigo_serie}/dados` pattern
  with `formato`, `dataInicial`, and `dataFinal` query parameters.
- PTAX Open Data documents the OData pattern and the `Currencies`,
  `DollarRatePeriod`, and `ExchangeRatePeriod` endpoint families.
- Focus Open Data documents the OData resources listed above and identifies the
  institution-level expectations resource as deactivated.
