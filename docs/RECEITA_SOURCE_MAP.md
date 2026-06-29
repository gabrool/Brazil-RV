# Receita Federal Source Map

Receita Federal do Brasil is the Brazilian federal revenue agency and administers federal tax collection and customs functions. This PR adds a lean source-specific ingestion path for official monthly federal tax collection only.

Official pages used:

- Receita Federal homepage: https://www.gov.br/receitafederal/
- Receita Dados Abertos: https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos
- Receita Arrecadacao category: https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/arrecadacao
- dados.gov.br landing for Resultado da Arrecadacao: https://dados.gov.br/dados/conjuntos-dados/resultado-da-arrecadacao

Monthly collection data describes an economic reference month, but publication happens after the month closes. Until official historical publication dates are fixture-verified, the silver table uses `receita_monthly_collection_conservative_next_month_end_plus_5bd`: last business day of the next month plus five business days. Download timestamps are never used as historical availability.

| dataset_id | priority | status | source_page_or_endpoint | raw_format | expected_frequency | silver_output | known_limitations |
|---|---:|---|---|---|---|---|---|
| `receita_tax_collection_monthly` | P0 | live_download | Receita Arrecadacao category and `https://dados.gov.br/dados/conjuntos-dados/resultado-da-arrecadacao` | official structured tabular, CSV/TXT/ZIP preferred | monthly | `data/silver/receita_tax_collection_monthly/` | Link discovery is fixture-tested against official page HTML. PDFs are ignored. XLSX/ODS is unsupported unless a future official fixture requires `fastexcel`. |
| `receita_tax_collection_by_state_monthly` | P1 | source_map_only_unless_fixture_verified | Receita/dados.gov official pages, not fixture-verified as a deterministic structured state dataset in this PR | pending structured tabular | monthly or annual | none in this PR | State attribution for federal taxes can be economically noisy because some taxes are centralized or attributed to headquarters/financial institutions. |
| `receita_tax_expenditures_annual` | P1 | source_map_only_slow_fiscal | https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/beneficios-e-renuncias-fiscais | structured or report pending | annual | none in this PR | Useful for fiscal-policy regime and revenue risk, but slower/report-like. |
| `receita_fiscal_benefits_reports` | P1 | source_map_only_report | Receita benefits/renunciation pages | PDF or XLSX pending | annual or irregular | none in this PR | No PDF parsing; defer report semantics. |
| `receita_tax_burden_annual` | P2 | source_map_only_structural | https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/carga-tributaria | structured or report pending | annual | none in this PR | Structural annual context, not first-pass daily model input. |
| `receita_monthly_collection_reports_pdf` | P2 | source_map_only_pdf_metadata | Receita Arrecadacao/report pages | PDF | monthly | none in this PR | Narrative reports are useful for audit/release metadata, but are not parsed. |
| `receita_cnpj_open_data_reference` | P2 | source_map_only_reference | https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/cadastros | large ZIP CSV | snapshot or monthly | none in this PR | Reference/entity mapping only; not tax-collection ingestion. |
| `receita_simples_nacional_collection` | P2 | source_map_only_specialized | Receita/Simples official context | structured pending | monthly | none in this PR | Dedicated source semantics are required. |
| `receita_refis_installment_programs` | P3 | source_map_only_policy_events | Receita parcelamento/transacao context | mixed pending | event or monthly | none in this PR | Policy-event context deferred. |
| `receita_customs_trade_tax_collection` | P3 | source_map_only_specialized | https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/comercio-exterior | structured pending | monthly | none in this PR | Customs/tax specialty context deferred; Comex/BCB cover first-pass trade context. |

Ingestion scope is intentionally narrow. Silver preserves official nominal collection rows in long form. It does not compute inflation adjustment, real values, growth rates, year-over-year changes, revenue surprises, ratios, fiscal labels, rolling features, seasonal adjustment, model targets, or backtest fields.
