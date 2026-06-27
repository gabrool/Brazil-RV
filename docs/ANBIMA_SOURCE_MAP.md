# ANBIMA source map

This source map covers the conditional ANBIMA raw -> bronze ->
source-specific silver ingestion scaffold. ANBIMA public pages expose useful
fixed-income and projection data families, but this PR does not mark any
dataset as live because no stable free direct JSON/CSV/TXT endpoint or static
file contract was verified from official public documentation.

| dataset_id | priority | status | official_page_or_endpoint | raw_format | expected_frequency | silver_output | known_limitations |
|---|---:|---|---|---|---|---|---|
| `anbima_sovereign_secondary_market` | P0 | `not_implemented_pending_endpoint` | `https://www.anbima.com.br/pt_br/informar/taxas-de-titulos-publicos.htm` and `https://data.anbima.com.br/` | structured_json_csv_or_txt_pending | daily | `data/silver/anbima_sovereign_secondary_market/` | Public page and ANBIMA Data link are mapped, but no stable direct structured free endpoint/file is committed. |
| `anbima_sovereign_yield_curves` | P0 | `not_implemented_pending_endpoint` | `https://data.anbima.com.br/` | structured_json_csv_or_txt_pending | daily | `data/silver/anbima_sovereign_yield_curves/` | Curves are conditional-live only until fixed/pre, real, and breakeven structured endpoints are verified. |
| `anbima_vna` | P0 | `not_implemented_pending_endpoint` | `https://data.anbima.com.br/` | structured_json_csv_or_txt_pending | daily | `data/silver/anbima_vna/` | VNA remains pending until a stable free direct endpoint/file is verified. |
| `anbima_fixed_income_indices` | P0 | `not_implemented_pending_endpoint` | `https://www.anbima.com.br/pt_br/informar/indices.htm` and `https://data.anbima.com.br/` | structured_json_csv_or_txt_pending | daily | `data/silver/anbima_fixed_income_indices/` | IMA, IRF-M, IMA-B, IMA-S, and IDkA families are mapped, but no live endpoint is committed. |
| `anbima_inflation_projections` | P0 | `not_implemented_pending_endpoint` | `https://www.anbima.com.br/pt_br/informar/projecoes-ipca-e-igp-m.htm` | structured_json_csv_or_txt_pending | monthly_or_event | `data/silver/anbima_inflation_projections/` | Page states projections are based on ANBIMA macro consultative group consensus; structured endpoint remains pending. |
| `anbima_debenture_secondary_market` | P1 | `not_implemented_pending_endpoint` | `https://data.anbima.com.br/` | structured_json_csv_or_txt_pending | daily | none | Conditional on verified free structured access. No associate-login, manual-export, or page-scraping workflow is implemented. |
| `anbima_credit_curves` | P1 | `not_implemented_pending_endpoint` | `https://data.anbima.com.br/` | structured_json_csv_or_txt_pending | daily_or_periodic | none | Conditional on verified free structured access. No associate-login, manual-export, or page-scraping workflow is implemented. |

## Official source facts

- The ANBIMA public portal groups the relevant surface under
  `Informar -> Dados e estatisticas -> Precos e Indices`, with navigation for
  Curvas, Indices, Precos, and Dados.
- Public ANBIMA pages link to ANBIMA Data - Dados e Ferramentas de
  Investimentos.
- The Projecoes IPCA e IGP-M page describes projections for both indicators as
  consensus from ANBIMA's permanent macroeconomic consultative group.

## Endpoint verification policy

A dataset may be marked `live_download` only when it has a stable direct
structured endpoint/file, no login requirement, no browser automation, no
manual export, a fixture test, and no new dependency. If a source requires
associate access, hidden short-lived endpoints, CAPTCHA, manual export, PDF,
XLSX-only parsing, or brittle page scraping, keep it source-mapped as pending
or raw-only.
