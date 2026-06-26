# IBGE source map

This source map covers structured IBGE inputs for the raw -> bronze ->
source-specific silver spine. It intentionally excludes article text parsing,
PDF parsing, NLP, event-risk overlays, modeling, and broad scraping.

| dataset_id | priority | status | source | documented_endpoint_or_page | raw_format | expected_frequency | silver_output | known_limitations |
|---|---:|---|---|---|---|---|---|---|
| `ibge_sidra_series` | P0 | `live_download` | IBGE Aggregates API v3 / SIDRA | `https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos/{periodos}/variaveis/{variavel}` | SIDRA JSON | mixed | `data/silver/ibge_sidra_series/` | Default runs include P0 rows only. SIDRA observations are not model-usable unless matched to release-calendar timing. |
| `ibge_release_calendar` | P0 | `live_download` | IBGE Calendar API v3 | `https://servicodados.ibge.gov.br/api/v3/calendario/` and `/calendario/{pesquisa}` | calendar JSON | daily_or_event | `data/silver/ibge_release_calendar/` | Calendar rows are release metadata; actual outcome values still use their own release timing through SIDRA matching. Uses documented `qtd`, `de`, and `ate` parameters only; if very long ranges exceed response limits, call over smaller date windows rather than using undocumented page parameters. |
| `ibge_products_metadata` | P0 | `live_download` | IBGE Products API v1 | `https://servicodados.ibge.gov.br/api/v1/produtos/estatisticas` | products JSON | periodic | `data/silver/ibge_products_metadata/` | Products API exposes category hierarchy fields; `parent_product_id` remains null unless IBGE provides a true parent product field. |
| `ibge_news_releases_metadata` | P1 | `live_download_metadata_only` | IBGE News API v3 | `https://servicodados.ibge.gov.br/api/v3/noticias/` | news JSON | daily_or_event | `data/silver/ibge_news_releases_metadata/` | Metadata only. Article bodies, introductions, images, and scraped pages are intentionally excluded. |
| `ibge_errata_revisions_metadata` | P1 | `not_implemented_pending_url` | IBGE pending metadata source | pending stable documented endpoint/query | pending | daily_or_event | none | Source-map only until a stable metadata endpoint is confirmed. No text parsing. |

Official IBGE documentation used for this map:

- Aggregates API v3 documents that it powers SIDRA and that each SIDRA table is
  an API aggregate. It exposes `/agregados`, `/agregados/{agregado}/metadados`,
  `/agregados/{agregado}/periodos`, and
  `/agregados/{agregado}/periodos/{periodos}/variaveis/{variavel}` with
  `localidades`, `classificacao`, and `view` query parameters.
- Calendar API v3 documents `/calendario/` and `/calendario/{pesquisa}` with
  `qtd`, `de`, and `ate` query parameters.
- Products API v1 documents `/produtos/estatisticas`, which supports linking
  product identifiers to calendar and news endpoints.
- News API v3 documents `/noticias/` with `tipo`, `qtd`, `page`, `de`, `ate`,
  `busca`, and `idproduto` query parameters.

## Curated SIDRA aggregates

The corresponding `configs/series/ibge_sidra.yaml` rows include
`release_calendar_product_id_status`. Current non-null product ids are marked
`verified`; any future `needs_verification` row must stay non-model-usable until
the calendar product mapping is confirmed.

| dataset_slug | priority | aggregate_id | theme | release_calendar_product_id |
|---|---:|---:|---|---:|
| `ipca` | P0 | 7060 | inflation | 9256 |
| `ipca15` | P0 | 7062 | inflation | 9260 |
| `inpc` | P0 | 7063 | inflation | 9258 |
| `gdp_volume_change` | P0 | 5932 | quarterly national accounts | 9300 |
| `gdp_volume_index` | P0 | 1620 | quarterly national accounts | 9300 |
| `gdp_current_values` | P0 | 1846 | quarterly national accounts | 9300 |
| `pim_industrial_production` | P0 | 8888 | industrial production | 9294 |
| `pmc_retail_index` | P0 | 8880 | retail sales | 9227 |
| `pmc_broad_retail_index` | P0 | 8881 | retail sales | 9227 |
| `pmc_retail_by_activity` | P0 | 8882 | retail sales | 9227 |
| `pmc_broad_retail_by_activity` | P0 | 8883 | retail sales | 9227 |
| `pms_services_index` | P0 | 5906 | services | 9229 |
| `pms_services_segments` | P0 | 8163 | services | 9229 |
| `pms_services_activities` | P0 | 8688 | services | 9229 |
| `pnad_unemployment_rate` | P0 | 6381 | labor | 9171 |
| `pnad_participation_rate` | P0 | 5944 | labor | 9171 |
| `pnad_occupation_level` | P0 | 6379 | labor | 9171 |
| `pnad_underutilization_rate` | P0 | 6438 | labor | 9171 |
| `pnad_underutilization_level` | P0 | 6441 | labor | 9171 |
| `pnad_real_income` | P0 | 6390 | labor | 9171 |
| `pnad_real_income_usual` | P0 | 6387 | labor | 9171 |
| `pnad_real_income_mass` | P0 | 6392 | labor | 9171 |
| `pnad_real_income_mass_usual` | P0 | 6393 | labor | 9171 |
| `sinapi_cost_m2` | P0 | 2296 | construction costs | 9270 |
| `pms_services_regional` | P1 | 8693 | services optional | 9229 |
| `pms_services_activity_regional` | P1 | 8694 | services optional | 9229 |
| `pms_services_special_regional` | P1 | 8695 | services optional | 9229 |
| `sinapi_cost_m2_no_payroll_exemption` | P1 | 6586 | construction costs optional | 9270 |
| `sinapi_project_cost` | P1 | 647 | construction costs optional | 9270 |
| `ipp_producer_prices` | P1 | 6903 | producer prices | 9282 |
| `ipp_producer_prices_index` | P1 | 6904 | producer prices | 9282 |
| `ipp_producer_prices_by_activity` | P1 | 6723 | producer prices | 9282 |

SIDRA release-calendar matching is required before observations become
model-usable. If no calendar row matches a configured product and reference
period, the row remains in silver with `available_date = null`,
`model_usable = false`, and `availability_policy = unmatched_release_calendar`.
