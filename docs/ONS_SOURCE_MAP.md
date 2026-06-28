# ONS Source Map

Brazil-RV uses ONS Dados Abertos for free, structured Brazilian power-system
data. ONS publishes historical annual CSV/PARQUET resources through
`ons-aws-prod-opendata`, with CC-BY attribution and recurring consistency
processes that can update data after publication.

This PR ingests only annual CSV resources into raw, bronze, and source-specific
silver tables. It does not add daily aggregation, ratios, stress labels,
power-price forecasting, raw-to-research panels, modeling, or backtesting.

| dataset_id | priority | status | source_page_or_endpoint | raw_format | expected_frequency | silver_output | known_limitations |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ons_ear_subsystem_daily` | P0 | live_download | https://dados.ons.org.br/dataset/ear-diario-por-subsistema | csv_annual | daily | `data/silver/ons_ear_subsystem_daily/` | Subsystem stored energy only; reservoir/REE/basin detail deferred. |
| `ons_ena_subsystem_daily` | P0 | live_download | https://dados.ons.org.br/dataset/ena-diario-por-subsistema | csv_annual | daily | `data/silver/ons_ena_subsystem_daily/` | Subsystem ENA only; detail datasets deferred. |
| `ons_load_daily` | P0 | live_download | https://dados.ons.org.br/dataset/carga-energia | csv_annual | daily | `data/silver/ons_load_daily/` | Methodology breakpoints are noted but values are not adjusted. |
| `ons_cmo_weekly` | P0 | live_download | https://dados.ons.org.br/dataset/cmo-semanal | csv_annual | weekly | `data/silver/ons_cmo_weekly/` | Weekly rows are preserved; no daily fill in ingestion. |
| `ons_energy_balance_subsystem` | P0 | live_download | https://dados.ons.org.br/dataset/balanco-energia-subsistema | csv_annual | hourly | `data/silver/ons_energy_balance_subsystem/` | Hourly source mix is preserved; no daily aggregation or shares. |
| `ons_interchange_subsystem_hourly` | P1 | live_download | https://dados.ons.org.br/dataset/intercambio-nacional | csv_annual | hourly | `data/silver/ons_interchange_subsystem_hourly/` | Directional flows only; no net interchange. |
| `ons_ear_reservoir_daily` | P1 | source_map_only_high_cardinality | https://dados.ons.org.br/dataset/ear-diario-por-reservatorio | csv_annual | daily | none | Reservoir-level detail deferred. |
| `ons_ear_ree_daily` | P1 | source_map_only_high_cardinality | https://dados.ons.org.br/dataset/ear-diario-por-ree-reservatorio-equivalente-de-energia | csv_annual | daily | none | REE detail deferred. |
| `ons_ear_basin_daily` | P1 | source_map_only_high_cardinality | https://dados.ons.org.br/dataset/ear-diario-por-bacia | csv_annual | daily | none | Basin detail deferred. |
| `ons_ena_reservoir_daily` | P1 | source_map_only_high_cardinality | https://dados.ons.org.br/dataset/ena-diario-por-reservatorio | csv_annual | daily | none | Reservoir-level ENA deferred. |
| `ons_ena_ree_daily` | P1 | source_map_only_high_cardinality | https://dados.ons.org.br/dataset/ena-diario-por-ree-reservatorio-equivalente-de-energia | csv_annual | daily | none | REE-level ENA deferred. |
| `ons_ena_basin_daily` | P1 | source_map_only_high_cardinality | https://dados.ons.org.br/dataset/ena-diario-por-bacia | csv_annual | daily | none | Basin-level ENA deferred. |
| `ons_reservoir_hydrology_daily` | P1 | source_map_only_high_cardinality | https://dados.ons.org.br/dataset/dados-hidrologicos-reservatorios-base-diaria | csv_annual | daily | none | Detailed reservoir hydrology deferred. |
| `ons_reservoir_hydrology_hourly` | P2 | source_map_only_large_high_cardinality | https://dados.ons.org.br/dataset/dados-hidrologicos-reservatorios-base-horaria | csv_annual_large | hourly | none | Hourly reservoir detail is larger and high-cardinality. |
| `ons_generation_by_plant_hourly` | P2 | source_map_only_large_high_cardinality | https://dados.ons.org.br/dataset/geracao-usina-2 | csv_annual_large | hourly | none | Plant-level generation is large and high-cardinality. |
| `ons_constrained_off_wind` | P2 | source_map_only_specialized | https://dados.ons.org.br/dataset/restricao-operacao-constrained-off-usinas-eolicas | csv_annual | hourly | none | Specialized curtailment data deferred. |
| `ons_constrained_off_solar` | P2 | source_map_only_specialized | https://dados.ons.org.br/dataset/restricao-operacao-constrained-off-usinas-fotovoltaicas | csv_annual | hourly | none | Specialized curtailment data deferred. |
| `ons_thermal_dispatch_reason` | P2 | source_map_only_specialized | https://dados.ons.org.br/dataset/geracao-termica-motivo-despacho | csv_annual | event | none | Specialized dispatch-reason data deferred. |
| `ons_installed_generation_capacity` | P2 | source_map_only_reference | https://dados.ons.org.br/dataset/capacidade-instalada-geracao | csv_annual | monthly_or_snapshot | none | Slower reference data deferred. |
| `ons_generation_capacity_factor_wind_solar` | P2 | source_map_only_specialized | https://dados.ons.org.br/dataset/fator-capacidade-geracao-eolica-solar | csv_annual | hourly_or_daily | none | Renewable performance detail deferred. |
| `ons_reliability_indicators` | P3 | source_map_only_low_priority | https://dados.ons.org.br/ | mixed_pending | monthly_or_event | none | Slower reliability indicators deferred. |
| `ons_transmission_assets` | P3 | source_map_only_low_priority | https://dados.ons.org.br/ | mixed_pending | snapshot | none | Transmission asset reference data deferred. |

## Timing

ONS pages state that data may be updated after publication by recurring
consistency processes. Silver therefore uses the conservative policy
`ons_conservative_next_business_day`: date-only or hourly observations for
`ref_date = D` are usable no earlier than the next business day.

## Explicit Exclusions

This ingestion PR does not add daily aggregation, source shares, ratios,
scarcity labels, drought indicators, power-price forecasts, rolling features,
z-scores, stationarity transforms, portfolio fields, or backtest logic.
