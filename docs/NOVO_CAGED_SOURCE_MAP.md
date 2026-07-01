# Novo CAGED Source Map

Novo CAGED/PDET is used here as official formal-employment context for the
Brazil-RV macro pipeline. This PR implements only raw -> bronze ->
source-specific silver ingestion for movement microdata and the official
release-calendar page. It does not build research panels, model features,
admission/dismissal aggregates, net-job features, wage-growth measures, ratios,
seasonal adjustments, rolling statistics, labels, or portfolio/backtest fields.

Official source basis:

- The PDET home page lists Novo CAGED for monthly employment behavior and says
  RAIS and CAGED microdata are available in TXT format.
- The Novo CAGED methodology page says that since January 2020 part of CAGED was
  replaced by eSocial, while Novo CAGED statistics are generated from eSocial,
  CAGED, and Empregador Web information.
- The same page documents transition-period imputation to preserve the quality
  and integrity of formal-employment statistics.
- The release-calendar page lists expected disclosure dates by reference
  competence.
- The current Novo CAGED monthly page links official PDFs, Google Drive
  `Tabelas.xlsx`, the PowerBI panel, a technical note, and reference stock; those
  stay source-map only here.

| dataset_id | priority | status | source_page_or_endpoint | raw_format | expected_frequency | silver_output | known_limitations |
|---|---:|---|---|---|---|---|---|
| novo_caged_movements_monthly | P0 | live_download | https://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{year}/{yyyymm}/CAGEDMOV{yyyymm}.7z | 7z_txt | monthly from 2020-01 onward | data/silver/novo_caged_movements_monthly/ | Movement rows only; no late-declaration/exclusion revision handling, no aggregation, no labor features. |
| novo_caged_release_calendar | P0 | live_download | https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/o-pdet/calendario-de-divulgacao-do-novo-caged | html | current official calendar page | data/silver/novo_caged_release_calendar/ | Current official page only; historical/revision-aware release calendars are deferred. |
| novo_caged_late_declarations_monthly | P1 | source_map_only_revision_files | no fake endpoint | 7z_txt | monthly | none | Likely CAGEDFOR family; revision-aware ingestion belongs in a dedicated PR. |
| novo_caged_exclusions_monthly | P1 | source_map_only_revision_files | no fake endpoint | 7z_txt | monthly | none | Likely CAGEDEXC family; exclusions are not silently folded into movement rows. |
| novo_caged_official_monthly_tables | P1 | source_map_only_official_xlsx_crosscheck | current Novo CAGED page links Tabelas.xlsx through Google Drive | xlsx_google_drive_link | monthly | none | Useful cross-check but not deterministic enough for this raw->silver PR. |
| novo_caged_reference_stock | P2 | source_map_only_reference | current Novo CAGED page reference-stock links | html_or_xlsx_pending | annual_or_reference | none | Reference stock is useful level context but separate from row-level movement ingestion. |
| rais_microdata | P2 | source_map_only_separate_source | PDET microdata page | txt_archive_large | annual | none | RAIS is separate structural annual labor data, not Novo CAGED ingestion. |
| legacy_caged_microdata | P2 | source_map_only_legacy_source | PDET microdata page | txt_archive_large | monthly_historical | none | Pre-2020 CAGED history is deferred to a separate legacy-source PR. |
| pdet_powerbi_panel | P3 | source_map_only_human_validation | current Novo CAGED page PowerBI link | powerbi | dashboard | none | Human-validation dashboard only; no browser automation or scraping. |
| pdet_online_queries | P3 | source_map_only_human_validation | PDET online query links | web_query | interactive | none | Human-validation query interface only; no deterministic ingestion in this PR. |

Timing and point-in-time handling:

- Movement `ref_date` is the last calendar day of the competence month.
- Movement rows carry reference-only heuristic availability under
  `novo_caged_conservative_next_month_end_plus_2bd_reference_only`. Model-facing
  panels must join the official release calendar for the competence month; if
  no calendar row is available, model availability remains null.
- Release-calendar rows preserve the official listed `release_date`; their
  `available_date` is same-day EOD when the release date is a B3 business day,
  otherwise the next B3 business day.

Operational constraints:

- Generate one movement resource per month. Do not crawl FTP directories.
- Parse official `.7z` archive members as string-first TXT/CSV rows with
  `py7zr`.
- Do not parse XLSX or PDF files, call PowerBI, run browser automation, or commit
  downloaded raw/bronze/silver/manifests.
