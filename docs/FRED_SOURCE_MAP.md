# FRED Source Map

This PR implements one FRED-only live ingestion dataset. FRED's official
`fred/series/observations` endpoint returns observations for a configured
series and supports JSON via `file_type=json`. Every live request requires a
registered API key supplied by the `FRED_API_KEY` environment variable.

FRED redistributes data from multiple underlying sources. St. Louis Fed terms
and underlying series-owner restrictions apply. Copyrighted or limited-history
series must be cited and handled according to their FRED notes. In particular,
`SP500` is useful as a FRED equity proxy but should not be treated as a complete
long-history equity source, and `PCOPPUSDM` is an IMF monthly series with
copyright/citation terms.

| dataset_id | priority | status | source_page_or_api | raw_format | expected_frequency | silver_output | known_limitations |
|---|---:|---|---|---|---|---|---|
| fred_series_observations | P0 | live_download | https://api.stlouisfed.org/fred/series/observations | json | mixed | data/silver/fred_series_observations/ | Requires `FRED_API_KEY`; date-only next-business-day availability; underlying source terms and series-specific history limits apply. |

## Included Series Groups

- Treasury nominal yields: `DGS2`, `DGS5`, `DGS10`, `DGS30`
- Fed policy and money-market rates: `DFF`, `SOFR`, `DFEDTARU`, `DFEDTARL`
- Treasury real yields: `DFII5`, `DFII10`, `DFII30`
- Breakeven inflation: `T5YIE`, `T10YIE`
- Broad dollar and FX proxies: `DTWEXBGS`, `DTWEXAFEGS`, `DTWEXEMEGS`
- China FX proxy: `DEXCHUS`
- Equity and volatility proxies available on FRED: `SP500`, `NASDAQCOM`, `VIXCLS`
- Credit spread and yield proxies: `BAA10Y`, `AAA10Y`, `DBAA`, `DAAA`
- Oil and commodity proxies available on FRED: `DCOILWTICO`, `DCOILBRENTEU`, `PCOPPUSDM`

## Deferred Global Market Gaps

These remain useful but are not implemented here because they need a dedicated
source contract outside FRED:

- ETF prices: `EWZ`, `EEM`, `EMB`, `HYG`, `LQD`
- Long-history S&P/Nasdaq/Russell/EOD equity sources where FRED is insufficient
- Iron ore and non-FRED commodity futures EOD
- China equities and China PMI
- IMF, World Bank, OECD, and BIS slower-moving macro/regime datasets

No fake endpoints are configured for deferred sources.
