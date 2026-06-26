# Brazil RV systematic macro project plan

## 1. Research objective

Build a **free-data, daily-frequency, multi-asset futures model** for Brazilian markets.

The first production-style research problem is:

> Use information known by the end of date `t` to forecast risk-adjusted returns over `t+1`, `t+5`, and `t+20` for Brazilian rates, FX, and equity-index futures.

Initial tradable sleeves:

| Sleeve | Instruments |
|---|---|
| Rates | DI futures and related B3 rates products where data and liquidity support research |
| FX | USD/BRL futures |
| Equity beta | Ibovespa futures |

The first phase is daily only. Intraday data, order-book data, paid news, and paid consensus calendars are intentionally out of scope until a free daily research spine exists.

## 2. Strategic premise

The project should not assume Brazilian futures are easy because they are illiquid. They are not. The reason to begin here is that Brazilian macro futures provide a strong combination of:

- Direct macro exposure.
- Deep enough liquidity to monetize signals.
- Relatively clean tradable instruments compared with equities.
- Many free official datasets.
- Repeated macro/policy/event structures.
- Cross-asset relationships among rates, FX, equity beta, commodities, global rates, and fiscal risk.

The alpha hypothesis is not simple market inefficiency. It is:

> The mapping from Brazilian policy, inflation, fiscal risk, global rates, commodities, external balance, domestic activity, local flows, and political regimes into DI/BRL/Ibovespa futures returns is nonlinear, regime-dependent, and partly learnable from free daily data.

## 3. Data philosophy

Because the model is daily and free-data constrained, we should **ingest broadly and reject narrowly**.

A free dataset should be included if:

1. It is freely attainable through API, download, or permissible scraping.
2. It can be converted into numeric daily features without a dedicated NLP/modeling project.
3. It is likely to be useful for DI, USD/BRL, or Ibovespa futures.
4. It can be handled with point-in-time discipline.

Data cleaning is not a reason to exclude a dataset. Exclude only data that is paid, legally inaccessible, not plausibly relevant, or requires a separate major research project just to make basic features.

## 4. Free dataset universe

### 4.1 B3 market data

Include:

- Futures settlement prices: DI1, DOL/WDO, IND/WIN, DAP, DDI, FRC, and other liquid futures where available.
- Futures contract metadata: maturity codes, expiry dates, tick values, multipliers, quote conventions, trading calendars, settlement conventions.
- Volume and open interest by contract/maturity.
- B3 reference rates and curves.
- Daily bulletin chapters and public statistical reports.
- COTAHIST equities, ETFs, FIIs, BDRs, options, and related listed instruments.
- B3 index levels and historical statistics.
- B3 index composition, current portfolios, and theoretical portfolios.
- B3 investor participation and foreign-investor movement where freely available.
- Traded securities, ISIN/security master, trading parameters, and fee schedules.

### 4.2 BCB data

Include:

- SGS series: Selic, overnight rates, inflation, credit, fiscal, external sector, reserves, activity, monetary aggregates, debt, and related macro/financial series.
- PTAX and official FX reference rates.
- Focus expectations: IPCA, Selic, FX, GDP, current account, trade balance, fiscal variables, and Top 5 where available.
- Copom calendar, decisions, statements, minutes, monetary policy reports, inflation reports, speeches, and press releases.

Text data should begin with simple dictionary/count/similarity features. Do not train a dedicated NLP model initially.

### 4.3 IBGE macro actuals

Include:

- IPCA headline, components, and weights.
- IPCA-15 headline and components.
- INPC.
- GDP and national accounts.
- Industrial production.
- Retail sales.
- Services survey.
- PNAD labor-market data.
- Construction cost indices.
- Release calendar, revisions, and errata metadata.

### 4.4 ANBIMA and fixed income

Include when free access works:

- Sovereign secondary-market rates and prices.
- Fixed-rate sovereign yield curves.
- IPCA-linked real curves.
- Breakeven inflation curves.
- VNA data.
- IMA, IMA-B, IRF-M, and IDkA index data.
- Debenture/credit curves and secondary-market credit data where freely available.
- Inflation projections where available.

### 4.5 Tesouro, fiscal, budget, and public-sector data

Include:

- Resultado do Tesouro Nacional.
- Federal public debt stock, composition, emissions, redemptions, and reports.
- Federal bond auction calendar and auction results.
- Tesouro Direto historical prices/rates, operations, stock, and investors.
- SICONFI state/municipal fiscal data.
- CAPAG state/municipal credit ratings.
- Receita Federal tax collection and fiscal-benefit datasets.
- Portal da Transparência expenses, transfers, amendments, and other fiscal data.
- CAGED/RAIS labor data.

### 4.6 Legislative and election data

Include structured data first:

- Câmara propositions, bills, PECs, MPs, votes, events, committee agendas, and party guidance.
- Senado propositions, votes, events, and agendas.
- TSE candidates, parties, results, electorate, campaign finance, and registered polls.
- Derived poll aggregation in election periods.

Do not start with full speech-transcript NLP.

### 4.7 CVM and local flows

Include:

- CVM fund daily reports: AUM, NAV, quota value, subscriptions, redemptions, and quotaholders.
- Fund registration and classification data.
- Fund portfolio composition/CDA, with appropriate reporting-lag handling.
- Company structured filings: ITR, DFP, FRE, IPE/relevant-fact metadata.
- FII, FIAGRO, FIDC, securitization, and public-offerings datasets.

Use metadata and structured fields before free-text filings.

### 4.8 Trade, commodities, weather, power, and fuel

Include:

- Comex Stat exports/imports by product, country, state, and port/logistics dimensions.
- CEPEA agricultural prices: soy, corn, coffee, sugar, ethanol, cattle, wheat, milk, and other relevant indicators.
- Conab crop estimates, crop history, supply/demand, and agricultural prices.
- INMET daily weather data aggregated to economic regions.
- ONS reservoirs, load, generation, marginal cost, and interchange.
- ANEEL tariff flags and regulatory datasets.
- ANP fuel prices and oil/gas production/royalty datasets.

### 4.9 Global market and macro data

Include:

- U.S. Treasury yields, Fed funds, SOFR, real yields, and breakevens.
- DXY/broad dollar indices.
- S&P 500, Nasdaq, Russell, VIX.
- EWZ, EEM, HYG, LQD, and related global risk proxies.
- Oil, iron ore proxies, copper, soy, corn, coffee, sugar, gold.
- CNH/CNY, China equities, and China PMI/public macro proxies.
- IMF, World Bank, OECD, and BIS slower-moving macro/regime datasets.

### 4.10 News and attention without heavy NLP

Include:

- GDELT Events and GKG daily aggregates.
- Official press-release metadata and simple text features from BCB, Tesouro, Fazenda, IBGE, Petrobras, ANP, TSE, Câmara, and Senado.
- Google Trends only if free/reliable access is available.
- Wikipedia pageviews for selected policy/company/election pages.
- Public RSS/headline metadata only where terms allow it.

Do not ingest raw copyrighted article text into the model.

## 5. Derived feature groups

### Market features

- Continuous futures returns.
- Fixed-maturity DI curve.
- DI level/slope/curvature and butterflies.
- DI carry and roll-down.
- Implied Selic path.
- FX futures vs PTAX basis.
- Ibovespa futures basis.
- Realized volatility.
- Cross-asset correlations.
- Liquidity regime from volume/open interest.
- Roll and crowding flags.

### Macro features

- Inflation state and nowcast-lite features.
- Inflation surprise proxies versus Focus/ANBIMA where available.
- Focus revision momentum.
- Real-rate pressure.
- Growth regime.
- External-balance regime.
- Credit and financial-conditions stress.

### Policy/fiscal/election features

- Copom event windows.
- Policy surprise versus Focus or implied DI path.
- BCB tone and statement-change scores.
- Fiscal stress index.
- Legislative fiscal-event index.
- Election-risk regime and poll momentum.
- Petrobras/fuel-policy risk.

### Global/Brazil relative-value features

- Global risk index.
- EM risk index.
- Commodity terms-of-trade proxy.
- China shock index.
- Brazil idiosyncratic residuals after global factor controls.

## 6. Storage layers

Use a four-layer data design:

```text
data/raw/       # immutable downloaded files
data/bronze/    # parsed source-specific tables
data/silver/    # canonical point-in-time tables
data/gold/      # feature matrices, labels, model-ready panels
```

Raw files are never overwritten. Every download should have a manifest record containing source URL, timestamp, hash, byte size, status, and license/terms note.

## 7. Canonical tables

Minimum silver-layer tables:

- `market_daily`
- `curve_daily`
- `macro_observation`
- `expectation_observation`
- `event_calendar`
- `flow_observation`
- `text_document`
- `text_daily_score`
- `reference_security`
- `reference_contract`
- `reference_calendar`

Minimum gold-layer tables:

- `feature_matrix_daily`
- `label_matrix_daily`
- `asset_universe_daily`
- `cost_estimates_daily`
- `risk_model_daily`

## 8. Point-in-time policy

Every row used by a model needs:

- `ref_date` or `ref_period_start`/`ref_period_end`.
- `release_date` where applicable.
- `available_date`.
- `download_timestamp_utc`.
- `vintage_id` where applicable.

No model or backtest can use rows with:

```text
available_date > asof_date
```

`available_date` is the model-usable daily decision date, not simply the raw
source release date. Under the default EOD daily policy, exact timestamps at or
before the Sao Paulo cutoff are usable that date, exact timestamps after the
cutoff are usable the next business day, and date-only releases default to next
business day. For first-pass daily B3 market data, use the conservative
next-business-day convention until exact publication timestamps are modeled.

## 9. Modeling structure

Start with multiple models, not one deep net:

- Carry/trend/risk baseline.
- Linear/ElasticNet baseline.
- Gradient-boosted tree model.
- Random forest or other nonlinear tabular baseline.
- Later: temporal CNN/TCN or tabular-sequence model.
- Ensemble and calibration layer.

Models forecast expected/risk-adjusted returns. They do not directly size positions.

## 10. Portfolio and backtest structure

Portfolio layer converts forecasts into positions subject to:

- Volatility target.
- DV01 limits by DI maturity bucket.
- FX notional limits.
- Equity beta limits.
- Liquidity and cost estimates.
- Drawdown and event-risk controls.

Backtests must support:

- Walk-forward training.
- Expanding and rolling windows.
- Purged/embargoed validation where needed.
- Transaction costs.
- Volatility targeting.
- Feature-family and dataset-family attribution.
- Regime analysis.

## 11. Current immediate priority

Build the B3 data-ingestion spine first. See `docs/B3_INGESTION_SETUP.md`.
