# Licensing & redistribution

Most financial data **cannot legally be redistributed**. This is the hard constraint
that shapes the whole product (ROADMAP §6): building ingestion for data that can never
be published would be wasted effort at platform scope, so every source is classified
*before* it goes in the public lake.

## The two halves

The platform is split cleanly, and the split is enforced in code, not by convention:

1. **The open lake** — data the platform is permitted to republish (exchange-native
   public market data, U.S.-government macro). Free, hosted on a public R2 bucket,
   queryable directly with your own DuckDB, no credentials.
2. **The open-source ingestors** — for licensed sources, the *ingestor* is published,
   not the data. A user supplies their own API key and pulls into the **same group
   schema** as everything else. The schema unification — the part nobody wants to do —
   is given away in *both* halves.

## How it is enforced

Every `SourceSpec` in the registry carries `redistributable: bool` and a
`license_note`. That single flag drives the public-publish job (`qde.publish_public`):

The gate is an **allowlist**, not a list of exclusions, and the direction is the whole
point: a source is published only if the registry names it *and* marks it
redistributable.

- **Bronze** files are uploaded only when their `source=` partition is on the allowlist.
- **Gold** marts blend sources (e.g. `fct_bars_daily` includes yfinance ETFs), so their
  rows are **filtered** `WHERE source IN (allowed)` before the public copy is written —
  the private gold stays whole; only the public copy is trimmed.

Filtering by "everything except the sources known to be forbidden" would publish anything
the registry has not heard of: a spec retired while its files remained, a directory
dropped in by hand, a source seeded before it was declared. Each is an ordinary mistake,
and each would have been served publicly with no error raised. The asymmetry decides it —
withholding something that should have been public is fixed by the next nightly;
publishing something licensed cannot be fixed at all.

Identity is `(group, source)` rather than the name alone, because a venue can appear in
more than one group: Binance is both a `bars` source and a `microstructure` one. A
permission granted in one group must not authorise publication in another.

So a source marked `redistributable=False`, or absent from the registry entirely,
**cannot** reach the public bucket, by construction. The `catalogue.json` records the
exclusion and the reason.

## Per-source classification

Generated from the registry (`dim_sources`); re-verify each source's current terms
before relying on it for public publishing.

| Source | Group | Redistributable | Basis |
|---|---|---|---|
| binance | bars | **Yes** | Exchange-native public REST market data (historical klines). |
| bybit | bars | **Yes** | Exchange-native public spot OHLCV (via ccxt). |
| coinbase | bars | **Yes** | Exchange-native public spot OHLCV (via ccxt). |
| kraken | bars | **Yes** | Exchange-native public REST OHLC. |
| kucoin | bars | **Yes** | Exchange-native public spot OHLCV (via ccxt). |
| okx | bars | **Yes** | Exchange-native public spot OHLCV (via ccxt). |
| **yfinance** | bars | **No — code-only** | Scrapes Yahoo Finance; Yahoo's terms prohibit redistribution. The ingestor is open-sourced; the data is not published. |
| fred | series | **Yes** | FRED delivery of U.S.-government statistical series (BLS / BEA / Census / Federal Reserve Board / Treasury) — public domain. *NB: FRED also hosts third-party series (ICE, S&P/Case-Shiller) that are **not** redistributable; only government series are curated here.* |
| cboe | series | **Yes** | CBOE end-of-day volatility index **levels** (VIX / VVIX / SKEW), free CDN CSVs. The real-time feed and underlying options data are **not** redistributable. |
| cftc | series | **Yes** | CFTC Commitments of Traders (TFF, futures-only) — U.S.-government public-domain. |
| binancefut | series | **Yes** | Binance USD-M perpetual funding + settlement mark price (public fapi REST); exchange-native. |
| fredcal | events | **Yes** | U.S. macro release calendar from FRED releases + ALFRED vintages — public domain. The consensus **forecast** column is proprietary and is *not* included (a code-only enrichment). |
| binance | microstructure | **Yes** | Binance public websocket feed (trades, diff-depth, book ticker) — exchange-native, same terms as the REST klines above. |
| coinbase | microstructure | **Yes** | Coinbase Exchange public websocket feed (matches, level2_batch, ticker, heartbeat) — public and no-auth. Canonical `BTCUSDT` maps to Coinbase's **USD** pair; the USD/USDT difference against Binance is the signal, not an error. |

### A note on microstructure

The streamed archive was withheld from the public lake until **2026-08-16**, and it is
worth being precise about why, because the reason was never legal. Both venues are
`redistributable=True` — it was excluded by a hardcoded group list in the publisher,
entirely independent of the flag above. It was a *choice*: the tick and order-book
capture is the owner's own order-flow research (ROADMAP §3.3), and the relationship
between venues — rather than either venue alone — is the platform's wedge.

That choice was reversed on the reasoning that the platform serves data while the
strategy consuming it lives outside this repo. The archive is now published **in full**.

Two consequences worth recording:

- It reaches the public bucket by a **different route** from everything else. The nightly
  sync prunes microstructure from local disk after uploading it, so at publish time the
  only copy on the machine is a half-written current day. It is mirrored bucket-to-bucket
  instead (`publish_public.mirror_private_prefix`), server-side.
- The derived `fct_cross_venue_basis` mart stays **private**. Not for secrecy — it is a
  rolling window rebuilt nightly from whatever bronze is still on local disk, so
  publishing it would serve a dataset that silently shrinks. If it is ever made public it
  should first be rebuilt from R2 as a genuinely accumulating mart.

## The general rule of thumb

- **Redistributable** → U.S.-government data (BLS, BEA, Census, Treasury, Federal
  Reserve Board, CFTC) is public domain; exchange-native public market data
  (Binance, Kraken, Coinbase, Bybit, OKX, KuCoin) generally is too.
- **Code-only** → vendor APIs and scrapes (yfinance/Yahoo, Stooq, Tiingo, Alpha
  Vantage, Trading Economics/FMP calendars, ICE) forbid redistribution. The ingestor
  is open; the data is not published.
- **Per-series, not just per-source** → FRED is the sharp edge: it delivers both
  public-domain government series *and* third-party-owned series on the same API. Only
  government series are curated into the public lake.

> This classification is a good-faith reading of each source's terms, not legal advice.
> Re-verify before relying on it.
