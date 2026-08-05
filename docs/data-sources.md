# Data sourcing plan

*Research for the source-expansion phase (ROADMAP §5). Maps the platform's data
needs — both the owner's two-model strategy and broader coverage for other users —
to concrete sources, the registry group each writes to, and the licensing status
that decides whether it can go in the public lake or ships as code-only.*

> **Status: research / proposal.** Nothing here is built yet. This is the shortlist
> we align on *before* adding registry rows and ingestors. Licensing notes are a
> good-faith reading of each source's terms and must be re-verified per source
> before anything is published publicly (ROADMAP §6).

---

## 1. How to read this

The platform is a **data platform, not a modelling platform** (see
`docs/ROADMAP.md` §1). It houses clean, unified data; the predictive models are a
**separate downstream project** that consumes the lake. So the job here is
*coverage and correctness of the raw inputs*, not signals.

Every candidate source is judged on five axes:

| Axis | Why it matters |
|---|---|
| **Group** | `bars` / `series` / `events` / `microstructure` — determines schema, partitioning, cost (ROADMAP §3.3). |
| **Redistributable?** | Can the *data* live in the public lake, or only the *ingestor* (code-only)? Government + exchange-native = usually yes; vendors/scrapes = no. |
| **Cost** | Free vs paid. The platform is cost-disciplined; default to free/open. |
| **Cadence + history** | How fresh, how far back — decides watermarking and backfill. |
| **Fit** | Does it serve the owner's strategy, general coverage, or both? |

**Licensing rule of thumb (the gate for the public lake):**
- **Redistributable** — US-government data (BLS, BEA, Census, Treasury, Federal
  Reserve Board, CFTC) is public domain; exchange-native public market data
  (Binance, Kraken, Coinbase, Bybit, OKX) generally is too.
- **Code-only** — vendor APIs and scrapes (yfinance/Yahoo, Stooq, Tiingo, Alpha
  Vantage, Trading Economics, FMP, ICE) forbid redistribution. We open-source the
  ingestor; the user brings their own key and pulls into the same group schema.
- **Per-series, not just per-source** — FRED is the sharp edge: FRED *delivers*
  both public-domain government series **and** third-party-owned series (ICE BofA
  credit indices, S&P/Case-Shiller, etc.). The registry must carry
  `redistributable` at the granularity that lets a government series publish while
  a third-party series on the same API stays code-only.

---

## 2. The strategy's data pipeline

The owner's research uses two chained models (built downstream, outside this repo).
The platform's task is to feed both.

### Model 1 — macro + volatility bias
Reads macro and volatility state, forms a directional/regime bias, and emits
predictive numbers. **Raw inputs it needs the platform to serve:**

- **Macro series** — growth, inflation, employment, rates, money/liquidity, credit,
  housing, consumer. → group `series` (+ `events` for the release-time/bitemporal view).
- **The volatility complex** — VIX, VVIX, SKEW, term structure; ideally cross-asset
  (bond vol, credit vol). → group `series`.
- **Rates & the curve** — Treasury par yields, 2s10s, real yields, breakevens. → `series`.
- **Positioning** — CFTC COT (who is long/short the futures). → `series`.
- **Cross-asset context** — the dollar (DXY / trade-weighted), credit spreads. → `series`.

### Model 2 — order-flow entries
Reads Model 1's bias and hunts entries with ATR, order-flow, and confluences.
**Raw inputs it needs:**

- **Bars** for ATR / VWAP / volume profile — already have crypto daily; needs finer
  intervals and more symbols. → group `bars`.
- **Microstructure** — tick trades + L2 order book (the owner's wedge). Have Binance;
  extend to more venues. → group `microstructure`.
- **Crypto derivatives confluences** — funding rate, open interest, liquidations
  (positioning, conviction, forced flow). → `series` (per-symbol perp metrics).
- **Cross-asset risk context** — VIX, yields, DXY (feeds risk-on/off into entries) —
  same series as Model 1, reused.

**Note on "derived" inputs.** ATR, VWAP, CVD, volume profile, net liquidity, macro
*surprise* are **computed features (gold layer), not sources.** The platform's job
is to house the clean raw inputs they are computed from. They are called out so the
raw dependencies are explicit, not to be ingested directly.

---

## 3. Source catalog by group

### 3.1 `series` — macro, volatility, rates, positioning *(the Model-1 engine)*

| Source | What | Redist? | Cost | Notes |
|---|---|---|---|---|
| **FRED** (St. Louis Fed API) | The macro spine: growth, CPI/PCE, employment, rates, money supply, housing, sentiment — hundreds of thousands of series | **Per-series** (gov = yes; 3rd-party = no) | Free (API key) | Single free tier. Fair-use polling limits. The canonical macro source. |
| **ALFRED** (Archival FRED) | *Vintages* of FRED series — the value as it was published on a given date (`realtime_start/end`) | Same as FRED | Free | **Bitemporal gold.** Kills lookahead bias in backtests; the correct way to store revisable macro (ROADMAP §3.4). |
| **U.S. Treasury** (fiscaldata / par yield curve) | Daily Treasury par yield curve, real yields | **Yes** (public domain) | Free | Authoritative curve source; FRED mirrors much of it (DGS2, DGS10, T10Y2Y). |
| **CFTC COT** (Socrata public API) | Commitments of Traders — long/short by trader category; TFF report covers financial futures (ES, Treasuries, FX) | **Yes** (public domain) | Free | Weekly (Fri, for Tue). History to 1986 (legacy) / 2006 (TFF). `cot_reports` lib exists. Positioning input. |
| **CBOE indices** (CDN CSV) | VIX, VVIX, SKEW, VIX9D, put/call ratios — EOD index levels | **Likely** (EOD levels; verify bulk-republish terms) | Free | Direct CSV e.g. `cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv`. Term structure (VX futures) is harder. |
| **Binance / Bybit derivatives** (public REST) | Perp **funding rate, open interest, liquidations** per symbol | **Yes** (exchange-native) | Free | Order-flow confluences for Model 2. Native endpoints; no aggregator needed. |
| ICE BofA credit OAS (via FRED) | High-yield / IG credit spreads (risk appetite) | **No** (ICE-owned 3rd-party series) | Free to pull | Code-only. Great macro input but ICE owns it → don't republish. |
| **MOVE index** (ICE) | Treasury-market implied vol | **No** (ICE proprietary) | Paid / scrape | Bond-vol gauge. No free API; code-only via Yahoo (`^MOVE`) or ICE paid. Gap — see §6. |

### 3.2 `events` — the economic calendar (bitemporal) *(licensing is the blocker)*

| Source | What | Redist? | Cost | Notes |
|---|---|---|---|---|
| **FRED releases + ALFRED** | Release *schedule* + the actual value and its vintages for U.S. macro | **Yes** (gov) | Free | **The redistributable core of a calendar.** Gives scheduled_ts + observed value + revision history for free — the bitemporal model ROADMAP §3.4 wants. |
| Trading Economics / FMP / FXStreet / Finnhub | Full global calendar with **consensus forecast** + actual + previous | **No** (vendor license) | Free-tier / paid | Code-only. The *forecast/consensus* column (needed for "surprise") is the proprietary part FRED can't give. |

> **Design call:** build the `events` calendar from FRED/ALFRED for U.S. macro (free,
> redistributable, bitemporally correct). Treat the consensus-forecast column as a
> code-only enrichment layered on top for the user's own use. This is exactly the
> "two halves" product shape (ROADMAP §6).

### 3.3 `bars` — OHLCV coverage *(have crypto; expand)*

| Source | What | Redist? | Cost | Notes |
|---|---|---|---|---|
| **Binance / Kraken** (have) | Crypto spot OHLCV | **Yes** (exchange-native) | Free | Already in the registry. |
| **ccxt** | Unified OHLCV across 100+ exchanges (spot + perp) | **Per-exchange** (exchange-native = yes) | Free | The clean way to widen crypto venue coverage as one ingestor. Replaces bespoke per-exchange loaders. |
| **yfinance** (have) | Equities/ETF/index/FX daily | **No** (Yahoo ToS) | Free | Already flagged code-only. |
| Stooq | Global stocks/indices/FX daily, no key | **No** (personal use) | Free | Code-only alt to yfinance; good for reproducible equity/FX pulls. |
| Tiingo / Alpha Vantage | Cleaner EOD equities + fundamentals | **No** (vendor free tier) | Free tier | Code-only. Better data quality than yfinance if the user brings a key. |

### 3.4 `microstructure` — crypto order flow *(the wedge; the only expensive group)*

| Source | What | Redist? | Cost | Notes |
|---|---|---|---|---|
| **Binance WebSocket** (have) | trades, depth diffs, book_ticker + REST snapshots | **Yes** | Free (self-hosted) | Deployed 24/7. The template for other venues. |
| **Coinbase / Bybit / OKX / Kraken WS** | Same kinds, native per-venue feeds | **Yes** (exchange-native) | Free (self-hosted) | Each is a new collector on the existing pattern. Cross-venue order flow = the differentiator. |
| **ccxt.pro** | Unified WS for trades/order book across venues | **Per-exchange** | Paid (ccxt.pro) | Convenience layer; the bespoke collectors are already free and battle-tested. |
| Tardis.dev | Historical tick L2/L3, funding, liquidations | **No** (vendor) | Paid | Code-only. Backfill option for history the live collector missed. |

---

## 4. Licensing map (the public-lake gate)

**Redistributable → the open lake:** FRED (gov series only), Treasury, CFTC COT,
CBOE EOD index levels, exchange-native crypto (Binance/Kraken/Coinbase/Bybit/OKX
spot + perp metrics + microstructure).

**Code-only → publish the ingestor, not the data:** yfinance/Yahoo, Stooq, Tiingo,
Alpha Vantage, Trading Economics/FMP/FXStreet calendars, ICE (MOVE, BofA credit),
Tardis, ccxt.pro. Also FRED's third-party-owned series (ICE, S&P/Case-Shiller).

Every `SourceSpec` already carries `redistributable` + `license_note`; per-series
sources (FRED) need that flag pushed down to the series/symbol level.

## 5. Recommended sequencing

Ordered by value to the owner's strategy first, then coverage, then gaps.

**Wave 1 — feed the two models, free + redistributable.**
1. **FRED + ALFRED** ingestor (`series`) — the macro spine and the bitemporal core.
   Highest single payoff: unlocks Model 1's macro inputs *and* the `events` calendar.
2. **CBOE volatility indices** (`series`) — VIX/VVIX/SKEW EOD.
3. **CFTC COT** (`series`) — positioning.
4. **Crypto derivatives** (funding / OI / liquidations, `series`) — Model 2 confluences.
5. **Extend microstructure** to 1–2 more venues (Coinbase or Bybit) — cross-venue order flow.

**Wave 2 — coverage for other users.**
6. **ccxt** for wide crypto `bars` (one ingestor, many exchanges).
7. **Stooq / Tiingo** equities `bars` (code-only) — broaden asset coverage.
8. **Treasury curve** detail + FRED credit/liquidity series.

**Wave 3 — paid / code-only / user-requested.**
9. Consensus-forecast calendar enrichment (code-only).
10. Tardis historical microstructure backfill (paid, on demand).
11. A lightweight "request a source" intake so users can nominate additions.

## 6. Known gaps / open questions

- **Consensus forecasts** (macro *surprise* = actual − forecast) are proprietary.
  FRED gives actual + vintages, not the market's prior expectation. Model 1's
  surprise signal needs a code-only forecast source (or a homegrown nowcast). **[open]**
- **MOVE / bond-market implied vol** has no free API (ICE proprietary). Proxy with
  the VIX complex + credit spreads, or accept code-only via Yahoo. **[open]**
- **Equities microstructure** (L2/tick) is not freely available like crypto — it is
  expensive and licensed. Out of scope unless a concrete need appears. **[open]**
- **Symbol normalization across venues** (ccxt vs native spellings) will need the
  registry's symbol map to do real work — a known source of quiet bugs (ROADMAP §11).

---

## 7. How this lands in the platform

Each source above becomes **one `SourceSpec` row + a small ingestor** — the Phase-4
machinery already in place. `series`, `events`, and `bars` are cheap ("house
everything"); `microstructure` stays strictly scoped. The group schemas (Phase 3)
should be pinned down for `series` and `events` *before* Wave 1, since FRED/ALFRED
and the calendar are the first non-`bars` shapes the lake will hold.
