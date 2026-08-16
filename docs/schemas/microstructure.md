# `microstructure` schema

Tick and L2 order-book data captured live from exchange websockets — the owner's
order-flow wedge, and the **only expensive group** (orders of magnitude larger than
the rest combined). **Implemented** (`qde.stream`); documented here for completeness.

Bronze keeps exactly what the exchange sent: prices and quantities stay as the exact
**strings** transmitted, and exchange timestamps stay as raw epoch milliseconds.
Numeric casting, datetime conversion, and order-book reconstruction belong to the
silver layer, where a bug can be fixed and the data rebuilt from bronze.

## Partition layout

Unlike the low-volume groups, this is **many immutable date-partitioned part
files** — one per flushed micro-batch — because the volume is enormous and the data
is append-only. A daily compaction merges the small part files per settled
partition.

```
bronze/group=microstructure/source=<source>/kind=<kind>/symbol=<symbol>/date=<YYYY-MM-DD>/part-<stamp>.parquet
```

- `source` — exchange, e.g. `binance` or `coinbase`. Partition key. **Multiple
  venues capture the same `symbol` into the same partition tree**, with `source`
  distinguishing the book — Binance BTC/USDT (offshore, stablecoin) vs Coinbase
  BTC/USD (US, fiat) — which is exactly the cross-venue basis/lead-lag signal (see
  [`qde.analytics`](../../src/qde/analytics.py)).
- `kind` — message kind (below). Partition key.
- `symbol` — canonical symbol (e.g. `BTCUSDT`), same across venues. Partition key.
- `date` — UTC date of the flush. Partition key (the high-volume group *does*
  date-partition).

## Kinds and their columns

Every row carries `symbol` and `received_at` (local arrival time, epoch ms) — the
one field the exchange never sends, stamped live by our collector on the **same
clock for every venue**, so feed latency (`received_at − event_time`) is
recoverable *and* two venues are alignable free of exchange clock-skew (the basis
of the cross-venue analytics).

### Binance (`source=binance`)

| `kind` | Columns (beyond `symbol`, `received_at`) | Continuity key |
|---|---|---|
| `trades` | `trade_id`, `price`, `quantity`, `trade_time`, `event_time`, `buyer_is_maker` | `trade_id` (contiguous) |
| `depth` | `first_update_id`, `final_update_id`, `event_time`, `bids`, `asks` | update-id chain (contiguous) |
| `book_ticker` | `bid_price`, `bid_qty`, `ask_price`, `ask_qty`, `update_id` | `update_id` (ordering only) |
| `snapshot` | `last_update_id`, `bids`, `asks` | anchors the depth stream |
| `gaps` | continuity records — see [The `gaps` kind](#the-gaps-kind) | — |
| `session` | start/stop markers (`symbol=_all`) | bounds restart downtime |

`bids`/`asks` are preserved as lists of `[price, quantity]` string pairs; the book
is reconstructed downstream from these deltas anchored on the periodic REST
`snapshot`.

### Coinbase (`source=coinbase`)

The bronze contract is **per-venue faithful**: Coinbase sends prices/sizes as
strings and every timestamp as an **ISO-8601 string** (not epoch ms), so its rows
are kept exactly as sent and its columns differ from Binance's. The silver layer
casts and reconciles per source. (See [`qde.stream.venues.coinbase`](../../src/qde/stream/venues/coinbase.py).)

| `kind` | Columns (beyond `symbol`, `received_at`) | Continuity key |
|---|---|---|
| `trades` | `trade_id`, `price`, `quantity`, `side`, `trade_time`, `sequence` | `trade_id` (contiguous) |
| `depth` | `changes` (`[side, price, size]` triples), `event_time` | *unsequenced* — no update id |
| `book_ticker` | `bid_price`, `bid_qty`, `ask_price`, `ask_qty`, `update_id`, plus last-trade `price`, `last_size`, `trade_id`, `side`, `event_time` | `update_id` (= per-product `sequence`, ordering only) |
| `snapshot` | `bids`, `asks`, `snapshot_time` | anchors the depth stream (inline, no id) |
| `heartbeat` | `last_trade_id`, `sequence`, `heartbeat_time` | once/sec liveness beacon |
| `gaps` | continuity records (reconnects; no per-message depth jumps) | — |
| `session` | start/stop markers (`symbol=_all`) | bounds restart downtime |

Three structural differences from Binance, each a deliberate venue fidelity choice:

- **`book_ticker` is trade-coupled.** Coinbase's top-of-book comes from the `ticker`
  channel, which emits once per trade — so `book_ticker` row count equals `trades`
  row count, and it carries the last-trade fields inline. Binance's `book_ticker`
  fires on every best-bid/ask *size* change (decoupled from trades, ~20× chattier).
- **`depth` is a `changes` diff with no update id** (unsequenced), and the book
  anchors from an **inline** `snapshot` on every (re)connect rather than a REST
  pull. So per-message depth continuity is not checkable; depth re-anchors on
  reconnect, and `heartbeat` covers the quiet-market liveness case.
- **`heartbeat`** is a Coinbase-only kind (no Binance equivalent).

Because the per-venue schemas differ, a query spanning both venues reads with
`union_by_name=true` (missing columns fill NULL) — which `qde.lake`'s microstructure
views do, so `SELECT ... FROM book_ticker WHERE source IN ('binance','coinbase')`
just works.

## The `gaps` kind

Both venues write to the same `gaps` partition, and it is the one place the lake
admits to its own holes. A consumer that ignores it will silently treat a missing
stretch of tape as a quiet market.

| Column | Meaning |
|---|---|
| `stream_kind` | which stream the record concerns (`trades`, `depth`, `book_ticker`) |
| `symbol` | canonical symbol |
| `reason` | `sequence_jump`, `reconnect`, or `handover` — see below |
| `last_seq` / `next_seq` | the ids either side of a `sequence_jump` |
| `missing_count` | messages lost, where countable; `0` for a handover; null when unknowable |
| `gap_start_ms` / `gap_end_ms` | wall-clock bounds of the window |
| `duplicates` | overlap messages discarded during a `handover`; null otherwise |

**`sequence_jump`** — real missed data. The id chain skipped, so messages the venue
sent never arrived. Un-backfillable: this is the honest record that a hole exists.

**`reconnect`** — the socket dropped and was re-established. Nothing was received in
the window, so the loss is bounded by wall clock rather than by id.

**`handover`** — a *planned* connection replacement that lost nothing, recorded
because "the connection changed here" is what someone auditing a suspicious hour
needs to know, and because its absence around a scheduled cycle would mean the
handover silently failed. `missing_count` is `0` rather than null: null means
"unknowable" elsewhere in this table, and here it is known to be none.

The collector replaces a connection roughly daily, because **a connection left open
too long degrades without closing**: measured on this lake, Binance dropped a burst
of messages every 48.2–48.9 hours of continuous connection — all streams jumping
within 2 ms of each other, worst case ~2,900 depth messages, with no disconnect and
no error. The successor socket is opened and buffering *before* the predecessor is
closed, so the window that would have been a gap is covered by both; the overlap is
then discarded by id, and `duplicates` records how much was discarded.

That is only sound where every captured stream carries a monotonic id. A Coinbase
capture **including `depth`** cannot do it — `l2update` has no update id, so a
replayed diff is indistinguishable from a new one, and replaying an old one silently
rewinds the book. Those captures fall back to close-then-reopen and record an honest
`reconnect` instead. So expect `handover` rows from Binance, and `reconnect` rows
from Coinbase, for the same scheduled event.

## Why the shape differs

On Binance, `book_ticker` fires on every change to the best bid/ask size, so it is
the chattiest kind (~0.5–1 GB/day for 3 symbols × all kinds, ~27k files/day).
Coinbase's trade-coupled `ticker` makes its `book_ticker` far smaller (one row per
trade, not per size change), so the second venue adds proportionally little volume.
This is why the group is strictly scoped (a small symbol set) and why retention is
the one place the "house everything" ambition does **not** apply — see
[`../data-sources.md`](../data-sources.md) and ROADMAP §5.2 / §11.
