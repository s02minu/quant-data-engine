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

- `source` — exchange, e.g. `binance`. Partition key.
- `kind` — message kind (below). Partition key.
- `symbol` — canonical symbol. Partition key.
- `date` — UTC date of the flush. Partition key (the high-volume group *does*
  date-partition).

## Kinds and their columns

Every row carries `symbol` and `received_at` (local arrival time, epoch ms) — the
one field the exchange never sends, stamped live so feed latency
(`received_at − event_time`) is recoverable.

| `kind` | Columns (beyond `symbol`, `received_at`) | Continuity key |
|---|---|---|
| `trades` | `trade_id`, `price`, `quantity`, `trade_time`, `event_time`, `buyer_is_maker` | `trade_id` (contiguous) |
| `depth` | `first_update_id`, `final_update_id`, `event_time`, `bids`, `asks` | update-id chain (contiguous) |
| `book_ticker` | `bid_price`, `bid_qty`, `ask_price`, `ask_qty`, `update_id` | `update_id` (ordering only) |
| `snapshot` | `last_update_id`, `bids`, `asks` | anchors the depth stream |
| `gaps` | detected sequence gaps | — |
| `session` | start/stop markers | bounds restart downtime |

`bids`/`asks` are preserved as lists of `[price, quantity]` string pairs; the book
is reconstructed downstream from these deltas anchored on the periodic `snapshot`.

## Why the shape differs

`book_ticker` fires on every change to the best bid/ask size, so it is the chattiest
kind (~0.5–1 GB/day for 3 symbols × all kinds, ~27k files/day). This volume is why
the group is strictly scoped (a small symbol set) and why retention is the one place
the "house everything" ambition does **not** apply — see [`../data-sources.md`](../data-sources.md)
and ROADMAP §5.2 / §11.
