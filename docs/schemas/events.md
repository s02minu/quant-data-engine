# `events` schema

An `event` is a **scheduled, sparse release**: an economic figure (or earnings, or
a central-bank decision) that is published at a known time and then possibly
revised. The economic calendar is the canonical example. Tiny in volume, but the
temporal model has to be right — getting it wrong bakes lookahead bias into every
backtest that uses it.

`events` is distinct from [`series`](series.md): a `series` is the *stream of
values* a figure takes over time; an `event` is the *release* — when a value became
knowable, alongside what the market expected and the prior print. They compose (an
event's `actual` is a value that also lands in the series), but they answer
different questions: "what is CPI over time" (`series`) vs "when was the January CPI
released, what was expected, and what was the surprise" (`events`).

## Bitemporality — the point of the group

Two clocks matter, and storing only one destroys the data's usefulness:

- **`scheduled_ts`** — when the release *was scheduled* to happen (08:30 ET on
  release day). This is the only time available for a backtest: you cannot act on a
  number before it exists.
- **`observed_ts`** — when a value *actually became known* — the release, and then
  each revision. A revision is a new row with a later `observed_ts`.

A calendar that keeps only the current value silently rewrites history: the number
that existed at 08:30 on release day is not the number after two revisions. Storing
both clocks — *what was known, and when it was known* — is what makes the calendar
safe to backtest against, and it costs almost nothing (ROADMAP §3.4).

## Partition layout

Sparse and tiny — one **file per calendar**, keyed by source and a named calendar.
Rows are ordered by `scheduled_ts`; the release date lives as a column, not a
partition (same small-files reasoning as `series`).

```
bronze/group=events/source=<source>/calendar=<calendar>/events.parquet
```

- `source` — origin of the calendar, e.g. `fred`.
- `calendar` — a named slice, e.g. `us_macro`, `earnings`. Lets unrelated event
  streams live side by side without one giant file.

## Column contract (bronze)

| Column | Type | Notes |
|---|---|---|
| `event_id` | string | Stable id for one release — a specific figure for a specific reference period, `<series_id>:<ref-date>`, e.g. `CPIAUCSL:2024-06-01`. **Same across every revision of that release**, which is what makes it the per-event key the DQ checks group by. |
| `series_id` | string | The `series` this release feeds, when applicable (`CPIAUCSL`). Nullable for pure events (a rate decision). |
| `scheduled_ts` | timestamp, UTC | When the release was scheduled. The backtest-safe clock. |
| `observed_ts` | timestamp, UTC | When this row's value became known (release, or the revision's date). |
| `actual` | float64 | The released value; `NaN` before release / if unpublished. |
| `forecast` | float64 | Consensus expectation prior to release. **Nullable — not available free** (see below). |
| `previous` | float64 | The prior period's value at release time. |
| `revision_seq` | int | `0` = initial print; `1, 2, …` for successive revisions of the same `event_id`. |

The row key is `(event_id, revision_seq)` — one row per release and per revision.

## Deriving the surprise

The signal a macro model wants is the **surprise**: `actual − forecast` (often
normalized). That single subtraction is *why* `forecast` matters — and it is the
one column free public sources do not provide.

- `actual`, `previous`, `scheduled_ts`, `observed_ts`, and full revision history
  come **free and redistributable** from FRED releases + ALFRED vintages.
- `forecast` (economists' consensus) is **proprietary** — Trading Economics, FMP,
  Bloomberg. It ships as a **code-only** enrichment: the open calendar carries a
  `NaN` forecast, and a user with a vendor key backfills the column into the same
  schema. This is the "two halves" product shape (ROADMAP §6), applied at the
  column level.

## How current/first sources map

| Source | Fills | Redistributable | Notes |
|---|---|---|---|
| **FRED releases** | `scheduled_ts`, `event_id`, `series_id` | yes | The release calendar: which series publish when. |
| **ALFRED vintages** | `observed_ts`, `actual`, `previous`, `revision_seq` | yes | Revision history reconstructs the bitemporal rows. |
| Vendor calendar (TE/FMP) | `forecast` | **no** (code-only) | The consensus column only. Layered on top per user. |

## Data-quality notes (feed Phase 9)

- **`observed_ts >= scheduled_ts`** for every row — a value cannot be known before
  it is scheduled to exist. This is *the* bitemporal ordering test, and the most
  interesting custom financial check the platform has (ROADMAP §9).
- **`revision_seq` is contiguous from 0** per `event_id`, and `observed_ts`
  increases with `revision_seq`.
- **One initial print** (`revision_seq = 0`) per `event_id`.
- A `NaN` `forecast` is expected (the free calendar); it is not a defect.

All three are enforced by `qde.checks.run_events_checks`, wired into the nightly
`daily_update` beside the bars/series/microstructure passes (verified clean on the
seeded 184k-event calendar).

## As implemented

- **Source `fredcal`, calendar `us_macro`** — a `SourceSpec` (group `events`) over
  the eleven *revisable, scheduled* macro releases in the FRED spine (CPI/core CPI,
  PCE/core PCE, payrolls, unemployment, jobless claims, real GDP, industrial
  production, retail sales, housing starts). Daily market rates (`DGS*`, `SOFR`) and
  balance-sheet plumbing stay `series`-only — they are continuous observations, not
  scheduled-and-revised announcements. The ingestor is `qde.ingest.fred_releases`.
- **One request, the whole grid.** ALFRED returns the full vintage history when the
  observations endpoint is asked for the entire real-time range; `normalize` folds
  the `(reference period, vintage)` rows into one event per period, one row per
  revision.
- **`scheduled_ts` = the first vintage's `realtime_start`** (the release date).
  FRED does not cleanly map a scheduled date to a not-yet-published reference
  period, so the first appearance is the honest release clock — for historical data
  the scheduled and actual publication coincide. This makes `observed_ts >=
  scheduled_ts` hold by construction (equality for the initial print).
- **`previous` = the prior reference period's initial print** — a documented
  approximation of "the prior period's value at release time"; exact bitemporal
  previous is derivable but no DQ check depends on it.
- **ALFRED coverage** begins in the mid-1990s, so the platform seeds from
  `2000-01-01` (the genuine-revision era); earlier periods would carry a single
  vintage dated to ALFRED's start rather than a true release. The nightly refresh
  re-pulls in full (a revision is a new row for an old period, so a watermark-
  advancing pull would miss it) — cheap because the calendar is tiny and the upsert
  is idempotent.
