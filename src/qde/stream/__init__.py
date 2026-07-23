"""
Streaming ingestion (websockets) — the `microstructure` group.

Unlike the REST loaders in `qde.loaders` (pull-based, backfillable OHLCV),
this subpackage captures push-based, un-backfillable data: live trades and
order-book deltas. The design principle is "capture, don't interpret" — we
persist raw exchange messages to the bronze layer and defer any order-book
reconstruction to a later, replayable transformation.
"""
