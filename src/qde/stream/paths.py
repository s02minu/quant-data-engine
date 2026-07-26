"""Bronze partition layout for captured stream messages."""

from datetime import datetime
from pathlib import Path

from qde.stream.config import StreamConfig


def bronze_path(
    config: StreamConfig,
    kind: str,
    symbol: str,
    batch_time: datetime,
) -> Path:
    """Build the part-file path for one flushed micro-batch.

    Uses a Hive-style partition layout with `group` outermost, so a query
    engine can prune by kind, symbol, and date instead of scanning the whole
    lake. This is the single source of truth for where captured data lives.

    Args:
        config: Run config; supplies base_dir, group, and source.
        kind: Message kind, e.g. "trades" or "depth". A partition key.
        symbol: Canonical uppercase symbol, e.g. "BTCUSDT". A partition key.
        batch_time: UTC time of the flush. Its date selects the date
            partition; its full timestamp makes the filename unique so
            successive flushes on the same day never overwrite each other.

    Returns:
        Path to the part file. Parent directories are not created here; the
        caller creates them before writing, as in qde.storage.
    """
    date_str = batch_time.strftime("%Y-%m-%d")
    # %f is microseconds (6 digits); [:-3] trims to milliseconds. Z marks UTC.
    # Millisecond precision keeps successive flushes from colliding on a filename.
    stamp = batch_time.strftime("%Y%m%dT%H%M%S%f")[:-3] + "Z"

    return (
        Path(config.base_dir)
        / "bronze"
        / f"group={config.group}"
        / f"source={config.source}"
        / f"kind={kind}"
        / f"symbol={symbol}"
        / f"date={date_str}"
        / f"part-{stamp}.parquet"
    )
