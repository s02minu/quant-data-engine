"""Continuous capture entry point: python -m qde.stream."""

import asyncio
import contextlib
import os
import signal

from qde.log import configure
from qde.stream.collector import StreamCollector
from qde.stream.config import StreamConfig


async def main() -> None:
    configure()
    # StreamConfig is a pydantic-settings model: in-code defaults, each field
    # overridable from the environment under the QDE_ prefix (see config.py).
    config = StreamConfig()
    # Bounds a run for testing; unset means capture indefinitely.
    raw_max = os.getenv("QDE_MAX_MESSAGES")
    max_messages = int(raw_max) if raw_max else None

    collector = StreamCollector(config)
    task = asyncio.create_task(collector.run(max_messages=max_messages))

    # Turn container stop signals into task cancellation so the collector's
    # finally block runs and flushes buffered rows. `docker stop` sends SIGTERM;
    # the default handler would kill the process mid-buffer and lose
    # un-backfillable data. add_signal_handler is unavailable on Windows, where
    # this falls back to the default KeyboardInterrupt handling.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, AttributeError):
            loop.add_signal_handler(sig, task.cancel)

    with contextlib.suppress(asyncio.CancelledError):
        await task


if __name__ == "__main__":
    asyncio.run(main())
