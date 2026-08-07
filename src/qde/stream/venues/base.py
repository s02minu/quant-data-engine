"""The venue seam: everything exchange-specific about a capture, behind one ABC.

The collector owns the venue-neutral machinery — buffering, timed flush,
reconnect with backoff, sequence-gap tracking, session markers, and the bronze
partition layout. A `VenueAdapter` owns the parts that genuinely differ between
exchanges:

    * how streams are named and subscribed (Binance encodes them in the URL;
      Coinbase connects to a fixed URL and sends a subscribe frame),
    * how a raw message is decoded into a normalised bronze row, and
    * whether the order book is anchored by a REST snapshot (Binance) or by an
      inline snapshot the socket delivers on every connect (Coinbase).

Adding a venue is a new adapter, not a fork of the collector.
"""

from abc import ABC, abstractmethod

from qde.stream.config import StreamConfig


class VenueAdapter(ABC):
    """One exchange's websocket dialect, normalised to the bronze contract.

    Attributes:
        name: The partition `source=` key, e.g. "binance" or "coinbase". Must
            match the exchange the adapter speaks; the collector stamps it onto
            every file it writes.
    """

    name: str

    @abstractmethod
    def native_symbol(self, canonical: str) -> str:
        """Translate a canonical symbol (BTCUSDT) to the venue's own (BTC-USD)."""

    @abstractmethod
    def ws_url(self, config: StreamConfig) -> str:
        """The websocket URL to connect to.

        Binance encodes every subscription in the query string here; Coinbase
        returns a fixed feed URL and does its subscribing in `subscribe_frames`.
        """

    def subscribe_frames(self, config: StreamConfig) -> list[dict]:
        """Frames to send once, immediately after the connection opens.

        Coinbase subscribes this way; Binance needs nothing (its subscriptions
        live in the URL), so the default is no frames.
        """
        return []

    @property
    def max_frame_bytes(self) -> int:
        """`max_size` for `websockets.connect`.

        Defaults to the library's own default (1 MiB). A venue that pushes a
        larger frame — Coinbase's full order-book snapshot is >1 MiB and trips
        the default limit — raises this.
        """
        return 2**20

    @property
    def rest_snapshots(self) -> bool:
        """Whether the collector should poll REST order-book snapshots.

        True for venues whose diff stream is meaningless without a separately
        fetched anchor (Binance). False for venues that deliver the snapshot
        inline over the socket on every connect (Coinbase).
        """
        return False

    def fetch_snapshot(self, config: StreamConfig, symbol: str, received_at: int) -> dict:
        """Fetch one REST order-book snapshot row. Only called when
        `rest_snapshots` is True; the base implementation is never reached."""
        raise NotImplementedError

    @abstractmethod
    def route(self, message: dict, received_at: int) -> tuple[str, dict] | None:
        """Decode one raw (already JSON-parsed) message into a bronze row.

        Args:
            message: The decoded websocket frame.
            received_at: Local arrival time, epoch ms, stamped before decoding.

        Returns:
            `(kind, row)` where `kind` is the partition key and `row` carries a
            canonical `symbol`, or `None` for a message that is not captured
            (subscription acknowledgements and the like).
        """
