"""Sequence continuity tracking across captured streams.

A dropped message is invisible in the data itself: fewer rows looks exactly
like a quieter market. The only defence is the sequence id each stream carries,
and the only useful response is to record the break explicitly, so a hole in
the tape is never mistaken for an absence of activity.

Continuity guarantees differ by kind:

    trades       `trade_id` is contiguous per symbol, so a break yields an
                 exact count of missing trades.
    depth        each message's `first_update_id` must follow the previous
                 `final_update_id`; a jump means deltas were missed and the
                 book can no longer be reconstructed across that point.
    book_ticker  `update_id` only ever increases, by an arbitrary amount, so
                 missing messages cannot be counted. Only ordering is checked.
"""

SEQUENCE_JUMP = "sequence_jump"
RECONNECT = "reconnect"

SESSION_START = "start"
SESSION_STOP = "stop"


def session_record(
    event: str,
    at_ms: int,
    message_count: int | None = None,
    gap_count: int | None = None,
) -> dict:
    """Mark the start or clean stop of a capture session.

    Sequence tracking cannot span a process restart, so a gap across one is not
    a sequence jump but a wall-clock outage: the collector simply was not
    running. Recording when each session begins and ends makes that downtime
    the visible span between one session and the next, rather than a silent
    hole no id can reveal.

    Args:
        event: `SESSION_START` or `SESSION_STOP`.
        at_ms: Wall-clock time of the event, epoch ms.
        message_count: Messages handled this session (on stop).
        gap_count: Gaps recorded this session (on stop).
    """
    return {
        "event": event,
        "at_ms": at_ms,
        "message_count": message_count,
        "gap_count": gap_count,
    }


def _gap_record(
    stream_kind: str,
    symbol: str,
    reason: str,
    last_seq: int | None,
    next_seq: int | None,
    missing_count: int | None,
    gap_start_ms: int | None,
    gap_end_ms: int,
) -> dict:
    """Assemble one gap record.

    `stream_kind` names the stream the gap occurred in; the record itself is
    stored under the `gaps` partition.
    """
    return {
        "stream_kind": stream_kind,
        "symbol": symbol,
        "reason": reason,
        "last_seq": last_seq,
        "next_seq": next_seq,
        "missing_count": missing_count,
        "gap_start_ms": gap_start_ms,
        "gap_end_ms": gap_end_ms,
    }


def reconnect_gap(
    stream_kind: str,
    symbol: str,
    disconnected_at: int,
    reconnected_at: int,
) -> dict:
    """Record the outage between losing a connection and resuming it.

    Nothing was received in this window, so the break is known by wall clock
    rather than by sequence id.
    """
    return _gap_record(
        stream_kind=stream_kind,
        symbol=symbol,
        reason=RECONNECT,
        last_seq=None,
        next_seq=None,
        missing_count=None,
        gap_start_ms=disconnected_at,
        gap_end_ms=reconnected_at,
    )


class SequenceTracker:
    """Remembers the last sequence id per stream and reports discontinuities."""

    def __init__(self) -> None:
        self._last: dict[tuple[str, str], int] = {}

    def reset(self) -> list[tuple[str, str]]:
        """Forget every tracked position.

        Called on reconnect: the outage is already recorded, and the first
        message of a new connection would otherwise register as a second,
        spurious sequence jump.

        Returns:
            The (kind, symbol) pairs that were being tracked.
        """
        tracked = list(self._last)
        self._last.clear()
        return tracked

    def check(self, kind: str, symbol: str, row: dict) -> dict | None:
        """Compare a row against the expected next sequence id.

        Args:
            kind: Stream kind the row came from.
            symbol: Canonical symbol.
            row: Parsed bronze row.

        Returns:
            A gap record if the sequence broke, otherwise None.
        """
        key = (kind, symbol)
        previous = self._last.get(key)

        if kind == "trades":
            current = row["trade_id"]
            expected_start = current
        elif kind == "depth":
            # The chain runs final_update_id -> next first_update_id. Venues
            # whose diff stream carries no per-message update id (Coinbase's
            # level2_batch) cannot be checked this way; they re-anchor from the
            # inline snapshot on reconnect and lean on the heartbeat instead.
            if "final_update_id" not in row:
                return None
            current = row["final_update_id"]
            expected_start = row["first_update_id"]
        elif kind == "book_ticker":
            current = row["update_id"]
            expected_start = None  # increments are arbitrary; nothing to predict
        else:
            return None

        # First message on this stream establishes the baseline.
        if previous is None:
            self._last[key] = current
            return None

        if kind == "book_ticker":
            # Only a non-increasing id is anomalous, and it cannot be counted.
            if current > previous:
                self._last[key] = current
                return None
            return _gap_record(
                stream_kind=kind,
                symbol=symbol,
                reason=SEQUENCE_JUMP,
                last_seq=previous,
                next_seq=current,
                missing_count=None,
                gap_start_ms=None,
                gap_end_ms=row["received_at"],
            )

        self._last[key] = current

        # Contiguous: the next id must be exactly one past the last.
        if expected_start == previous + 1:
            return None

        # Replays and duplicates move backwards; they are not missing data.
        if expected_start <= previous:
            return None

        return _gap_record(
            stream_kind=kind,
            symbol=symbol,
            reason=SEQUENCE_JUMP,
            last_seq=previous,
            next_seq=expected_start,
            missing_count=expected_start - previous - 1,
            gap_start_ms=None,
            gap_end_ms=row["received_at"],
        )
