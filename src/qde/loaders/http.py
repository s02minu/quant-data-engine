"""The one HTTP boundary every batch ingestor goes through."""

import time

import requests

from qde.loaders import budget

# (connect, read) in seconds. A request without a timeout waits *forever* on a
# connection that opens and then goes silent — the socket stays half-open, requests
# blocks, and the nightly simply never finishes. Nothing raises, nothing alerts, and
# the process looks healthy the whole time, which is the worst shape a failure can
# take. Read is generous because a wide backfill page legitimately takes a while.
_TIMEOUT = (10, 60)


def get_with_requests(url, params, max_retries=4, timeout=_TIMEOUT, source=None):
    """Make a GET request, retrying on temporary failure.

    Every attempt is charged to the source's hourly budget before it is made --
    retries included. A 429 means the allowance is already gone, so the old
    behaviour of retrying four times *outside* any accounting spent four more
    requests proving a limit that had already been proven. ``source`` is normally
    left unset and taken from the fetch context ``BaseIngestor`` establishes; a
    source declaring no hourly limit is charged nothing.

    Retries rate limits (429), server errors (5xx), and transport failures
    (timeout, connection reset, DNS blip) with exponential backoff. A client error
    other than 429 raises immediately, since retrying a bad request just wastes the
    rate-limit budget.

    Args:
        url: the endpoint URL.
        params: query parameters.
        max_retries: attempts before giving up.
        timeout: ``(connect, read)`` seconds passed to requests.

    Returns:
        requests.Response: the successful response.

    Raises:
        ValueError: retries exhausted, or a permanent client error.
    """
    last_error = None
    metered = source or budget.current_source()

    for attempt in range(max_retries):
        # Charged before the socket opens, so a request that is made is a request
        # that was counted. Charging on success instead would let a run of 429s --
        # the very thing the quota produces -- pass through unmetered.
        budget.consume(metered)
        try:
            response = requests.get(url, params=params, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            # Transport-level failure. Treated like a 5xx rather than raised on the
            # spot: a dropped connection or a slow DNS answer is exactly the kind of
            # transient the backoff exists for, and failing the whole source over one
            # is how a nightly reports a gap that never really existed.
            last_error = exc
            if attempt == max_retries - 1:
                break
            time.sleep(2**attempt)
            continue

        # Success
        if response.status_code == 200:
            return response

        # Rate limited or server error - wait and retry
        if response.status_code == 429 or response.status_code >= 500:
            last_error = ValueError(f"status {response.status_code}")
            wait = 2**attempt  # 1, 2, 4, 8
            time.sleep(wait)
            continue

        # Permanent client error - don't retry
        raise ValueError(f"Request failed with status code {response.status_code}: {url}")

    # Exhausted all retries. The cause is carried through so a failure that turns
    # out to be systemic can be diagnosed from the nightly's log alone.
    raise ValueError(f"Request failed after {max_retries} retries: {url} ({last_error})")
