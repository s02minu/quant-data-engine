class NoNewData(ValueError):
    """A loader made a successful request that returned zero rows.

    This is the benign "already up to date" case -- the source simply has
    nothing newer to give -- and must be kept distinct from a real failure such
    as an unknown/delisted symbol or an API error (a 400/500), which raise a
    plain ``ValueError``. Conflating the two makes a dead series look healthy:
    an incremental update that swallows *any* ``ValueError`` treats an outage as
    "current" and the data quietly goes stale.

    Subclasses ``ValueError`` so callers that already catch ``ValueError`` on the
    empty-response path keep working, while a caller that wants to narrow -- like
    ``qde.storage.update_ohlcv`` -- can catch ``NoNewData`` alone and let real
    ``ValueError`` failures propagate.
    """
