import pandas as pd


# Gap detection
def check_gaps(df: pd.DataFrame, calendar: str = 'crypto') -> pd.DataFrame:
    """
    Builds a daily range from strat to end.
    Compares with date index for missing dates.

    Arg:
        df: pd.DataFrame.
        calendar: str.

    Returns:
        pd.DataFrame of missing dates.
    """

    # Build Daily Range
    full_range = pd.date_range(df.index.min(), df.index.max(), freq='D', tz='UTC')

    # Find Missing Dates
    missing = full_range.difference(df.index)

    if calendar == 'equity':
        missing = missing[missing.dayofweek < 5]

    return missing