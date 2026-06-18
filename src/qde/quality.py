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

# Check for duplicates
def check_duplicates(df):
    """
    Finds duplicate dates.

    Args:
     df: pd.DataFrame.

    Returns:
        pd.DataFrame of duplicate dates.
    """
    # Find duplicated index
    duplicates = df[df.index.duplicated(keep=False)]

    return duplicates


# Check for nulls
def check_nulls(df):
    """
    Finds null rows.

    Args:
     df: pd.DataFrame.

    Returns:
       Series with null count per column.
    """
    # Find null index
    nulls = df.isnull().sum()

    return nulls


# Price sanity check
def check_price_sanity(df):
    """
    Finds price insanities.

    Args:
        df: pd.DataFrame.

    Returns:
        pd.Dataframe with price insanities.
    :param df:
    :return:
    """
    # Price sanity check
    bad_rows = (
        (df["close"] <= 0) |
        (df["open"] <= 0) |
        (df["high"] <= 0) |
        (df["low"] <= 0) |
        (df["high"] < df["low"])
    )

    return df[bad_rows]

