import pandas as pd
import pandas_market_calendars as mcal


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

    if calendar == 'equity':
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=df.index.min(), end_date=df.index.max())
        expected = schedule.index.tz_localize('UTC')
        missing = expected.difference(df.index)

    else:
        full_range = pd.date_range(df.index.min(), df.index.max(), freq='D', tz='UTC')
        missing = full_range.difference(df.index)


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
    bad_rows = (
        (df["close"] <= 0) |
        (df["open"] <= 0) |
        (df["high"] <= 0) |
        (df["low"] <= 0) |
        (df["high"] < df["low"])
    )

    return df[bad_rows]


# The check runner
def run_quality_report(df, name='dataset', calendar='crypto'):
    """
    Runs all quality checks functions.

    Args:
        df: pd.DataFrame.
        name: str.
        calendar: str.

    Returns:
        Report Status

    """

    gaps= check_gaps(df=df, calendar=calendar)
    price_sanity = check_price_sanity(df)
    nulls = check_nulls(df)
    duplicates = check_duplicates(df)

    print(f'====== Quality Report: {name} ======\n')
    print(f'Gaps:             {len(gaps)}')
    print(f'Duplicates:       {len(duplicates)}')
    print(f'Nulls:            {nulls.sum()}')
    print(f'Price issues:     {len(price_sanity)}')

    all_clean = len(gaps) == 0 and len(duplicates) == 0 and nulls.sum() == 0 and len(price_sanity) == 0

    print(f'Status:           {"CLEAN" if all_clean else "ISSUES FOUND"}')



