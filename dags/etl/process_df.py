import pandas as pd
import datetime


def clean_df(path: str, keep_columns:list) -> pd.DataFrame:
    """Clean your dataframe in order to have the
    keep columns.

    Args:
        path (str): [description]
        keep_columns (list): [description]

    Returns:
        pd.DataFrame: [description]
    """

    df = pd.read_csv(path)
    tweet_df = df[keep_columns]

    if "date" not in tweet_df.columns:
        raise ValueError("The dataframe doesn't have a date column.")
    
    elif "timezone" not in tweet_df.columns:
        raise ValueError("The dataframe doesn't have a timezone column.")
    
    else:
        return convert_time(tweet_df)


 
def convert_timezone(df:pd.DataFrame) -> pd.DataFrame:
    """Convert timezones given a dataframe with `date`
    and `timezone` columns.

    Args:
        df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    return df["date"] + datetime.timedelta(hours=df['timezone']/100)

def convert_time(df: pd.DataFrame) -> pd.DataFrame:
    # convert datetime
    df['date'] = pd.to_datetime(df["date"])
    
    # apply timezone to convert to UTC
    df["date"] = df.apply(convert_timezone, axis=1)

    df = df.drop("timezone", axis=1)

    return df
