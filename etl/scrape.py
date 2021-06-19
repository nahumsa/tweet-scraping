import twint
import pandas as pd
import datetime


class scrapeTweets():

    def __init__(self, keyword: str) -> None:
        self._tweet_df = self._scrape(keyword)

    @property
    def df(self):
        return self._tweet_df

    def _scrape(self, keyword: str) -> pd.DataFrame:
        c = twint.Config()
        c.Search = keyword
        c.Since = str(datetime.datetime.now() - datetime.timedelta(hours=1))[:19]
        c.Pandas = True
        c.Hide_output = True

        twint.run.Search(c)

        return twint.storage.panda.Tweets_df

    def save_csv(self, path):
        self._tweet_df.to_csv(path)
