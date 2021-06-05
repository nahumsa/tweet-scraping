import twint
import pandas as pd


class scrapeTweets():

    def __init__(self, username: str, n_tweets: int) -> None:
        self._tweet_df = self._scrape(username, n_tweets)

    @property
    def df(self):
        return self._tweet_df

    def _scrape(self, username: str, n_tweets: int) -> pd.DataFrame:
        c = twint.Config()
        c.Limit = n_tweets
        c.Username = username
        c.Pandas = True
        c.Hide_output = True

        twint.run.Search(c)

        return twint.storage.panda.Tweets_df

    def save_csv(self, path):
        self._tweet_df.to_csv(path)
