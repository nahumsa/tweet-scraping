import twint
import pandas as pd


def scrape_tweets(username: str, n_tweets: int) -> pd.DataFrame:
    
    c = twint.Config()
    c.Limit = n_tweets
    c.Username = username
    c.Pandas = True

    twint.run.Search(c)

    return twint.storage.panda.Tweets_df
    

scrape_tweets("sa_nahum", 10).to_csv("nahum.csv")
