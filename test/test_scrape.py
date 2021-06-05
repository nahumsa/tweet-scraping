import unittest
from etl.scrape import scrape_tweets


class TestScrape(unittest.TestCase):

    def test_check_df(self):
        df_scrape = scrape_tweets("sa_nahum", 10)
        self.assertIn("tweet", df_scrape.columns)
