import unittest
import os
from etl.scrape import scrapeTweets


class TestScrape(unittest.TestCase):

    def setUp(self):
        self.scrape_obj = scrapeTweets("CPI")
        self.save_path = "cpi.csv"
    
    def test_check_df_columns(self):
        df_scrape = self.scrape_obj.df
        self.assertIn("id", df_scrape.columns)
        self.assertIn("tweet", df_scrape.columns)
        self.assertIn("date", df_scrape.columns)
        self.assertIn("timezone", df_scrape.columns)
    
    def test_data_save(self):
        self.scrape_obj.save_csv(self.save_path)
        self.assertIn(self.save_path, os.listdir())

    def tearDown(self):
        if self.save_path in os.listdir():
            os.remove(self.save_path)