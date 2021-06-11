import unittest
import os
import pandas as pd
import datetime

from etl.process_df import clean_df, convert_time


class TestCleanDF(unittest.TestCase):

    def test_convert_timezone(self):
        rng = pd.date_range('2015-02-24', periods=5, freq='T')
        df = pd.DataFrame({ 'date': rng, 'timezone': +100 }) 
        
        exp_df = df.copy()
        exp_df['date'] += datetime.timedelta(hours=+1)
        exp_df = exp_df.drop("timezone", axis=1)

        got_df = convert_time(df)

        pd.testing.assert_frame_equal(got_df, exp_df)

    def test_check_no_date(self):
        with self.assertRaises(ValueError):
            d = {'col1': [1, 2], 'col2': [3, 4]}
            df = pd.DataFrame(data=d)
            
            df.to_csv("mock.csv", index=False)
            
            clean_df("mock.csv", ['col1'])
    
    def test_check_no_timezone(self):
        with self.assertRaises(ValueError):
            d = {'date': [1, 2], 'col2': [3, 4]}
            df = pd.DataFrame(data=d)
            
            df.to_csv("mock.csv", index=False)
            
            clean_df("mock.csv", ['date'])

    def tearDown(self):
        if "mock.csv" in os.listdir():
            os.remove("mock.csv")
    
