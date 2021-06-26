from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.scrape import scrapeTweets
from etl.process_df import clean_df

from functools import partial

def extract_tweets():
    scrape_time = datetime.now().isoformat()
    name_file = f"{scrape_time}_CPI.csv"
    scrape_obj = scrapeTweets("CPI")
    scrape_obj.save_csv(name_file)
    return name_file

def clean_data(ti):
    scrape_name = ti.xcom_pull(task_ids=[
        "extract_from_twitter"
    ])
    keep_columns = ["date", "timezone", "tweet", "language"]
    c_df = clean_df(scrape_name, keep_columns)
    return c_df

def clean_data():
    return True

with DAG("etl_dag",
        start_date=datetime(2021, 1, 1),
         schedule_interval="@hourly",
         catchup=False,
        ) as dag:
    
        extract = PythonOperator(
            task_id="extract_from_twitter",
            python_callable=extract_tweets,
        )

        clean = PythonOperator(
            task_id="clean_data",
            python_callable=clean_data,
        )

        extract >> clean