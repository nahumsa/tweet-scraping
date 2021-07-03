import psycopg2
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.scrape import scrapeTweets
from etl.process_df import clean_df
from etl.database import create_table, add_df_to_sql
from sqlalchemy import create_engine

TABLE_NAME = "tweet_body"

def extract_tweets():
    scrape_time = datetime.now().isoformat()
    name_file = f"{scrape_time}_CPI.csv"
    scrape_obj = scrapeTweets("CPI")
    scrape_obj.save_csv(name_file)
    return name_file

def clean_data(ti):
    # Get  the first element of the list
    scrape_name = ti.xcom_pull(task_ids=[
        "extract_from_twitter"
    ])[0]
    
    keep_columns = ["date", "timezone", "tweet", "language"]
    
    c_df = clean_df(scrape_name, keep_columns)
    
    clean_name = scrape_name[:-4] + "_clean" + ".csv"
    c_df.to_csv(clean_name, index=False)

    return clean_name

def _create_table():

    con = psycopg2.connect(database="postgres",
                        user="airflow",
                        password="airflow",
                        host="postgres",
                        port="5432")


    
    ok = create_table(con, TABLE_NAME)
    con.close()

    if not ok:
        raise ValueError("Not able to create the table")


def add_scrape_data(ti):
    
    clean_name = ti.xcom_pull(task_ids=[
        "clean_data"
    ])[0]

    c_df = pd.read_csv(clean_name)

    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/postgres')

    ok = add_df_to_sql(c_df, engine, TABLE_NAME)

    if not ok:
        raise ValueError("Not able to add data to the database")


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

        create = PythonOperator(
            task_id="create_table",
            python_callable=_create_table,
        )

        add_data = PythonOperator(
            task_id="add_data",
            python_callable=add_scrape_data,
        )

        extract >> clean >> create >> add_data