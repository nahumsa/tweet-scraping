import psycopg2

from datetime import datetime
from dags.etl.scrape import scrapeTweets
from dags.etl.process_df import clean_df
from dags.etl.database import create_table, add_df_to_sql
from sqlalchemy import create_engine

scrape_time = datetime.now().isoformat()
scrape_obj = scrapeTweets("CPI")
scrape_obj.save_csv(f"{scrape_time}_CPI.csv")

keep_columns = ["date", "timezone", "tweet", "language"]
c_df = clean_df(f"{scrape_time}_CPI.csv", keep_columns) 

# DATABASE OPERATIONS

con = psycopg2.connect(database="tweets",
                       user="postgres",
                       password="7894561230",
                       host="127.0.0.1",
                       port="5432")


table_name = "tweetbody"

ok = create_table(con, table_name)

if not ok:
    print("Not able to create the table")


# Add data to the table
engine = create_engine('postgresql://postgres:7894561230@localhost:5432/tweets')

ok = add_df_to_sql(c_df, engine, table_name)

if not ok:
    print("Not able to add data to the database")

con.close()