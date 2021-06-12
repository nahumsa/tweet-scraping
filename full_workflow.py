from etl.scrape import scrapeTweets
from etl.process_df import clean_df


scrape_obj = scrapeTweets("sa_nahum", 10)
scrape_obj.save_csv("n.csv")

keep_columns = ["id", "date", "timezone", "tweet", "language"]
print(clean_df("nahum.csv", keep_columns))