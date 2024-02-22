from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
import pandas as pd

def reddit_pipeline(file_name, subreddit, time_filter='day', limit=None):
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscolar Agent')
    posts = extract_posts(instance, subreddit, time_filter, limit)

    posts_df = pd.DataFrame(posts)
    posts_df = transform_data(posts_df)

    file_name = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(posts_df, file_name)
    return file_name


