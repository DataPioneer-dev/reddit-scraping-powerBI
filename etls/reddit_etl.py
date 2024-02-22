from praw import Reddit
import sys
import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
from utils.constants import POST_FIELDS


def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
        print("Connected to reddit !")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)

def extract_posts(reddit_instance: Reddit, subreddit, time_filter, limit=None):
    posts_list = []
    retrieved_count = 0
    subreddit = reddit_instance.subreddit(subreddit)
    while retrieved_count < limit:
        posts = subreddit.top(time_filter=time_filter, limit=100)

        for post in posts:
            post_dict = vars(post)
            post = {key: post_dict[key] for key in POST_FIELDS}
            posts_list.append(post)

        retrieved_count = len(posts_list)

    return posts_list


def est_url(cellule):
    return bool(re.match(r"(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?", cellule))

def clean_html(text):
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text().strip()


def transform_data(posts_df):
    posts_df['created_utc'] = pd.to_datetime(posts_df['created_utc'], unit='s')
    posts_df['over_18'] = np.where((posts_df['over_18'] == True), True, True)
    posts_df['author'] = posts_df['author'].astype(str)
    posts_df['edited'] = np.where(posts_df['edited'].isin([True, False]), posts_df['edited'], True).astype(bool)
    posts_df['num_comments'] = posts_df['num_comments'].astype(int)
    posts_df['score'] = posts_df['score'].astype(int)
    posts_df['upvote_ratio'] = posts_df['upvote_ratio'].astype(float)
    posts_df['title'] = posts_df['title'].astype(str)
    posts_df['title'] = posts_df['title'].apply(lambda x: re.sub(r'[^\w\s]', '', x))
    posts_df['title'] = posts_df['title'].apply(lambda x: re.sub(r'\s+', ' ', x))
    posts_df['title'] = posts_df['title'].str.replace(r'[\n\t]', ' ', regex=True)
    posts_df['selftext'] = posts_df['selftext'].astype(str)
    posts_df = posts_df.query('selftext != ""')
    posts_df = posts_df[~posts_df["selftext"].apply(est_url)]
    posts_df['sleftext'] = posts_df['selftext'].apply(clean_html)
    posts_df['selftext'] = posts_df['selftext'].apply(lambda x: re.sub(r'[^\w\s]', '', x))
    posts_df['selftext'] = posts_df['selftext'].apply(lambda x: re.sub(r'\s+', ' ', x))
    posts_df['selftext'] = posts_df['selftext'].str.replace(r'[\n\t]', ' ', regex=True)
    return posts_df

def load_data_to_csv(data, path):
    data.to_csv(path, index=False)



