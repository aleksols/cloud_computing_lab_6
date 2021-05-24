import pandas as pd
import time
import glob
import os
from tqdm import tqdm

df = pd.read_csv("financial_tweets/tweets.csv")


# Start without any tweets
files = glob.glob("tweets/*")
for f in files:
    os.remove(f)

length = len(df)
for i, row in tqdm(df.iterrows(), total=length):
    tweet = row.tweet_text
    if not isinstance(tweet, str):
        continue
    with open(f"tweets/{int(time.time())}.txt", "a") as file:
        
        file.write(tweet.replace("\n", " ") + "\n")
        

