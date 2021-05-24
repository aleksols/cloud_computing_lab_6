import socketserver
import pandas as pd
import time
import glob
import os
from tqdm import tqdm

DF = pd.read_csv("financial_tweets/tweets.csv")
NUM_ITERATIONS = 1

class MyHandler(socketserver.BaseRequestHandler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.df = pd.read_csv("financial_tweets/tweets.csv")
        self.num_iterations = 1

    def handle(self):
        num_iterations = NUM_ITERATIONS
        while num_iterations != 0:
            row_index = 0
            while row_index < len(DF):
                now = int(time.time())
                send_str = ""
                while int(time.time()) == now:
                    row_index %= len(DF)
                    tweet = DF.iloc[row_index].tweet_text
                    if not isinstance(tweet, str):
                        row_index += 1
                        continue
                    send_str += tweet.replace("\n", " ") + "\n"
                    row_index += 1
                self.request.send(send_str.encode("utf-8"))

            
            for i, row in DF.iterrows():
                tweet = row.tweet_text
                if not isinstance(tweet, str):
                    continue
                self.request.send(tweet.replace("\n", " ").encode("utf-8"))
      
            num_iterations -= 1

            

myServer = socketserver.TCPServer(('localhost',9999), MyHandler)
myServer.serve_forever()