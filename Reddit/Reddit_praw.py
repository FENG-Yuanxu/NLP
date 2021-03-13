# -*- coding: utf-8 -*-

# https://www.storybench.org/how-to-scrape-reddit-with-python/
# https://asyncpraw.readthedocs.io/en/latest/getting_started/quick_start.html

import asyncpraw
import pandas as pd
import datetime as dt

reddit = asyncpraw.Reddit(
    client_id="I45HWugJC0iJ7g",
    client_secret="FBG1ftO9kpYdUrTz7KaxUWhvDpXNPQ",
    password="nlp20210306",
    user_agent="NLP_Bitcoin",
    username="yuanxufeng")

topics_dict = { "title":[], \
                "score":[], \
                "id":[], "url":[], \
                "comms_num": [], \
                "created": [], \
                "body":[]}

urls = [
        'r/Bitcoin/',
        'r/BitcoinBeginners/',
        'r/BitcoinPrivate/',
        'r/CryptoMarkets/',
        'r/BitcoinMarkets/',
        'r/BitcoinMining/',
        'r/bitcoinxt/',
        'r/btc/',
        'r/bitcoin_uncensored/',
        'r/BitcoinUK/',
        'r/CryptoCurrencies/',
        'r/BitcoinAirdrops/',
        'r/Bitcoincash/',
        'r/CryptoCurrency/',
        'r/BitcoinCA/',
        'r/bitcoincashSV/',
        'r/Crypto_Currency_News/',
        'r/BitcoinAUS/',
        'r/CryptoCurrencyTrading/',
        'r/BitcoinSerious/']

for url in urls:
    url = url[2:-1]
    subreddit = await reddit.subreddit(url)
    top_subreddit = subreddit.top(limit=2000) # maximum 1000
    
    async for submission in top_subreddit:
        topics_dict["title"].append(submission.title)
        topics_dict["score"].append(submission.score)
        topics_dict["id"].append(submission.id)
        topics_dict["url"].append(submission.url)
        topics_dict["comms_num"].append(submission.num_comments)
        topics_dict["created"].append(submission.created)
        topics_dict["body"].append(submission.selftext)

topics_data = pd.DataFrame(topics_dict)

def get_date(created):
    return dt.datetime.fromtimestamp(created)
_timestamp = topics_data["created"].apply(get_date)
topics_data = topics_data.assign(timestamp = _timestamp)

topics_data.to_csv('reddit_bitcoin.csv', index=False) 





# Data Preprocessing
import pandas as pd
import re

topics_data = pd.read_csv('reddit_bitcoin.csv') 
topics_data['date'] = topics_data['timestamp'].str[0:10]

df = topics_data[['date', 'id', 'title']]

total = df.drop_duplicates().sort_values(['date','id']).set_index(['date'])
total.to_csv('total_reddits.csv')

clean = total["title"].str.lower()
clean = clean.apply(lambda x :re.sub('@[a-z]*','',x))      # Remove tags
clean = clean.apply(lambda x :re.sub('#[a-z0-9]*','',x))   # Remove hash tags
clean = clean.apply(lambda x :re.sub('[0-9]+[a-z]*',' ',x)) # Remove numnbers and associated text. Like : 1st, 2nd, nth....
clean = clean.apply(lambda x :re.sub('\n','',x))            # Remove \n\t
clean = clean.apply(lambda x :re.sub('https?:\/\/.*',' ',x))        # Remove URLs
clean = clean.apply(lambda x :re.sub('[:;!-.,()%/?|]',' ',x))       # Remove Special characters
clean = clean.apply(lambda x :re.sub('$[a-z]*',' ',x))                        # Remove tickers and strings have $abc pattern
clean = clean.apply(lambda x : x.encode('ascii', 'ignore').decode('ascii'))   # Remove emojis
clean = clean.apply(lambda x :re.sub('[0-9]{4}-[0-9]{2}-[0-9]{2}','',x))      # Remove date
clean = clean.apply(lambda x :re.sub('[0-9]*','',x))

total["title"] = clean
total.to_csv('cleaned_reddits.csv')




# Merge 'cleaned_reddits.csv' with 'cleaned_tweets.csv'
import pandas as pd

cleaned_reddits = pd.read_csv('cleaned_reddits_allyears.csv')
cleaned_tweets = pd.read_csv('cleaned_tweets.csv')

cleaned_reddits.date = pd.to_datetime(cleaned_reddits.date)
cleaned_reddits.columns = ['date','user_or_id','text']

cleaned_tweets.date = pd.to_datetime(cleaned_tweets.dt)
del cleaned_tweets['dt']
cleaned_tweets.columns = ['date','user_or_id','text']

cleaned_reddits = cleaned_reddits[cleaned_reddits.date >= pd.to_datetime("2020-09-01")]
cleaned_reddits = cleaned_reddits[cleaned_reddits.date <= pd.to_datetime("2021-03-01")]

cleaned_20200901_20210101_tweets_reddits = pd.concat(
    [cleaned_tweets, cleaned_reddits]).sort_values(['date']).set_index(['date'])

cleaned_20200901_20210101_tweets_reddits.to_csv('cleaned_20200901_20210301_tweets_reddits.csv')
