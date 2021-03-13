# -*- coding: utf-8 -*-

# https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c

import requests
import pandas as pd
from datetime import datetime

# we use this function to convert responses to dataframes
def df_from_response(res):
    # initialize temp dataframe for batch of data in response
    df = pd.DataFrame()
    # loop through each post pulled from res and append to df
    for post in res.json()['data']['children']:
        df = df.append({
            'subreddit': post['data']['subreddit'],
            'title': post['data']['title'],
            'selftext': post['data']['selftext'],
            'upvote_ratio': post['data']['upvote_ratio'],
            'ups': post['data']['ups'],
            'downs': post['data']['downs'],
            'score': post['data']['score'],
            'link_flair_css_class': post['data']['link_flair_css_class'],
            'created_utc': datetime.fromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'id': post['data']['id'],
            'kind': post['kind']
        }, ignore_index=True)
    return df

# authenticate API
client_auth = requests.auth.HTTPBasicAuth('I45HWugJC0iJ7g', 'FBG1ftO9kpYdUrTz7KaxUWhvDpXNPQ')
data = {'grant_type': 'password',
        'username': 'yuanxufeng',
        'password': 'nlp20210306'}
headers = {'User-Agent': 'NLP_Bitcoin/0.0.1'}

# send authentication request for OAuth token
res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=client_auth, data=data, headers=headers)
# extract token from response and format correctly
TOKEN = f"bearer {res.json()['access_token']}"
# update API headers with authorization (bearer token)
headers = {**headers, **{'Authorization': TOKEN}}

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

# initialize dataframe
data = pd.DataFrame()

for url in urls:
    # initialize parameters for pulling data in loop
    params = {'limit': 100}  
    # loop through n times, returning 100*n posts
    for i in range(100):
        # make request
        res = requests.get("https://oauth.reddit.com/" + url,
                           headers=headers,
                           params=params)
        # get dataframe from response
        new_df = df_from_response(res)
        # take the final row (oldest entry)
        if len(new_df) == 0:
            print(url + " stop scraping at i= " + str(i) + ", no more data!!!")
            break
        row = new_df.iloc[len(new_df)-1]
        # create fullname
        fullname = row['kind'] + '_' + row['id']
        # add/update fullname in params
        params['after'] = fullname
        # append new_df to data
        data = data.append(new_df, ignore_index=True)

data.to_csv("reddit_bitcoin.csv")
