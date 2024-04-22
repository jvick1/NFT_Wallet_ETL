#solscan API

import requests
import pandas as pd

import time
from datetime import date

"""UPDATE PATH + CREATE LOGS FOLDER BEFORE RUNNING"""

path = 'D:/Python Work Area/02_NFT/Wallet_ETL/'
startTime = time.time()
today = date.today()

#take multiple wallets 
FF_tokens = pd.read_csv(path + "Data/02_final_token.csv",  encoding='utf-8', index_col=0)

#SolscanAPI
solscan_url = 'https://public-api.solscan.io/account/'

results_num = []
results_uri = []
count = 0
fails = 0

for index, row in FF_tokens.iterrows():
    
    count += 1
    token = row['token'] #"12c8HGvixKPRD9wPHDKKVuKug6szpQv52NSJf6M5DT5S" 
    Token_api = solscan_url + token
    response = requests.get(Token_api)
    
    try:
        json_response = response.json()
        FrogNumber = json_response['metadata']['data']['name'].split('#')[1]
        ImageURL = json_response['metadata']['data']['image']
        results_num.append(FrogNumber)
        results_uri.append(ImageURL)
        print(f"{count} - Frog #{FrogNumber} ({token})")
    except:
        fails =+ 1
        results_num.append("Fail")
        results_uri.append("Fail")
        print(f"{count} - {token}")
        pass
                                 
FF_tokens["frog_num"] = results_num
FF_tokens["image"] = results_uri

FF_tokens.to_csv(path + "Data/06_token.csv", encoding='utf-8')

executionTime = (time.time() - startTime)
log = open(path + "logs/06_log.txt", "a")
log.write(f"Last Ran: {today} | Execution Time: {str(executionTime)} seconds | Fails: {fails} | Notes: ")

