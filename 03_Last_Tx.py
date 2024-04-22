#web stuff
import requests

#data
import pandas as pd

#time
from datetime import date
import time

path = 'D:/Python Work Area/02_NFT/Wallet_ETL/'
startTime = time.time()
today = date.today()
txURL = "https://public-api.solscan.io/account/transactions?account=" #this pulls Transactions, Sol Transfers, etc. But NOT span NFTs 

#read in data
FF_Wallets = pd.read_csv(path + "Data/02_final_wallets.csv",  encoding='utf-8').rename(columns={"addy": "Wallet_Address", "0": "Count_of_Frogs"})

#print(FF_Wallets)

results = []
count = 0
fails = 0
for index, row in FF_Wallets.iterrows():

    #for addressBitch in addresses['address']:
    addr = row["Wallet_Address"]
    yap = txURL + addr

    try: 
        txRes = requests.get(yap).json()
        print(f"{count} URL:", yap)
        txRes = requests.get(yap).json()
        txRes = txRes[0]['blockTime']
        results.append(txRes)
        count += 1
    except:
        results.append("Fail")
        fails += 1
        pass

#print(results)
print(f">> Checking {fails} failed...")

FF_Wallets["Last_Tx_blocktime"] = results

FF_Wallets.to_csv(path + "Data/03_wallets_with_tx.csv", encoding='utf-8')

#logs
executionTime = (time.time() - startTime)
log = open(path+"logs/03_log.txt", "a")
log.write(f"Last Ran: {today} | Execution Time: {str(executionTime)} seconds | Count: {count} | Fails: {fails} \n")