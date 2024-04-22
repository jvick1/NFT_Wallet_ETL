#web stuff
import requests

#data
import pandas as pd

#time
from datetime import date
import time
import datetime
import calendar 

"""UPDATE PATH + CREATE LOGS FOLDER BEFORE RUNNING"""

path = 'D:/Python Work Area/02_NFT/Wallet_ETL/'
startTime = time.time()
today = date.today()
txURL = "https://public-api.solscan.io/account/transactions?account=" #this pulls Transactions, Sol Transfers, etc. But NOT span NFTs 


FF_Wallets = pd.read_csv(path + "Data/03_wallets_with_tx.csv",  encoding='utf-8', index_col=False)

results = []
count = 0
fails = 0
for index, row in FF_Wallets.iterrows():
    if "Fail" == row["Last_Tx_blocktime"]:
        #for addressBitch in addresses['address']:
        time.sleep(0.5)
        
        try:
            addr = row["Wallet_Address"]
            yap = txURL + addr
            txRes = requests.get(yap).json()
            count += 1
            print(f"{count} URL:", yap)
            txRes = requests.get(yap).json()
            txRes = txRes[0]['blockTime']
            results.append(txRes)
            #row["Last_Tx_blocktime"] = txRes
            FF_Wallets._set_value(index, "Last_Tx_blocktime", txRes)
            
        except:
            results.append("Fail")
            fails += 1
            pass

#print(results)
print(f"Attention... we had {fails} wallet(s) fail this section.")

FF_Wallets.insert(4,"test", 0)

results = []
for index, row in FF_Wallets.iterrows():
    if "Fail" == row["Last_Tx_blocktime"]:
        results.append("Fail")
    else:
        test = int(row["Last_Tx_blocktime"])
        test = datetime.datetime.fromtimestamp(test)
        results.append(test)

FF_Wallets["test"] = results
FF_Wallets.to_csv(path + "Data/04_wallets_with_tx.csv", encoding='utf-8')

#maybe save csv here.

threshold = datetime.datetime(2022,1, 1, 0, 0, 0)
t = calendar.timegm(threshold.timetuple())

deadWallets = [] 
counter = 0 
for addressSlut in FF_Wallets['Last_Tx_blocktime']:
    if addressSlut == "Fail":
        print("Tx",addressSlut,"| Cutoff", int(t))
        deadWallets.append(FF_Wallets['Wallet_Address'][counter])
    elif int(addressSlut) < int(t):
        print("Tx",datetime.datetime.fromtimestamp(int(addressSlut)),"| Cutoff", int(t))
        deadWallets.append(FF_Wallets['Wallet_Address'][counter])
    counter +=1
print("Dead Wallets:", deadWallets)

with open( path + 'Data/04_dead.txt', 'w') as fp:
    for item in deadWallets:
        # write each item on a new line
        fp.write("%s\n" % item)
    print('Done')

#other projects being held too
#https://magiceden.io/u/
#https://hyperspace.xyz/account/


executionTime = (time.time() - startTime)
log = open(path+"logs/04_log.txt", "a")
log.write(f"Last Ran: {today} | Execution Time: {str(executionTime)} seconds | Failed: {fails} | Dead: {deadWallets}\n")