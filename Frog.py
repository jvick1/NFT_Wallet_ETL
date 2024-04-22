import requests
import json
from bs4 import BeautifulSoup as bs
import pandas as pd
import datetime
import calendar 

"""UPDATE PATH"""

path = 'D:/Python Work Area/02_NFT/Dead_Wallet/'
time = datetime.datetime(2022,1, 1, 0, 0, 0)
t = calendar.timegm(time.timetuple())
url = "https://public-api.solscan.io/token/holders?tokenAddress="

addresses = {'address':[], 'time':[]}


addrs = open(path + "frogs.txt", 'r')
lines = addrs.readlines()
count =0
for line in lines:
    if count > 10:
       break
    try:
        bap = url + str(line.strip())
        #print("Bap:",bap)
        res = requests.get(bap).json()
        #print("Res:",res)
        need = res['data'][0]['owner']
        addresses["address"].append(need)
        count +=1
    except:
        pass
print(addresses['address'])

#we may want to turn the top part into a schedualed ETL process
#then we could remove duplicate wallet address
#and maybe even a count of frogs if we are feeling fancy


#make as a step 2 in bat file
#pull from wallet cvs made above
#get last tx by date
#count dead wallets

txURL = "https://public-api.solscan.io/account/transactions?account="

#print(txRes[0]['blockTime'])
#03 read from final wallet csv

for addressBitch in addresses['address']:
    addr = addressBitch
    yap = txURL + addr

    txRes = requests.get(yap).json()
    try: 
        print("URL:", yap)
        txRes = requests.get(yap).json()
        txRes = txRes[0]['blockTime']
        addresses["time"].append(txRes)
    except:
        pass

print(addresses)

#print(datetime.datetime.fromtimestamp(addresses['time'][0]))

deadWallets = [] 
counter = 0 
for addressSlut in addresses['time']:
    if int(addressSlut) < int(t):
        deadWallets.append(addresses['address'][counter])
    counter +=1
print(deadWallets)