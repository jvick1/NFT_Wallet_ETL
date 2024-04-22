import requests
import pandas as pd
import time
from datetime import date


"""UPDATE PATH"""

#api key
#
#Default Limit: 150 requests/ 30 seconds, 100k requests / day

path = 'D:/Python Work Area/02_NFT/Wallet_ETL/'
startTime = time.time()
today = date.today()
url = "https://public-api.solscan.io/token/holders?tokenAddress="

addresses = []
failed_addy = []
token = []
fails = 0
count = 0
supply = 7801
#read in text file
#using token address find all wallets
with open(path + "frogs.txt", 'r') as addrs:
    #lines = addrs.readlines()
    
    for line in addrs:

        time.sleep(0.5)

        try:
            line = line.strip()
            bap = url + str(line)
            #print("Bap:",bap)
            res = requests.get(bap)
            
            #print(res.status_code)

            if res.status_code == 429:
                print("oh shit...", res.status_code)
                time.sleep(60)
                res = requests.get(bap)

            res = res.json()
            #print("Res:",res)
            need = res['data'][0]['owner']
            addresses.append(need)
            token.append(line)

            count +=1
            print(count, "/", supply, "-", line)

        except:
            fails += 1
            print("Fail:", line)
            failed_addy.append(line)
            #time.sleep(1)
            pass
#print(addresses)

with open(path + "Data/00_Failed_Frogs.txt",'w') as f:
    for item in failed_addy:
        f.write("%s\n" % item)
    print(f"Failed frogs {fails} printed to text file.")

#count of duplicates for the addy column
df = pd.DataFrame(addresses, columns = ["addy"])
wallets = df.pivot_table(columns=["addy"], aggfunc='size')
print(wallets)

#data
df["token"] = token
df.to_csv(path + "Data/00_token.csv", encoding='utf-8')
wallets.to_csv(path + "Data/00_wallets.csv", encoding='utf-8')

#logs
executionTime = (time.time() - startTime)
log = open(path+"logs/00_log.txt", "a")
log.write(f"Last Ran: {today} | Execution Time: {str(executionTime)} seconds | Count: {count}/{supply} | Fails: {fails} \n")
