#web stuff
import requests
from bs4 import BeautifulSoup as bs
from selenium import webdriver

#data
import pandas as pd
import numpy.random as random 

#time
from datetime import date
import time

path = 'D:/Python Work Area/02_NFT/Wallet_ETL/'
startTime = time.time()
today = date.today()

addresses = []
failed_addy = []
token = []
count = 0
fails = 0
driver = webdriver.Chrome()

with open(path + "Data/00_Failed_Frogs.txt", 'r') as addrs:
    for line in addrs:
        try:
            line = line.strip()

            #line = "12c8HGvixKPRD9wPHDKKVuKug6szpQv52NSJf6M5DT5S"
            driver.get("https://solscan.io/token/"+ line +"#holders")
            time.sleep(5)

            html = driver.page_source

            soup  = bs(html,  "html.parser")

            messy = []
            for a in soup.find_all('a', href=True):
                if "account" in a['href']:
                    messy.append(str(a['href']))
            addy = messy[-1].replace("/account/","")
            addresses.append(addy)
            token.append(line)

            count +=1
            print(count, "-", line, "-", addy)

        except:
            #time.sleep(1)
            fails += 1
            failed_addy.append(line)
            print("FAIL -", line)
            pass

#print(addresses)

with open(path + "Data/01_Failed_Frogs.txt",'w') as f:
    for item in failed_addy:
        f.write("%s\n" % item)
    print(f"Failed frogs {fails} printed to text file.")

#count of duplicates for the addy column
df = pd.DataFrame(addresses, columns = ["addy"])
wallets = df.pivot_table(columns=["addy"], aggfunc='size')
print(wallets)

#data
df["token"] = token
df.to_csv(path + "Data/01_token.csv", encoding='utf-8')
wallets.to_csv(path + "Data/01_wallets.csv", encoding='utf-8')

#logs
executionTime = (time.time() - startTime)
log = open(path+"logs/01_log.txt", "a")
log.write(f"Last Ran: {today} | Execution Time: {str(executionTime)} seconds | Count: {count} | Fails: {fails} \n")