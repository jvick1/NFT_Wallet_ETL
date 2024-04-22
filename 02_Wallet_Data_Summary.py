#data
import pandas as pd
import numpy as np

path = 'D:/Python Work Area/02_NFT/Wallet_ETL/'

#read in data
FF_Wallets = pd.read_csv(path + "Data/00_wallets.csv",  encoding='utf-8')
Failed_Wallets = pd.read_csv(path + "Data/01_wallets.csv", encoding='utf-8')

#append df 
Wallets = pd.concat([FF_Wallets,Failed_Wallets], ignore_index=True)

#agg
Wallets = Wallets.pivot_table(columns=["addy"], aggfunc=np.sum).transpose() 
Wallets.to_csv(path + "Data/02_final_wallets.csv", encoding='utf-8')

print(f"=== Step 1 === \nSaved: {path}/Data/")

#read in data
FF_Tokens = pd.read_csv(path + "Data/00_token.csv",  encoding='utf-8')
Failed_Tokens = pd.read_csv(path + "Data/01_token.csv", encoding='utf-8')

#append df 
Tokens = pd.concat([FF_Tokens,Failed_Tokens], ignore_index=True)

#export
Tokens.to_csv(path + "Data/02_final_token.csv", encoding='utf-8')

print(f"=== Step 2 ===\nSaved: {path}/Data/")