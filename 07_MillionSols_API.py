#millionSol API

import requests
import json
from bs4 import BeautifulSoup as bs
import pandas as pd

import time
from datetime import date


"""UPDATE PATH + CREATE LOGS FOLDER BEFORE RUNNING"""
path = 'D:/Python Work Area/02_NFT/Wallet_ETL/'
startTime = time.time()
today = date.today()

#millionSOL URL
url = "http://millionsols.art/collections.html?c="
Cyber_Frogs_URL = "cyberfrogs&id="

#nft dataframe
#add wallet addy & token addy to dataframe <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#Cyber_Frog_DF = pd.DataFrame(columns= ['Frog Number','Current_Rank','Image_URL','Last_Price','Attributes', 'Background', 'Skin', 'Eyes', 'Outfit', 'Item', 'Headware', 'Mouth', 'Blend', 'Faction', 'KIRA'])
FF_tokens = pd.read_csv(path + "Data/06_token.csv",  encoding='utf-8', index_col=0)

#Staking rewards
rarity_result = 0

def frog_rarity(Rank):
    if (Rank <= 1000):
        rarity_result = 1
    else:
        rarity_result = 0
    return rarity_result

count = 0

#loop over wallets 
for index, row in FF_tokens.iterrows():

    FrogNumber = row["frog_num"]
    FinalURL = url + Cyber_Frogs_URL + FrogNumber #pulling the str after break
    #web scrape
    web_data = requests.get(FinalURL)

    #pull all attribute data
    soup =bs(web_data.content, "html.parser")

    #rank
    Current_Rank = soup.find("h3", attrs={"style":"margin-bottom:0px !important;"})
    Current_Rank = Current_Rank.text.split(" ")[1]

    #pull price data
    Price_Table = soup.find("table", attrs={"class":"table table-striped table-dark"})
    Price_Table = Price_Table.find_all('td')
    Last_Price = Price_Table[2].text.split("SOL")[0]

    #pull date
    Date_Table = soup.find("table", attrs={"class":"table table-striped table-dark"})
    Date_Table = Date_Table.find_all('td')
    Last_Date = Date_Table[1].text.split("Mint:")[0]

    #parse attributes
    for a in soup.find_all("div", attrs={"class":"col-12 col-md-7 mt-4 mt-sm-0"}):
        if "Attributes: " in a.text: #attributes could be 6 or 7 so just search attribute
            Data_List = [FrogNumber, Current_Rank, Last_Date, Last_Price]

            #paragraph of text (no structure)
            text_data = a.get_text(separator="\n") 

            #this prints a list of each trait
            elements = text_data.split("\n")
            element_len = len(elements)

            for trait in elements:

                trait_attribute = trait.split(":")[0].strip()
                trait_name = trait.split(":")[1].strip()

                #Need to fill Blend (Alpha or Beta or OG)
                if trait_attribute == 'Alpha':
                    trait_name = trait_name.replace("Yes", "Alpha")
                    Data_List.append(trait_name)
                elif trait_attribute == 'Beta':
                    trait_name = trait_name.replace("Yes", "Beta")
                    Data_List.append(trait_name)  
                else: 
                    Data_List.append(trait_name)

            #OG FROGS
            if element_len == 8:
                Data_List.append("OG")
                Data_List.append("N/A")
            
            #check last element if faction we need to insert OG to fill 'blend'
            if element_len == 9:
                if trait_attribute[-1] == "Faction":
                    Data_List.insert(8,"OG") #would be cool to add in % taking number from alpha and beta - 100%
                else:
                    Data_List.append("N/A")

            #check rarity & if alpha to calc KIRA while staked
            if (Data_List[-2] == "OG" or Data_List[-1] == "OG"):
                KIRA = 10 + frog_rarity(int(Current_Rank))
            elif (Data_List[-2].startswith('Beta') or Data_List[-1].startswith('Beta')):
                KIRA = 13 + frog_rarity(int(Current_Rank))
            else:
                KIRA = 21 + frog_rarity(int(Current_Rank))
            Data_List.append(KIRA)

        #append frog to dataframe
        #can i add this list to the current row???
        #You can't mutate the df using row here to add a new column
        # you'd either refer to the original df or use .loc, .iloc, or .ix, example:
        columns_name =['Frog Number','Current_Rank',"Last_Sold",'Last_Price','Attributes', 'Background', 'Skin', 'Eyes', 'Outfit', 'Item', 'Headware', 'Mouth', 'Blend', 'Faction', 'KIRA']
        #Cyber_Frog_DF.loc[len(Cyber_Frog_DF)] = Data_List 
        FF_tokens.loc[index, columns_name] = Data_List
        #FF_tokens.append(df)
        count += 1
        print(count,"- Data List:", Data_List)
    #end attributes


#fix column fuck up 
print("="*4,"Starting on column clean up","="*4)
counter = 0
for row in FF_tokens['Blend']:
    if (row.startswith('Alpha') or row.startswith('Beta') or row.startswith('OG')):
        pass
    else:
        temp1 = FF_tokens.loc[counter,"Faction"] #Cyber_Frog_DF.loc[counter, "Faction"]
        temp2 = FF_tokens.loc[counter,"Blend"] #Cyber_Frog_DF.loc[counter, "Blend"]

        #print(temp1)
        #print(temp2)

        FF_tokens.loc[counter,"Faction"]  = temp2
        FF_tokens.loc[counter,"Blend"] = temp1

    counter +=1

#to csv
FF_tokens.to_csv(path + "07_CyberFrog_Metadata_Faction.csv")

executionTime = (time.time() - startTime)
log = open(path + "logs/07_log.txt", "a")
log.write(f"Last Ran: {today} | Execution Time: {str(executionTime)} seconds | Notes: ")