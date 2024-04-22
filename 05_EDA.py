#time
from datetime import date, timedelta
import time
import datetime

import calendar 
import pandas as pd 
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


"""UPDATE PATH + CREATE LOGS FOLDER BEFORE RUNNING"""

path = 'D:/Python Work Area/02_NFT/Wallet_ETL/'
startTime = time.time()
today = date.today()

recently = today - timedelta(weeks=1)
mth = today - timedelta(weeks=4)
quarter = today - timedelta(weeks=12)
half_yr = today - timedelta(weeks=26)
yr = today - timedelta(weeks=52)

#read in DF
FF_Wallets = pd.read_csv(path + "Data/04_wallets_with_tx.csv",  encoding='utf-8', index_col=0)

#print some summary data
print("====EDA====\n",">Head:\n",FF_Wallets.head(),"\n>Shape:",FF_Wallets.shape,"\n>Describe:\n", FF_Wallets.describe())

summary = FF_Wallets.describe()

#count all frogs should be around 7798
FF_Wallets["Count_of_Frogs"].sum()

#date bins
FF_Wallets["test"] = pd.to_datetime(FF_Wallets["test"])

date_bins = []

for index, row in FF_Wallets.iterrows():
    if  recently <= row["test"].date() <= today:
        date_bins.append("recently")
    elif mth <= row["test"].date() <= recently:
        date_bins.append("mth")
    elif  quarter <= row["test"].date() <= mth:
        date_bins.append("quarter")
    elif half_yr <= row["test"].date() <= quarter:
        date_bins.append("half_yr")
    elif yr <= row["test"].date() <= half_yr:
        date_bins.append("yr")
    else:
        date_bins.append("yr+")

FF_Wallets["date_bins"] = date_bins

dt_bin = sns.stripplot(x = "date_bins", y = "Count_of_Frogs", order=["recently","mth","quarter","half_yr","yr","yr+"], data=FF_Wallets).get_figure()
plt.show()
dt_bin.savefig(path + f"chart/05_date_plot_{today}.png")


#FF_Wallets.value_counts(subset=['Count_of_Frogs'], sort=False) #shows counts for all

#sns.scatterplot(x="TimeSeries", y="Count_of_Frogs", data=FF_Wallets) #can't read the x axis 
#plt.show

#sns.boxplot(FF_Wallets[["Count_of_Frogs"]]) #Not super helpful bc data is so skewed
#plt.show()

#sns.distplot(FF_Wallets["Count_of_Frogs"], hist=True, kde=False, rug=False) #again skew - might be better to plot bins
#plt.show()

range = [0, 1, 2, 5, 10, 24, 50, 2000]
group_names = ["1","2","3-4","5-9","10-23","24-49","50+"]

data = FF_Wallets['Count_of_Frogs'].value_counts(bins=range)
data = data.to_frame("Count")
data = data.transpose()
data = data.set_axis(group_names, axis=1)
data = data.transpose()
type(data)

bins = sns.barplot(x=data.index, y=data["Count"]).get_figure() #again skew - might be better to plot bins
plt.show()
bins.savefig(path + f"chart/05_bin_plot_{today}.png")

#save df
FF_Wallets.to_csv(path + "05_wallets.csv", encoding='utf-8')

executionTime = (time.time() - startTime)
log = open(path + "logs/05_log.txt", "a")
log.write(f"Last Ran: {today} | Execution Time: {str(executionTime)} seconds | Summary: \n{summary} ")