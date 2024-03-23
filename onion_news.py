from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import csv
import pandas as pd
from currency_converter import ECB_URL, CurrencyConverter
from datetime import date, datetime, timedelta
import numpy as np
import os

INPUT_URL = os.environ.get("INPUT_URL")
OUTPUT_URL = os.environ.get("OUTPUT_URL")

def connect_db(url,db,collection):
  uri = url
  # Create a new client and connect to the server
  client = MongoClient(uri, server_api=ServerApi('1'))
  client.admin.command('ping')
  db = client[db]
  table = db[collection]
  #table=client.india.onion_news_raw
  return table

url=INPUT_URL
table=connect_db(url,"india","onion_news_raw")

data_list=table.find()
df=pd.DataFrame(data_list)
date_format = "%d.%m.%Y"
df['Date'] = pd.to_datetime(df['Date'], format=date_format)
df=df.sort_values(by='Date')
df['MEP'] = df['MEP'].fillna(method='ffill')
# Remove duplicate dates
df = df.drop_duplicates(subset='Date')
# Reindex the DataFrame to include all dates within the date range
date_range = pd.date_range(start=df['Date'].min(), end=pd.Timestamp.today())
df = df.set_index('Date').reindex(date_range).reset_index()
# Iterate through each row to update MEP value to the next date until a new MEP value is found
for i in range(len(df) - 1):
    current_mep = df.loc[i, 'MEP']
    next_mep = df.loc[i + 1, 'MEP']
    if pd.isnull(next_mep):
        df.loc[i + 1, 'MEP'] = current_mep
# Iterate through each row to update MEP unit to the next date until a new MEP unit is found
for i in range(len(df) - 1):
    current_mep_unit = df.loc[i, 'MEP_Unit']
    next_mep_unit = df.loc[i + 1, 'MEP_Unit']
    if pd.isnull(next_mep_unit):
        df.loc[i + 1, 'MEP_Unit'] = current_mep_unit
df = df.rename(columns={'index': 'Date'})
df['Flag_Start_Stop'] = df['Flag'].apply(lambda x: x if x in ['Start', 'Stop'] else None)
# Iterate through each row to update MEP value to the next date until a new MEP value is found
for i in range(len(df) - 1):
    current_flag = df.loc[i, 'Flag_Start_Stop']
    next_flag = df.loc[i + 1, 'Flag_Start_Stop']
    if pd.isnull(next_flag):
        df.loc[i + 1, 'Flag_Start_Stop'] = current_flag
df['MEP_Updated'] = df.apply(lambda x: x['MEP'] if x['Flag_Start_Stop'] == 'Start' else None, axis=1)
c = CurrencyConverter()
# Start date
start_date = datetime(2000, 1, 1)
# Today's date
end_date = datetime.today()
# List to store dates
date_list = []
# Generate dates
current_date = start_date
while current_date <= end_date:
    date_list.append(current_date)
    current_date += timedelta(days=1)
# Create DataFrame
exchange_df = pd.DataFrame({'Date': date_list})
def convert_currency(x):
  try:
    cr=c.convert(1, 'USD', 'INR', x)
    #print(x)
    return cr
  except Exception as e:
    return None
exchange_df['CR'] = exchange_df['Date'].apply(convert_currency)
# Iterate through each row to update MEP value to the next date until a new MEP value is found
for i in range(len(exchange_df) - 1):
    current_cr = exchange_df.loc[i, 'CR']
    next_cr = exchange_df.loc[i + 1, 'CR']
    if pd.isnull(next_cr):
        exchange_df.loc[i + 1, 'CR'] = current_cr

df=pd.merge(df,exchange_df,on='Date',how='left')
df['MEP_INR_PERKG'] = df.apply(lambda x: x['MEP_Updated'] * x['CR'] / 1000, axis=1)
df['Contract_Kg']=df['Contract'].apply(lambda x:x*1000)
df['Flag_Value'] = df['Flag_Start_Stop'].apply(lambda x: 1 if x == 'Start' else (0 if pd.notnull(x) else np.nan))
df_final=df[['Date','MEP_Updated','Contract_Kg','MEP_INR_PERKG','Flag_Value']]
data_list=df_final.to_dict(orient='records')

url2=OUTPUT_URL
table=connect_db(url2,"india","onion_news")
table.delete_many({})
# Insert the new data
table.insert_many(data_list)
