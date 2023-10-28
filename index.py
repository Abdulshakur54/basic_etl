from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd
import time
load_dotenv()
#connect to MongoDB
conn_uri = os.getenv("MONGO_URI")

#read csv file 
df = pd.read_csv('./datasource/diamond.csv')


#remove rows with null values
df.dropna(inplace=True) 

#drop Unnamed column
df.drop(columns=['Unnamed: 0'],inplace=True)

current_time_struct = time.gmtime(time.time() + 3600)
current_time = time.strftime("%Y:%m:%d, %H:%M:%S", current_time_struct)
df['INSERTED_AT'] = current_time
#insert into mongodb
try:
    client = MongoClient(conn_uri)
    client.basic_etl.diamonds.insert_many(df.to_dict('records'))
    client.close()
except Exception as e:
    print(e)