from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd
load_dotenv()
#connect to MongoDB
conn_uri = os.getenv("MONGO_URI")
client = MongoClient(conn_uri)

#read csv file 
df = pd.read_csv('./datasource/diamond.csv')


#remove rows with null values
df.dropna(inplace=True) 

#drop Unnamed column
df.drop(columns=['Unnamed: 0'],inplace=True)

#insert into mongodb
client.basic_etl.diamonds.insert_many(df.to_dict('records'))
client.close()