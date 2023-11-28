from pymongo.mongo_client import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd
import time
import mysql.connector as mysql
load_dotenv()
def connections():
    try:
        #make connections to mysql
        mysql_connection = mysql.connect(host = 'localhost',database = 'etl',user = 'Abdulshakur',password = os.getenv('MYSQL_PASSWORD'))
        #make connection to mongodb
        conn_uri = os.getenv("MONGO_URI")
        mongodb_connection = MongoClient(conn_uri)
        return mongodb_connection, mysql_connection
    except Exception as e:
        print('Error:',e)

mongodb_connection, mysql_connection = connections()

time_interval_in_minutes = 15



def clean_data(data):
    data.dropna(inplace=True) #remove rows with null values
    return data

def append_processing_time(data):
    current_time_struct = time.gmtime(time.time() + 3600)
    current_time = time.strftime("%Y:%m:%d, %H:%M:%S", current_time_struct)
    data['INSERTED_AT'] = current_time
    return data

def etl_pipeline(data):
    try:
        mongodb_connection.basic_etl.diamonds.insert_many(data.to_dict('records'))
    except Exception as e:
        print('Error: ', e)
        
        
if __name__ == "__main__":     
    try:
        cursor = mysql_connection.cursor()
        cursor.execute(f'SELECT * from diamonds where created > DATE_SUB(NOW(), INTERVAL {time_interval_in_minutes} MINUTE)')
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns = ['id','carat','cut','color','clarity','depth','table', 'price','x','y','z','created'])    
        data = clean_data(df)
        data = append_processing_time(data)
        etl_pipeline(data)
    except Exception as e:
        print('Error: ',e)




