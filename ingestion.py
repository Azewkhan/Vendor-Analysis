import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename='logs/ingestion_db.log',
    filemode='a',      # 'w' to overwrite, 'a' to append
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
   
)

engine = create_engine('sqlite:///inventory.db')
def ingest_db(df,table_name,engine):
    '''This function conversts dataframe into sql DB '''
    df.to_sql(table_name,con=engine, if_exists= 'replace',index= False)

def load_data():
    ''' This function reads the different csv files and stores them in DB'''
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv("data/"+file)
            logging.info(f"Ingestion for the {file} has started")
            ingest_db(df,file[:-4],engine)
            logging.info(f"Ingestion for the {file} has ended")
    end = time.time()
    logging.info("Ingestion has been completed")
    total_time  = (end - start)/60
    logging.info(f"Ingestion has been completed in {total_time} minutes")

if __name__ == '__main__':
    load_data()
