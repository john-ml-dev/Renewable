"""Loading csv files to Postgresql Database"""
import pandas as pd 
import logging
import os
from sqlalchemy import create_engine

def load_csv_sql(file_dir:str, output_directory:str, username:str, password:str, port:int, host:str, database:str):
    # create connection uri
    connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
    # create engine
    db_engine = create_engine(connection_string)
    # list all files in directory
    files = os.listdir(file_dir)
    for file_path in files:
        # create a relative path
        file_path = os.path.join(output_directory,file_path)
        # check if it is a file and is a .csv file
        if(os.path.isfile(file_path)) and file_path.endswith('.csv'):  
            # creating dataframe from file
            file_df = pd.read_csv(file_path)
            # getting the name of the file without its extension 
            name,_ = os.path.splitext(os.path.basename(file_path)) # returns a tuple (filename,ext) 
            try: 
                # loading data to database
                file_df.to_sql(name=name, con=db_engine, if_exists='replace', index=False)
                logging.info(f"{name} was successfully added to database")
                
            except Exception as e:
                logging.error(f'An error occured: {e}')