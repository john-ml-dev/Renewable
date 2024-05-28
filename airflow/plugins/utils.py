# import logging
# import os
# import pandas as pd 
# from sqlalchemy import create_engine
# import logging
# import zipfile
# import boto3
# from botocore.exceptions import ClientError


# logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

# # Create output directory for files
# output_directory = "airflow/partitioned_data/"

# def extract_transform(file_path):
#     """
#     Extracts and transform data given
    
#     :para file_path: file path of dataset
    
#     """
#     #Loading data
#     df = pd.read_csv(file_path, parse_dates=['Time'], index_col=['Time'])
#     # extracting  year,month, day, hour from data
#     df['year'] = df.index.year 
#     df['month'] = df.index.month
#     df['day'] = df.index.day
#     df['hour'] = df.index.hour
#     df['minute'] = df.index.minute
#     # Group data by year
#     date_partitions = df.groupby(pd.Grouper(freq='Y'))
#     return date_partitions

# def create_csvs(group_df, output_path):
# # Iterate over groups
#     for date, partition_df in group_df:
#         # Generate filename for CSV file
#         filename = f"{output_path}partition_{date.strftime('%Y')}.csv"
#         # Write partition_df to CSV
#         partition_df.to_csv(filename, index=False)

# def load_csv_sql(file_dir:str, username:str, password:str, port:int, host:str, database:str):
#     # create connection uri
#     connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
#     # create engine
#     db_engine = create_engine(connection_string)
#     # list all files in directory
#     files = os.listdir(file_dir)
#     for file_path in files:
#         # create a relative path
#         file_path = os.path.join(output_directory,file_path)
#         # check if it is a file and is a .csv file
#         if(os.path.isfile(file_path)) and file_path.endswith('.csv'):  
#             # creating dataframe from file
#             file_df = pd.read_csv(file_path)
#             # getting the name of the file without its extension 
#             name,_ = os.path.splitext(os.path.basename(file_path)) # returns a tuple (filename,ext) 
#             try: 
#                 # loading data to database
#                 file_df.to_sql(name=name, con=db_engine, if_exists='replace', index=False)
#                 logging.info(f"{name} was successfully added to database")
                
#             except Exception as e:
#                 logging.error(f'An error occured: {e}')

# def zip_directory(directory_path, zip_path = 'data.zip'):
#     """
#     Zips the entire directory including all its subdirectories and files.

#     :param directory_path: Path to the directory to be zipped
#     :param zip_path: Path where the zip file will be created (ending with .zip)
#     """
#     try:
#         with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf: # creates a new compressed zip file at the specified zip_path
#             for root, _, files in os.walk(directory_path): # generates the file names in the directory tree
#                 for file in files:
#                     file_path = os.path.join(root, file) # gets the full file path
#                     arcname = os.path.relpath(file_path, directory_path) # maintains relative path to preserve structure
#                     zipf.write(file_path, arcname)
#         logging.info(f'Filed zipped => {zip_path}')
#     except OSError as e:
#         logging.error(f'Error occured: {e}')

# def upload_file(file_name, bucket, object_name=None):
#     """Upload a file to an S3 bucket

#     :param file_name: File to upload
#     :param bucket: Bucket to upload to
#     :param object_name: S3 object name. If not specified then file_name is used
#     :return: True if file was uploaded, else False
#     """

#     # If S3 object_name was not specified, use file_name
#     if object_name is None:
#         object_name = os.path.basename(file_name)

#     # Upload the file
#     s3_client = boto3.client('s3', aws_access_key_id= os.getenv("ACCESS_KEY"), aws_secret_access_key= os.getenv("SECREET_KEY"))
#     try:
#         response = s3_client.upload_file(file_name, bucket, object_name)
#     except ClientError as e:
#         logging.error(e)
#         return False
#     return True



# """
#  |+| dtypes conversion
#  |+| index reseting (Time Series Data)
#  |+| data partitioning for easy querying
#  |+| zip partitioned files
#  |+| loading to db
#  |-| loading to s3
#  |-| coping to redshift

# TESTS
#  |-| check dtypes 
#  |-| check number of columns and rows
#  |-| check number of years and total number of partitions
#  |+| check data has been zipped
#  |-| check data has been successfully loaded to db, s3, redshift

# """

# zip_directory(output_directory,os.path.join(output_directory,'data'))