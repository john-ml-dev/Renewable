"""Extracting, Transforming Data and Writing as csv"""
import pandas as pd 

def extract_transform(file_path, output_path):
    """
    Extracts and transform data given
    
    :para file_path: file path of dataset
    :para output_path: file path to save csvs
    
    """
    #Loading data
    df = pd.read_csv(file_path, parse_dates=['Time'], index_col=['Time'])
    # extracting  year,month, day, hour from data
    df['year'] = df.index.year 
    df['month'] = df.index.month
    df['day'] = df.index.day
    df['hour'] = df.index.hour
    df['minute'] = df.index.minute
    # Group data by year
    date_partitions = df.groupby(pd.Grouper(freq='Y'))
    for date, partition_df in date_partitions:
    # Generate filename for CSV file
        filename = f"{output_path}partition_{date.strftime('%Y')}.csv"
        # Write partition_df to CSV
        partition_df.to_csv(filename, index=False)