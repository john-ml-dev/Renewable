"""Creating CSV files"""

def create_csv(group_df, output_path):
# Iterate over groups
    for date, partition_df in group_df:
        # Generate filename for CSV file
        filename = f"{output_path}partition_{date.strftime('%Y')}.csv"
        # Write partition_df to CSV
        partition_df.to_csv(filename, index=False)
