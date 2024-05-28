"""Uploads file to s3 bucket"""

import os
import logging
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def upload_directory_to_s3(directory_path, bucket_name= os.getenv("bucket"), s3_prefix=''):
    """
    Uploads all files from a directory to an S3 bucket.
    
    :param directory_path: Local path to the directory to upload.
    :param bucket_name: Name of the S3 bucket.
    :param s3_prefix: Prefix (folder) in the S3 bucket to upload the files to.
    """
    # Create an S3 client
    s3_client =  boto3.client('s3', aws_access_key_id= os.getenv("ACCESS_KEY"), aws_secret_access_key= os.getenv("SECREET_KEY"))
    
    # Walk through the directory and upload each file
    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            # Get the full path to the file
            local_file_path = os.path.join(root, file_name)
            
            # Compute the S3 object name by combining the prefix with the relative path from the root directory
            relative_path = os.path.relpath(local_file_path, directory_path)
            s3_object_name = os.path.join(s3_prefix, relative_path).replace("\\", "/")  # Replace backslashes with forward slashes for S3 compatibility
            
            try:
                # Upload the file
                s3_client.upload_file(local_file_path, bucket_name, s3_object_name)
                logging.info(f"File '{local_file_path}' uploaded successfully to '{bucket_name}/{s3_object_name}'.")
            except (NoCredentialsError, PartialCredentialsError) as e:
                logging.warn(f"Error: {e}")
                return
            except Exception as e:
                logging.error(f"Error uploading file '{local_file_path}': {e}")
