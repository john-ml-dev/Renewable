"""Zips a directory into zip file"""
import zipfile 
import os 
import logging 

def zip_directory(directory_path, zip_path = 'data.zip'):
    """
    Zips the entire directory including all its subdirectories and files.

    :param directory_path: Path to the directory to be zipped
    :param zip_path: Path where the zip file will be created (ending with .zip)
    """
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf: # creates a new compressed zip file at the specified zip_path
            for root, _, files in os.walk(directory_path): # generates the file names in the directory tree
                for file in files:
                    file_path = os.path.join(root, file) # gets the full file path
                    arcname = os.path.relpath(file_path, directory_path) # maintains relative path to preserve structure
                    zipf.write(file_path, arcname)
        logging.info(f'Filed zipped => {zip_path}')
    except OSError as e:
        logging.error(f'Error occured: {e}')