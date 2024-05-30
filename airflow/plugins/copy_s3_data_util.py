import boto3
import psycopg2
import os
import logging 

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

s3 =  boto3.client('s3', aws_access_key_id= os.getenv("ACCESS_KEY"), aws_secret_access_key= os.getenv("SECREET_KEY"))

def lambda_handler(event, context):
    # S3 event details
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    s3_path = f's3://{bucket}/{key}'
    
    # Redshift connection details
    redshift_host = os.getenv('redshift_host')
    redshift_port = os.getenv('redshift_port')
    redshift_dbname = os.getenv('redshift_dbname')
    redshift_user = os.getenv('redshift_user')
    redshift_password = os.getenv('redshift_password')
    
    # Redshift COPY command
    copy_command = f"""
    COPY weather_data
    FROM '{s3_path}'
    IAM_ROLE 'arn:aws:iam::your-account-id:role/your-role-name'
    FORMAT AS CSV
    IGNOREHEADER 1
    DELIMITER ','
    DATEFORMAT 'auto'
    TIMEFORMAT 'auto'
    EMPTYASNULL
    BLANKSASNULL
    TRIMBLANKS
    FILLRECORD;
    """
    
    # Connect to Redshift and execute COPY command
    try:
        conn = psycopg2.connect(
            dbname=redshift_dbname,
            user=redshift_user,
            password=redshift_password,
            host=redshift_host,
            port=redshift_port
        )
        cur = conn.cursor()
        cur.execute(copy_command)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("COPY command executed successfully.")
    except Exception as e:
        logging.error(f"Error executing COPY command: {e}")

    return {
        'statusCode': 200,
        'body': 'Data copied to Redshift successfully'
    }
