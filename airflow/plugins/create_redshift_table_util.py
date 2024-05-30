import psycopg2
import os
import logging 

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

# Define connection details
redshift_host = os.getenv('redshift_host')
redshift_port = os.getenv('redshift_port')
redshift_dbname = os.getenv('redshift_dbname')
redshift_user = os.getenv('redshift_user')
redshift_password = os.getenv('redshift_password')

# Define the SQL statement to create the table
create_table_sql = """
CREATE TABLE IF NOT EXISTS renewable (
    energy_delta_wh FLOAT,                -- Energy delta in watt-hours
    ghi FLOAT,                            -- Global Horizontal Irradiance (GHI)
    temp FLOAT,                           -- Temperature
    pressure FLOAT,                       -- Atmospheric pressure
    humidity FLOAT,                       -- Humidity percentage
    wind_speed FLOAT,                     -- Wind speed
    rain_1h FLOAT,                        -- Rain volume for the last hour
    snow_1h FLOAT,                        -- Snow volume for the last hour
    clouds_all INTEGER,                   -- Cloudiness percentage
    is_sun BOOLEAN,                       -- Indicates if the sun is out
    sunlight_time FLOAT,                  -- Total sunlight time
    day_length FLOAT,                     -- Length of the day
    sunlight_time_daylength FLOAT,        -- Ratio of sunlight time to day length
    weather_type VARCHAR(50),             -- Weather type description
    hour INTEGER,                         -- Hour of the day
    month INTEGER,                        -- Month of the year
    year INTEGER,                         -- Year
    day INTEGER,                          -- Day of the month
    minute INTEGER                        -- Minute of the hour
);
"""
def create_rds_table():
    # Connect to Redshift
    try:
        conn = psycopg2.connect(
            dbname=redshift_dbname,
            user=redshift_user,
            password=redshift_password,
            host=redshift_host,
            port=redshift_port
        )
        cur = conn.cursor()
        
        # Execute the CREATE TABLE command
        cur.execute(create_table_sql)
        conn.commit()

        logging.info("Table created successfully.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
