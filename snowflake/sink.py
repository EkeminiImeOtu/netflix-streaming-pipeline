import snowflake.connector
import boto3
import json

ssm = boto3.client('ssm', region_name='us-east-1')

def get_param(name):
    return ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']

def create_table_if_not_exists(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS PROD.DBT_RAW.NETFLIX_STREAMING_EVENTS (
            USER_ID VARCHAR,
            SHOW_NAME VARCHAR,
            DEVICE VARCHAR,
            COUNTRY VARCHAR,
            WATCH_DURATION_MINS INTEGER,
            RATING FLOAT,
            EVENT_TIMESTAMP VARCHAR,
            IS_COMPLETED BOOLEAN,
            LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Table ready!")
    cur.close()

def write_to_snowflake(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"Batch {batch_id}: No data to write")
        return

    conn = snowflake.connector.connect(
        user=get_param('/snowflake/username'),
        password=get_param('/snowflake/password'),
        account=get_param('/snowflake/accountname'),
        warehouse='COMPUTE_WH',
        database='PROD',
        schema='DBT_RAW'
    )

    create_table_if_not_exists(conn)
    cur = conn.cursor()

    rows = batch_df.collect()
    inserted = 0

    for row in rows:
        cur.execute("""
            INSERT INTO NETFLIX_STREAMING_EVENTS
            (USER_ID, SHOW_NAME, DEVICE, COUNTRY, 
             WATCH_DURATION_MINS, RATING, EVENT_TIMESTAMP, IS_COMPLETED)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.user_id,
            row.show_name,
            row.device,
            row.country,
            row.watch_duration_mins,
            row.rating,
            row.timestamp,
            row.is_completed
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Batch {batch_id}: Inserted {inserted} rows into Snowflake!")

