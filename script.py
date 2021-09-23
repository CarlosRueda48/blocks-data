import psycopg2
import psycopg2.extras
from google.cloud import bigquery

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

pqlconn = psycopg2.connect(dbname=config['DEFAULT']['db_name'],
                        user=config['DEFAULT']['user'],
                        password=config['DEFAULT']['password'],
                        host=config['DEFAULT']['host'],
                        port=config['DEFAULT']['port'],
                        sslmode='require',
                        sslkey=config['DEFAULT']['sslkey_path'],
                        sslcert=config['DEFAULT']['sslcert_path']
                        )

# Construct a BigQuery client object.
bqclient = bigquery.Client.from_service_account_json('keys/gcp_key.json')

cursor = pqlconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cursor.execute("""SELECT * FROM onearizona.shifts
ORDER BY id ASC """)
shifts = cursor.fetchall()
for row in shifts:
    print(row['id'])




query = """
    SELECT name, SUM(number) as total_people
    FROM `bigquery-public-data.usa_names.usa_1910_2013`
    WHERE state = 'TX'
    GROUP BY name, state
    ORDER BY total_people DESC
    LIMIT 20
"""
query_job = bqclient.query(query)  # Make an API request.

print("The query data:")
for row in query_job:
    # Row values can be accessed by field name or index.
    print("name={}, count={}".format(row[0], row["total_people"]))


