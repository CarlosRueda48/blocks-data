import psycopg2
import psycopg2.extras
from google.cloud import bigquery
from google.cloud import storage
import json
import datetime

import configparser


def datetime_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

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

cursor = pqlconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

t_path_n_file = config['DEFAULT']['shifts_create_csv_path']
query = "copy  (select * from onearizona.shifts) TO STDOUT WITH (FORMAT csv, DELIMITER ',', HEADER)"
with open(t_path_n_file, 'w') as f_output:
    
    cursor.copy_expert(query, f_output)


storage_client = storage.Client.from_service_account_json(config['DEFAULT']['gcp_key_location'])
bucket = storage_client.get_bucket('oneaz_blocks_data')

#Upload shifts csv to Google Cloud Storage
blob = bucket.blob('blocks_shifts.csv')
blob.upload_from_filename('shifts.csv')

# Next, we populate shifts table from csv in GCS

# Construct a BigQuery client object.
bqclient = bigquery.Client.from_service_account_json(
    config['DEFAULT']['gcp_key_location'])

# Specify table id
table_id = config['DEFAULT']['shifts_table_id']

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = config['DEFAULT']['cloud_storage_shifts_path']

load_job = bqclient.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = bqclient.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))
