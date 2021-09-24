import psycopg2
import psycopg2.extras
from google.cloud import bigquery
from google.cloud import storage
import json
import datetime
import csv

import configparser


def datetime_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def bigquery_schema_from_json(json_path):
    schema = []
    with open(json_path, encoding="utf-8") as f:
        json_file = f.read()
        fields = json.loads(json_file)

        for field in fields:
            schema.append(bigquery.SchemaField(
                name=field["name"], field_type=field["field_type"], mode=field["mode"]))
    return schema

def csv_remove_line_breaks(path):
    csv_split = []

    with open(t_path_n_file, 'r') as csv_file:
        for line in csv_file:
            whitespace_split = line.split(" ")
            remove_returns = (line.replace('\n', "") for line in whitespace_split)
            csv_split.append(remove_returns)

    with open(t_path_n_file, 'w+', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(csv_split)
    
    return csv_split

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
fd = open('queries/shifts.sql', 'r')
sqlQuery = fd.read()
fd.close()
t_path_n_file = config['DEFAULT']['shifts_create_csv_path']
query = "copy  (" + sqlQuery + ") TO STDOUT WITH (FORMAT csv, DELIMITER ',', HEADER)"
with open(t_path_n_file, 'w') as f_output:

    cursor.copy_expert(query, f_output)




storage_client = storage.Client.from_service_account_json(
    config['DEFAULT']['gcp_key_path'])
bucket = storage_client.get_bucket(
    config['DEFAULT']['cloud_storage_bucket_name'])


# Upload shifts csv to Google Cloud Storage
blob = bucket.blob('blocks_shifts.csv')
blob.upload_from_filename('shifts.csv')

# Next, we populate shifts table from csv in GCS

# Construct a BigQuery client object.
bqclient = bigquery.Client.from_service_account_json(
    config['DEFAULT']['gcp_key_path'])

# Specify table id
table_id = config['DEFAULT']['shifts_table_id']
shifts_schema = bigquery_schema_from_json(
    config['DEFAULT']['shifts_schema_json_path'])
#Truncate table
bqclient.query("TRUNCATE TABLE " + config['DEFAULT']['shifts_table_id'])

job_config = bigquery.LoadJobConfig(
    schema=shifts_schema,
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV
)
uri = config['DEFAULT']['cloud_storage_shifts_path']

load_job = bqclient.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = bqclient.get_table(table_id)  # Make an API request.
print("Loaded {} rows into table: {}.".format(
    destination_table.num_rows, config['DEFAULT']['shifts_table_id']))
