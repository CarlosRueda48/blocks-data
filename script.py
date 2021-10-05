import psycopg2
import psycopg2.extras
from google.cloud import bigquery
from google.cloud import storage
import json
import datetime
import csv
import time
import configparser


def bigquery_schema_from_json(json_path):
    schema = []
    with open(json_path, encoding="utf-8") as f:
        json_file = f.read()
        fields = json.loads(json_file)

        for field in fields:
            schema.append(bigquery.SchemaField(
                name=field["name"], field_type=field["type"], mode=field["mode"]))
    return schema

def csv_remove_line_breaks(path):
    csv_split = []

    with open(path, 'r') as csv_file:
        for line in csv_file:
            whitespace_split = line.split(" ")
            remove_returns = (line.replace('\n', "") for line in whitespace_split)
            csv_split.append(remove_returns)

    with open(path, 'w+', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(csv_split)
    
    return csv_split


def postgresql_table_to_csv(table_name):
    pqlconn = psycopg2.connect(dbname=config['DEFAULT']['db_name'],
                               user=config['DEFAULT']['user'],
                               password=config['DEFAULT']['password'],
                               host=config['DEFAULT']['host'],
                               port=config['DEFAULT']['port'],
                               sslmode='require',
                               sslkey=config['DEFAULT']['sslkey_path'],
                               sslcert=config['DEFAULT']['sslcert_path']
                               )
    pqlconn.set_client_encoding('UTF8')

    cursor = pqlconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    fd = open(config[table_name]["query_file"], 'r')
    sql_query = fd.read()

    fd.close()
    t_path_n_file = config[table_name]['create_csv_path']
    query = "copy  (" + sql_query + ") TO STDOUT WITH (FORMAT csv, DELIMITER ',', HEADER)"
    with open(t_path_n_file, 'w', encoding='utf-8') as f_output:

        cursor.copy_expert(query, f_output)
    print("Saved information to csv: ", t_path_n_file)

def upload_csv_to_gcp_storage(table_name):
    storage_client = storage.Client.from_service_account_json(
        config['DEFAULT']['gcp_key_path'])
    bucket = storage_client.get_bucket(
        config['DEFAULT']['cloud_storage_bucket_name'])

    path = config[table_name]['create_csv_path']

    # Upload shifts csv to Google Cloud Storage
    blob = bucket.blob(path)
    blob.chunk_size = 1024*1024*10
    blob.upload_from_filename(path)

    print("Uploaded csv to GCP Storage path: ", config[table_name]['cloud_storage_csv_path'])

def storage_csv_to_bigquery(table_name):
    # Construct a BigQuery client object.
    bqclient = bigquery.Client.from_service_account_json(
        config['DEFAULT']['gcp_key_path'])

    # Specify table id
    table_id = config[table_name]['table_id']
    shifts_schema = bigquery_schema_from_json(
        config[table_name]['schema_json_path'])

    #Truncate table
    bqclient.query("TRUNCATE TABLE " + config[table_name]['table_id'])

    job_config = bigquery.LoadJobConfig(
        schema=shifts_schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    )
    uri = config[table_name]['cloud_storage_csv_path']

    load_job = bqclient.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bqclient.get_table(table_id)  # Make an API request.
    print("Loaded {} rows into table: {}.".format(
        destination_table.num_rows, config[table_name]['table_id']))


def blocks_to_bigquery(table_name):
    start = time.time()
    postgresql_table_to_csv(table_name)
    upload_csv_to_gcp_storage(table_name)
    storage_csv_to_bigquery(table_name)
    end = time.time()
    print("Data processed for ", table_name, " table in", (end - start), " seconds.")

def main():
    start = time.time()
    blocks_to_bigquery("SHIFTS")
    blocks_to_bigquery("HARD_REPORT")
    blocks_to_bigquery("CANVASSERS")
    blocks_to_bigquery("REGISTRATION_FORMS")
    blocks_to_bigquery("TURFS")
    #blocks_to_bigquery("LOCATIONS")
    end = time.time()
    print("Total processing time: ", (end - start), " seconds.")

config = configparser.ConfigParser()
config.read('config.ini')
main()
