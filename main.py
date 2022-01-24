import psycopg2
import psycopg2.extras
from google.cloud import bigquery
from google.cloud import storage
import json
import time
import configparser
import os
from sys import platform


def bigquery_schema_from_json(json_path):
    schema = []
    with open(json_path, encoding="utf-8") as f:
        json_file = f.read()
        fields = json.loads(json_file)

        for field in fields:
            schema.append(bigquery.SchemaField(
                name=field["name"], field_type=field["type"], mode=field["mode"]))
    return schema


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
    query = "copy  (" + sql_query + \
        ") TO STDOUT WITH (FORMAT csv, DELIMITER ',', HEADER)"
    with open(t_path_n_file, 'w', encoding='utf-8') as f_output:

        cursor.copy_expert(query, f_output)
    print("Saved information to csv: ", t_path_n_file)


def upload_csv_to_gcp_storage(table_name):
    #Get Cloud Storage bucket
    storage_client = storage.Client.from_service_account_json(
        config['DEFAULT']['gcp_key_path'])
    bucket = storage_client.get_bucket(
        config['DEFAULT']['cloud_storage_bucket_name'])

    #Get path where csv will be saved to in bucket
    path = config[table_name]['create_csv_path']

    # Upload shifts csv to Google Cloud Storage
    blob = bucket.blob(path)
    blob.chunk_size = 1024*1024*10
    blob.upload_from_filename(path)

    print("Uploaded csv to GCP Storage path: ",
          config[table_name]['cloud_storage_csv_path'])


def storage_csv_to_bigquery(table_name):
    # Construct a BigQuery client object.
    bqclient = bigquery.Client.from_service_account_json(
        config['DEFAULT']['gcp_key_path'])

    # Specify table id
    table_id = config[table_name]['table_id']
    table_schema = bigquery_schema_from_json(
        config[table_name]['schema_json_path'])

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=table_schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    )
    uri = config[table_name]['cloud_storage_csv_path']

    load_job = bqclient.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.
    
    print("Loading data into table: ", config[table_name]['table_id'])
    load_job.result()  # Waits for the job to complete.
    
    destination_table = bqclient.get_table(table_id)  # Make an API request.
    print("Loaded {} rows into table: {}.".format(
        destination_table.num_rows, config[table_name]['table_id']))


def blocks_to_bigquery(table_name):
    print("Processing data for: ", table_name)
    start = time.time()
    postgresql_table_to_csv(table_name)
    upload_csv_to_gcp_storage(table_name)
    storage_csv_to_bigquery(table_name)
    end = time.time()
    print("Data processed for ", table_name,
          " table in", (end - start), " seconds.")


def blocks_update(event=None, context=None):
    global config

    config = configparser.ConfigParser()
    config.read("config.ini")

    #Get SSL cert and key
    storage_client = storage.Client.from_service_account_json(
        config['DEFAULT']['gcp_key_path'])
    bucket = storage_client.get_bucket(
        config['DEFAULT']['cloud_storage_bucket_name'])

    print("Obtaining SSL certificate and key")
    if(platform == 'linux'):
        print("Found platform: Linux, adjusting file path.")
        sslkey_path = config['GCP']['sslkey_path']
        sslcert_path = config['GCP']['sslcert_path']
    else:
        sslkey_path = config['DEFAULT']['sslkey_path']
        sslcert_path = config['DEFAULT']['sslcert_path']
    
    blob = bucket.get_blob(config['DEFAULT']['gcp_sslkey_path'])
    blob.download_to_filename(sslkey_path)
    blob = bucket.get_blob(config['DEFAULT']['gcp_sslcert_path'])
    blob.download_to_filename(sslcert_path)
    print("SSL information retrieved successfully.")
    
    if(platform == 'linux'):
        print("Found platform: Linux, adjusting file permissions.")
        os.chmod(config['DEFAULT']['sslkey_path'], 0o600)
        os.chmod(config['DEFAULT']['sslcert_path'], 0o600)

    start = time.time()
    blocks_to_bigquery("SHIFTS")
    blocks_to_bigquery("HARD_REPORT")
    blocks_to_bigquery("CANVASSERS")
    blocks_to_bigquery("REGISTRATION_FORMS")
    blocks_to_bigquery("TURFS")
    blocks_to_bigquery("REPORT_TO_DATE")
    blocks_to_bigquery("SCANS_QC_OVERVIEW")
    end = time.time()
    print("Total processing time: ", (end - start), " seconds.")


blocks_update()
