import ijson
from google.cloud import bigquery
from google.cloud import storage
from functions_framework import cloud_event
from google.cloud.exceptions import GoogleCloudError
import time

@cloud_event
def trigger_bigquery_load(cloud_event):
    """
    Cloud Function triggered by a Cloud Storage event to load a single JSON file - IP Locations into BigQuery.
    Moves the processed file to a separate processed bucket.
    """

    try:
        # Get bucket name and file name from event data
        source_bucket_name = cloud_event.data.get('bucket')
        file_name = cloud_event.data.get('name')

        if not source_bucket_name:
            print("Error: 'bucket' key not found or empty in cloud_event data.")
            return
        if not file_name:
            print("Error: 'name' key not found or empty in cloud_event data.")
            return

        print(f"Processing file: gs://{source_bucket_name}/{file_name}")

        destination_bucket_name = "ip_locations_processed"

        if not file_name.endswith(".json"):
            print(f"Skipping file {file_name}: not a .json file.")
            return

        # Initialize BigQuery and Cloud Storage clients.
        client = bigquery.Client(project="striking-figure-445310-d1")
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(source_bucket_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        # Define BigQuery table details.
        table_id = "striking-figure-445310-d1.glamira_data.user_ip_locations"

        # Get table schema for efficient row insertion.
        table = client.get_table(table_id)
        schema = table.schema

        # Initialize counters.
        total_inserted_records = 0
        inserted_count_file = 0
        rows_to_insert = []

        # Get the blob (file) from Cloud Storage.
        blob = source_bucket.blob(file_name)

        try:
            print(f"Processing file: {blob.name}")
            parser = ijson.parse(blob.open("r"))
            for prefix, event, value in parser:
                if (prefix == 'item' and event == 'start_map'):
                    row = {}
                elif (prefix == 'item.record_id' and event == 'string'):
                    row['record_id'] = value
                elif (prefix == 'item.ipAddress' and event == 'string'):
                    row['ip_address'] = value
                elif (prefix == 'item.country_code' and event == 'string'):
                    row['country_code'] = value
                elif (prefix == 'item.country_name' and event == 'string'):
                    row['country_name'] = value
                elif (prefix == 'item.region' and event == 'string'):
                    row['region'] = value
                elif (prefix == 'item.city' and event == 'string'):
                    row['city'] = value
                elif (prefix == 'item' and event == 'end_map'):
                    rows_to_insert.append(row)
                    if len(rows_to_insert) >= 1000:
                        errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                        if errors:
                            print(f"Errors while loading data batch from {blob.name}: {errors}")
                            for i in range(3):
                                time.sleep(2**(i+1)) # Exponential backoff
                                errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                                if not errors:
                                    break
                            else:
                                print(f"Failed to insert rows after multiple retries from {blob.name}: {errors}")
                        else:
                            inserted_count_file += len(rows_to_insert)
                            total_inserted_records += len(rows_to_insert)
                            print(f"Inserted {len(rows_to_insert)} records from {blob.name}. Total inserted: {total_inserted_records}")
                        rows_to_insert = []

            # Insert remaining rows
            if rows_to_insert:
                errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                if errors:
                    print(f"Errors while loading final data batch from {blob.name}: {errors}")
                    for i in range(3):
                        time.sleep(2**(i+1))
                        errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                        if not errors:
                            break
                    else:
                        print(f"Failed to insert final batch after multiple retries from {blob.name}: {errors}")

                else:
                    inserted_count_file += len(rows_to_insert)
                    total_inserted_records += len(rows_to_insert)
                    print(f"Inserted {len(rows_to_insert)} records from {blob.name}. Total inserted: {total_inserted_records}")

        except ijson.JSONError as e:
            print(f"Error decoding JSON in {blob.name}: {e}")
        except Exception as e:
            print(f"Error processing {blob.name}: {e}, type={type(e)}")

        print(f"Finished processing file: {blob.name}. Inserted {inserted_count_file} records.")

        # Move the processed file to the destination bucket.
        try:
            destination_blob = destination_bucket.blob(file_name)
            blob_copy = source_bucket.copy_blob(blob, destination_bucket, file_name)
            source_bucket.delete_blob(file_name)
            print(f"Blob {blob.name} in bucket {source_bucket_name} moved to blob {destination_blob.name} in bucket {destination_bucket_name}.")

        except Exception as e:
            print(f"Error moving file from {blob.name} to {destination_bucket_name}: {e}, type={type(e)}, blob_name={blob.name}")

        print(f"Finished processing file: {file_name}. Total inserted: {total_inserted_records}")

    except GoogleCloudError as e:
        print(f"Google Cloud Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")