import ijson
import json
from google.cloud import bigquery
from google.cloud import storage
from functions_framework import cloud_event
from google.cloud.exceptions import GoogleCloudError
import time 

@cloud_event
def trigger_bigquery_load(cloud_event):
    """
    Cloud Function triggered by a Cloud Storage event to load JSON data into BigQuery.
    Moves processed files to a 'processed' folder within the same bucket.
    """

    try:
        # Get bucket name from event data
        bucket_name = cloud_event.data.get('bucket')
        if not bucket_name:
            print("Error: 'bucket' key not found or empty in cloud_event data.")
            return
        
        print(f"Processing bucket: {bucket_name}")
        
        # Initialize BigQuery and Cloud Storage clients.
        client = bigquery.Client(project="striking-figure-445310-d1")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Define BigQuery table details.
        table_id = "striking-figure-445310-d1.glamira_data.user_ip_locations"

        # Define Cloud Storage folder for processed files.
        processed_bucket_name = bucket_name
        processed_folder = "bigdata_processed/ip_locations/"
        prefix = "bigdata_importing_ip_locations/"

        # Get table schema for efficient row insertion.
        table = client.get_table(table_id)
        schema = table.schema

        # Initialize counter for inserted records.
        total_inserted_records = 0

        # Iterate through all blobs with the specified prefix.
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            if blob.name.endswith(".json"):
                print(f"Processing file: {blob.name}")
                inserted_count_file = 0
                rows_to_insert = []
                try:
                    parser = ijson.parse(blob.open("r"))
                    for prefix, event, value in parser:
                        # Build a row dictionary from the JSON data.
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
                            # Insert rows into BigQuery in batches.
                            if len(rows_to_insert) >= 1000:
                                errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                                if errors:
                                    print(f"Errors while loading data batch from {blob.name}: {errors}")
                                    # Retry mechanism for BigQuery insertion errors.
                                    for i in range(3):  # Retry up to 3 times
                                        time.sleep(2**(i+1)) # Exponential backoff
                                        errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                                        if not errors:
                                            break
                                    else:  # if the loop completes without a break
                                        print(f"Failed to insert rows after multiple retries from {blob.name}: {errors}")
                                else:
                                    inserted_count_file += len(rows_to_insert)
                                    total_inserted_records += len(rows_to_insert)
                                    print(f"Inserted {len(rows_to_insert)} records from {blob.name}. Total inserted: {total_inserted_records}")
                                rows_to_insert = [] # Clear batch after insertion

                    # Insert remaining rows
                    if rows_to_insert:
                        errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                        if errors:
                            print(f"Errors while loading final data batch from {blob.name}: {errors}")
                            # Retry mechanism for final batch
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

                # File moving code
                destination_blob_name = processed_folder + blob.name.replace("bigdata_importing_ip_locations/", "")
                try:
                    destination_generation_match_precondition = 0
                    blob_copy = bucket.copy_blob(
                        blob,
                        storage_client.bucket(processed_bucket_name),
                        destination_blob_name,
                        if_generation_match=destination_generation_match_precondition,
                    )
                    bucket.delete_blob(blob.name)
                    print(
                        f"Blob {blob.name} in bucket {bucket_name} moved to blob {blob_copy.name} in bucket {processed_bucket_name}."
                    )
                except Exception as e:
                    print(f"Error moving file from {blob.name} to {destination_blob_name}: {e}, type={type(e)}, blob_name={blob.name}")


        print(f"Finished processing all files. Total inserted: {total_inserted_records}")

    except GoogleCloudError as e:
        print(f"Google Cloud Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")