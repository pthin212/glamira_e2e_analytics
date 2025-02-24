# This Python code defines a Google Cloud Function that is triggered by a Cloud Storage event.
# It processes JSON files uploaded to a specified bucket, extracts product information, 
# and inserts it into a BigQuery table. After processing, the files are moved to a processed folder and deleted from source.
import json
from google.cloud import bigquery
from google.cloud import storage
from functions_framework import cloud_event
from google.cloud.exceptions import GoogleCloudError

@cloud_event
def trigger_bigquery_load(cloud_event):
    """
    Cloud Function triggered by a Cloud Storage event to load JSON data into BigQuery.
    Moves processed files to a 'processed' folder within the same bucket.
    """

    try:
        # Extract the bucket name from the cloud event data.
        bucket_name = cloud_event.data.get('bucket')
        if not bucket_name:
            print("Error: 'bucket' key not found or empty in cloud_event data.")
            return

        print(f"Processing bucket: {bucket_name}")

        # Initialize BigQuery and Storage clients.
        client = bigquery.Client(project="striking-figure-445310-d1")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Define the BigQuery table ID and the destination bucket and folder for processed files.
        table_id = "striking-figure-445310-d1.glamira_data.products"
        processed_bucket_name = bucket_name 
        processed_folder = "bigdata_processed/product_details/" 
        
        # Define the prefix for the files to be processed in the bucket.
        prefix = "bigdata_importing_product_details/"

        # Get the BigQuery table schema.
        table = client.get_table(table_id)
        schema = table.schema

        # List all blobs (files) in the bucket that match the defined prefix.
        blobs = bucket.list_blobs(prefix=prefix)
        total_inserted_records = 0

        # Iterate through each blob.
        for blob in blobs:
            if blob.name.endswith(".json"):
                print(f"Processing file: {blob.name}")
                try:
                    # Download the blob content as bytes and decode it to a Python object (list of dictionaries).
                    content = blob.download_as_bytes()
                    data = json.loads(content.decode('utf-8'))
                    
                    # Check if the data is a list (expected format).
                    if not isinstance(data, list):
                        print(f"Error: Expected a list of dictionaries in {blob.name}")
                        continue
                    
                    rows_to_insert = []
                    inserted_count_file = 0
                    # Iterate through each record in the JSON data.
                    for i, record in enumerate(data):
                        try:
                            # Ensure the record is a dictionary.
                            if isinstance(record, dict):
                                # Prepare the row data for insertion into BigQuery, adding a record_id
                                row = {
                                    'record_id': str(i),
                                    'product_id': record.get('product_id'),
                                    'product_name': record.get('product_name'),
                                    'url': record.get('url')
                                }
                                rows_to_insert.append(row)
                            else:
                                print(f"Warning: Skipping invalid record (not a dictionary) in {blob.name}, index {i}: {record}")
                            # Insert rows in batches of 1000 to improve performance.
                            if len(rows_to_insert) >= 1000:
                                errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                                if errors:
                                    print(f"Errors while loading data batch from {blob.name}: {errors}")
                                else:
                                    inserted_count_file += len(rows_to_insert)
                                    total_inserted_records += len(rows_to_insert)
                                    print(f"Inserted {len(rows_to_insert)} records from {blob.name}. Total inserted: {total_inserted_records}")
                                rows_to_insert = []

                        except Exception as e:
                            print(f"Error processing record {i} in {blob.name}: {e}")
                    # Insert remaining rows if any.
                    if rows_to_insert:
                        errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                        if errors:
                            print(f"Errors while loading final data batch from {blob.name}: {errors}")
                        else:
                            inserted_count_file += len(rows_to_insert)
                            total_inserted_records += len(rows_to_insert)
                            print(f"Inserted {len(rows_to_insert)} records from {blob.name}. Total inserted: {total_inserted_records}")
                    print(f"Finished processing file: {blob.name}. Inserted {inserted_count_file} records.")

                    # Create the destination blob name for the processed file.
                    destination_blob_name = processed_folder + blob.name[len(prefix):]

                    # Move the processed blob to the processed folder in the same bucket and then remove the source file.
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

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in {blob.name}: {e}")
                except Exception as e:
                   print(f"Error processing {blob.name}: {e}, type={type(e)}")

        # Print a message indicating the completion of the process.
        print(f"Finished processing all files. Total inserted: {total_inserted_records}")

    except GoogleCloudError as e:
        print(f"Google Cloud Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")