import json
from google.cloud import bigquery
from google.cloud import storage
from functions_framework import cloud_event
from google.cloud.exceptions import GoogleCloudError

@cloud_event
def trigger_bigquery_load(cloud_event):
    """
    Cloud Function triggered by a Cloud Storage event to load a single JSON file - Product details into BigQuery.
    Moves the processed file to a separate processed bucket.
    """

    try:
        # Extract bucket name and file name from the cloud event data.
        bucket_name = cloud_event.data.get('bucket')
        file_name = cloud_event.data.get('name')

        if not bucket_name:
            print("Error: 'bucket' key not found or empty in cloud_event data.")
            return
        if not file_name:
            print("Error: 'name' key not found or empty in cloud_event data.")
            return

        if not file_name.endswith(".json"):
            print(f"Skipping non-JSON file: {file_name}")
            return

        print(f"Processing file: {file_name} in bucket: {bucket_name}")

        # Initialize BigQuery and Storage clients.
        client = bigquery.Client(project="striking-figure-445310-d1")
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(file_name)

        # Define the BigQuery table ID and the destination bucket.
        table_id = "striking-figure-445310-d1.glamira_data.products"
        destination_bucket_name = "product_details_processed"
        destination_bucket = storage_client.bucket(destination_bucket_name)

        # Get the BigQuery table schema.
        table = client.get_table(table_id)
        schema = table.schema

        try:
            # Download the blob content as bytes and decode it to a Python object.
            # list of dictionaries.
            content = source_blob.download_as_bytes()
            data = json.loads(content.decode('utf-8'))

            # Check if the data is a list.
            if not isinstance(data, list):
                print(f"Error: Expected a list of dictionaries in {file_name}")
                return

            rows_to_insert = []
            inserted_count_file = 0
            for i, record in enumerate(data):
                try:
                    if isinstance(record, dict):
                        row = {
                            'record_id': str(i),
                            'product_id': record.get('product_id'),
                            'product_name': record.get('product_name'),
                            'url': record.get('url')
                        }
                        rows_to_insert.append(row)
                    else:
                        print(f"Warning: Skipping invalid record (not a dictionary) in {file_name}, index {i}: {record}")

                    if len(rows_to_insert) >= 1000:
                        errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                        if errors:
                            print(f"Errors while loading data batch from {file_name}: {errors}")
                        else:
                            inserted_count_file += len(rows_to_insert)
                            print(f"Inserted {len(rows_to_insert)} records from {file_name}")
                        rows_to_insert = []

                except Exception as e:
                    print(f"Error processing record {i} in {file_name}: {e}")

            # Insert remaining rows.
            if rows_to_insert:
                errors = client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
                if errors:
                    print(f"Errors while loading final data batch from {file_name}: {errors}")
                else:
                    inserted_count_file += len(rows_to_insert)
                    print(f"Inserted {len(rows_to_insert)} records from {file_name}")
            print(f"Finished processing file: {file_name}. Inserted {inserted_count_file} records.")

            # Move the processed file to the destination bucket.
            try:
                destination_blob = destination_bucket.blob(file_name)
                blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, file_name)
                source_blob.delete()

                print(
                    f"Blob {file_name} in bucket {bucket_name} moved to blob {destination_blob.name} in bucket {destination_bucket_name}."
                )
            except Exception as e:
                print(f"Error moving file from {file_name} to {destination_bucket_name}: {e}, type={type(e)}, blob_name={file_name}")


        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in {file_name}: {e}")
        except Exception as e:
            print(f"Error processing {file_name}: {e}, type={type(e)}")


    except GoogleCloudError as e:
        print(f"Google Cloud Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")