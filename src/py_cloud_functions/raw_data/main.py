import json
from google.cloud import bigquery
from google.cloud import storage
from functions_framework import cloud_event
from google.cloud.exceptions import GoogleCloudError
from datetime import datetime
import time
from decimal import Decimal

@cloud_event
def trigger_bigquery_load(cloud_event):
    """
    Cloud Function triggered by a Cloud Storage event to load a single JSON file - Raw data into BigQuery.
    Moves the processed file to a separate processed bucket.
    """

    try:
        # Get bucket name and file name from event data.
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

        # Initialize BigQuery and Cloud Storage clients.
        client = bigquery.Client(project="striking-figure-445310-d1")
        storage_client = storage.Client()

        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(file_name)

        # Define BigQuery table and destination bucket.
        table_id = "striking-figure-445310-d1.glamira_data.raw_events"
        destination_bucket_name = "raw_data_processed"
        destination_bucket = storage_client.bucket(destination_bucket_name)

        table = client.get_table(table_id)
        schema = table.schema
        total_inserted_records = 0
        chunk_size = 1000 # Batch size for BigQuery inserts.
        records_processed = 0

        try:
            json_string = source_blob.download_as_string().decode("utf-8")
            lines = json_string.splitlines()
        except Exception as e:
            print(f"Error reading file {file_name}: {e}")
            return

        buffer = []
        inserted_count_file = 0

        for line in lines:
            try:
                data = json.loads(line)
                buffer.append(data)
                records_processed += 1

                if len(buffer) >= chunk_size:
                    rows_to_insert = process_data_chunk(buffer)
                    insert_rows_with_retry(client, table, rows_to_insert)
                    inserted_count_file += len(buffer)
                    total_inserted_records += len(buffer)
                    buffer = []
                    print(f"Processed {records_processed} records so far...")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON object: {e}, Line: {line.strip()}")
            except Exception as e:
                print(f"Error processing line: {e}, Line: {line.strip()}")

        # Process remaining records
        if buffer:
            rows_to_insert = process_data_chunk(buffer)
            insert_rows_with_retry(client, table, rows_to_insert)
            inserted_count_file += len(buffer)
            total_inserted_records += len(buffer)

        print(f"Finished processing file: {file_name}. Inserted {inserted_count_file} records.")

        # Move the processed file to the destination bucket.
        try:
            destination_blob = destination_bucket.blob(file_name)
            blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, file_name)
            source_blob.delete()

            print(f"Blob {file_name} in bucket {bucket_name} moved to blob {destination_blob.name} in bucket {destination_bucket_name}.")
        except Exception as e:
            print(
                f"Error moving file from {file_name} to {destination_bucket_name}: {e}, type={type(e)}, blob_name={file_name}")

        print(f"Finished processing all files. Total inserted: {total_inserted_records}")


    except GoogleCloudError as e:
        print(f"Google Cloud Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def process_option_array(option_array_data):
    """
    Processes option array data from the JSON and converts it into a list of dictionaries.
    """

    options = []
    if option_array_data is not None and isinstance(option_array_data, list):
        for item in option_array_data:
            option = {}
            if isinstance(item, dict):
                option['option_label'] = item.get('option_label')
                if isinstance(item.get('option_id'), dict) and '$numberInt' in item['option_id']:
                    option['option_id'] = str(item['option_id']['$numberInt'])
                else:
                    option['option_id'] = str(item.get('option_id', ''))
                option['value_label'] = item.get('value_label')
                if isinstance(item.get('value_id'), dict) and '$numberInt' in item['value_id']:
                    option['value_id'] = str(item['value_id']['$numberInt'])
                else:
                    option['value_id'] = str(item.get('value_id', ''))
                option['quality'] = item.get('quality')
                option['quality_label'] = item.get('quality_label')
            options.append(option)
    return options


def process_cart_products(cart_products_data):
    """
    Processes cart product data, handling number types and option arrays.
    """

    cart_products = []
    if cart_products_data is not None and isinstance(cart_products_data, list):
        for product in cart_products_data:
            cart_product = {}

            # product_id
            if 'product_id' in product and product['product_id'] is not None:
                cart_product['product_id'] = int(product['product_id'].get('$numberInt', 0))
            else:
                cart_product['product_id'] = None

            # amount
            if 'amount' in product and product['amount'] is not None:
                cart_product['amount'] = int(product['amount'].get('$numberInt', 0))
            else:
                cart_product['amount'] = None

            cart_product['price'] = product.get('price')
            cart_product['currency'] = product.get('currency')
            cart_product['option'] = process_option_array(product.get('option'))
            cart_products.append(cart_product)
    return cart_products


def process_extended_options(extended_options_data):
    """
    Processes extended option data, extracting relevant fields.
    """

    extended_options = {}
    if extended_options_data is not None and isinstance(extended_options_data, dict):
        extended_options['alloy'] = extended_options_data.get('alloy')
        extended_options['stone'] = extended_options_data.get('stone')
        extended_options['pearlcolor'] = extended_options_data.get('pearlcolor')
        extended_options['finish'] = extended_options_data.get('finish')
        extended_options['price'] = extended_options_data.get('price')
        extended_options['category_id'] = extended_options_data.get('category id')
        extended_options['kollektion'] = extended_options_data.get('Kollektion')
        extended_options['kollektion_id'] = extended_options_data.get('kollektion_id')
        extended_options['diamond'] = extended_options_data.get('diamond')
        extended_options['shapediamond'] = extended_options_data.get('shapediamond')
    return extended_options


def handle_number_field(order_id_data):
    """
    Handles different number types in order_id field.
    """

    if order_id_data is None:
        return None
    if '$numberInt' in order_id_data:
        return int(order_id_data['$numberInt'])
    elif '$numberDouble' in order_id_data:
        return Decimal(order_id_data['$numberDouble'])
    else:
        raise ValueError("Invalid order_id format: {}".format(order_id_data))


def process_data_chunk(chunk):
    """
    Processes a chunk of data and prepares it for BigQuery insertion.
    """

    rows_to_insert = []
    for item in chunk:
        row = {}
        row['record_id'] = item.get('_id', {}).get('$oid')
        row['event_collection'] = item.get('collection')
        row['timestamp'] = int(item['time_stamp']['$numberInt']) if 'time_stamp' in item and '$numberInt' in item[
            'time_stamp'] else 0
        row['ip'] = item.get('ip')
        row['user_agent'] = item.get('user_agent')
        row['resolution'] = item.get('resolution')
        row['user_id_db'] = item.get('user_id_db')
        row['device_id'] = item.get('device_id')
        row['api_version'] = item.get('api_version')
        row['store_id'] = item.get('store_id')
        row['local_time'] = item.get('local_time')
        row['show_recommendation'] = item.get('show_recommendation')
        row['current_url'] = item.get('current_url')
        row['referrer_url'] = item.get('referrer_url')
        row['email_address'] = item.get('email_address')
        row['product_id'] = item.get('product_id')
        row['viewing_product_id'] = item.get('viewing_product_id')
        row['price'] = item.get('price')
        row['currency'] = item.get('currency')
        row['is_paypal'] = item.get('is_paypal')
        row['key_search'] = item.get('key_search')
        row['cat_id'] = item.get('cat_id')
        row['collect_id'] = item.get('collect_id')
        row['utm_source'] = str(item.get('utm_source'))
        row['utm_medium'] = str(item.get('utm_medium'))
        row['recommendation'] = item.get('recommendation')
        row['recommendation_product_id'] = item.get('recommendation_product_id')

        # local_time
        if 'local_time' in item and item['local_time']:
            parsed_time = datetime.strptime(item['local_time'], "%Y-%m-%d %H:%M:%S")
            row['local_time'] = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            row['local_time'] = None

        # recommendation_clicked_position
        if 'recommendation_clicked_position' in item and item['recommendation_clicked_position'] is not None:
            row['recommendation_clicked_position'] = int(item['recommendation_clicked_position'].get('$numberInt', 0))
        else:
            row['recommendation_clicked_position'] = None

        # recommendation_product_position
        if 'recommendation_product_position' in item:
            value = item['recommendation_product_position']
            if isinstance(value, str):
                if value.isdigit():
                    row['recommendation_product_position'] = int(value)
                elif value == "":
                    row['recommendation_product_position'] = None
                else:
                    row['recommendation_product_position'] = None
            elif isinstance(value, int):
                row['recommendation_product_position'] = value
            else:
                row['recommendation_product_position'] = None
        else:
            row['recommendation_product_position'] = None

        # order_id
        order_id_data = item.get('order_id')
        if order_id_data is not None and isinstance(order_id_data, dict) and (
                '$numberInt' in order_id_data or '$numberDouble' in order_id_data):
            row['order_id'] = handle_number_field(order_id_data)
        else:
            row['order_id'] = None

        row['cart_products'] = process_cart_products(item.get('cart_products'))

        option_data = item.get('option')
        if option_data is not None:
            if isinstance(option_data, dict):
                row['extended_options'] = process_extended_options(option_data)
                row['product_options'] = []
            elif isinstance(option_data, list):
                row['product_options'] = process_option_array(option_data)
                row['extended_options'] = {}
            else:
                row['product_options'] = []
                row['extended_options'] = {}
        else:
            row['product_options'] = []
            row['extended_options'] = {}

        rows_to_insert.append(row)
    return rows_to_insert


def insert_rows_with_retry(client, table, rows_to_insert):
    """
    Inserts rows into BigQuery with retry.
    """

    errors = client.insert_rows(table, rows_to_insert)
    if errors:
        print(f"Errors while loading data batch: {errors}")
        for i in range(3):
            time.sleep(2 ** (i + 1))
            errors = client.insert_rows(table, rows_to_insert)
            if not errors:
                break
        else:
            print(f"Failed to insert rows after multiple retries: {errors}")