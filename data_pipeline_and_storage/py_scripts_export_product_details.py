import configparser
import pymongo
import json
import logging
import datetime
import os
from google.cloud import storage
from google.cloud import exceptions


CONFIG_FILE = "config.ini"
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
batch_size = int(config["app"]["batch_size"])

# MongoDB configuration
mongodb_uri = config["mongodb"]["uri"]
db_name = config["mongodb"]["database"]
main_collection_name = "product_details"

# GCS configuration
bucket_name = config["gcs"]["bucket"]
now = datetime.datetime.now()
timestamp = now.strftime("%Y%m%d_%H%M%S")
output_blob_name = f"product_details_{timestamp}.jsonl"


def export_to_gcs():
    """
    Exports all data from MongoDB to a single JSONL file - Product details in GCS using batch processing.
    """

    try:
        # Establish connection to MongoDB
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        collection = db[main_collection_name]

        # Initialize GCS client and specify bucket/blob
        try:
            storage_client = storage.Client()
        except Exception as e:
            logging.error(f"Failed to initialize GCS client: {e}")
            return
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(output_blob_name)

        logging.info(f"Connected to MongoDB collection '{main_collection_name}' and GCS")

        # Open the blob for writing in JSONL format
        with blob.open('w') as f:
            skip = 0

            while True:
                # Retrieve a batch of documents - excluding the _id field
                cursor = collection.find({}, {"_id": 0}).skip(skip).limit(batch_size)
                documents = list(cursor)

                if not documents:
                    break

                for doc in documents:
                    f.write(json.dumps(doc, default=str) + '\n')

                skip += batch_size

        logging.info(f"Uploaded data from '{main_collection_name}' to gs://{bucket_name}/{output_blob_name}")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if 'client' in locals():
            client.close()
        logging.info("Disconnected from MongoDB")


if __name__ == "__main__":
    export_to_gcs()
