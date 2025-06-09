import configparser
import pymongo
import json
import logging
import datetime
import os
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Read configs for app, MongoDB, and GCS
config = configparser.ConfigParser()
config.read([
    "configs/app_config.ini",
    "configs/mongodb_config.ini",
    "configs/gcs_config.ini"
])

# Setup logging format and level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get batch size from app config
batch_size = int(config["app"]["batch_size"])

# MongoDB connection parameters
mongodb_uri = config["mongodb"]["uri"]
db_name = config["mongodb"]["database"]
main_collection_name = "user_ip_locations"

# GCS parameters
bucket_name = config["gcs"]["bucket"]
now = datetime.datetime.now()
timestamp = now.strftime("%Y%m%d_%H%M%S")
output_blob_name = f"user_ip_locations_{timestamp}.jsonl"

def export_to_gcs():
    try:
        # Connect to MongoDB and GCS
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        collection = db[main_collection_name]

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(output_blob_name)

        logging.info("Connected to MongoDB and GCS")

        # Export collection documents in batches as JSON lines
        with blob.open('w') as f:
            skip = 0
            while True:
                cursor = collection.find({}, {"_id": 0}).skip(skip).limit(batch_size)
                documents = list(cursor)
                if not documents:
                    break
                for doc in documents:
                    f.write(json.dumps(doc, default=str) + '\n')
                skip += batch_size

        logging.info(f"Uploaded all documents to gs://{bucket_name}/{output_blob_name}")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        # Close MongoDB client and log disconnection
        if 'client' in locals():
            client.close()
        logging.info("Disconnected from MongoDB")

if __name__ == "__main__":
    export_to_gcs()
