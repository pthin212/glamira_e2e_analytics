import pymongo
import json
import logging
from google.cloud import storage

# MongoDB connection details
mongodb_uri = "mongodb://localhost:27017/"
db_name = "glamiradb"
main_collection_name = "user_ip_locations"

# GCS bucket name
bucket_name = "raw_gcpdp_01"

# GCS output file name (single file)
output_blob_name = "user_ip_locations.json"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Batch size 
batch_size = 10000

def export_to_gcs():
    """Exports all data from MongoDB to a single JSON file in GCS using batch processing."""
    try:
        # MongoDB connection
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        collection = db[main_collection_name]

        # GCS client and bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(output_blob_name)

        logging.info("Connected to MongoDB and GCS")

        with blob.open('w') as f:  # Open the blob for writing
            f.write('[')  # Start the JSON array
            first_batch = True
            skip = 0

            while True:
                cursor = collection.find().skip(skip).limit(batch_size)
                documents = list(cursor)

                if not documents:
                    break

                for i, doc in enumerate(documents):
                    if not first_batch or i > 0:
                        f.write(',')  # Add comma separator between documents
                    json.dump(doc, f, default=str)  # Write each document

                first_batch = False
                skip += batch_size

            f.write(']')  # Close the JSON array

        logging.info(f"Uploaded all documents to gs://{bucket_name}/{output_blob_name}")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if 'client' in locals():
            client.close()
        logging.info("Disconnected from MongoDB")


if __name__ == "__main__":
    export_to_gcs()
