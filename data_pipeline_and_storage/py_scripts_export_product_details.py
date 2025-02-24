import pymongo
import json
import logging
from google.cloud import storage

# MongoDB connection details
mongodb_uri = "mongodb://localhost:27017/"
db_name = "glamiradb"
main_collection_name = "product_details"

# GCS configuration
bucket_name = "raw_gcpdp_01"
output_blob_name = "product_details.json"

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Batch size
batch_size = 1000

def export_to_gcs():
    """
    Exports data from MongoDB to GCS in JSON format using batch processing.

    Connects to the specified MongoDB collection, retrieves data in batches,
    formats it as a JSON array, and uploads it to the specified GCS bucket.
    Handles potential errors during the process and ensures proper disconnection
    from MongoDB.
    """

    try:
        # Establish connection to MongoDB
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        collection = db[main_collection_name]

        # Initialize GCS client and specify bucket/blob
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(output_blob_name)

        logging.info(f"Connected to MongoDB collection '{main_collection_name}' and GCS")

        # Open the blob for writing in JSON array format
        with blob.open('w') as f:
            f.write('[') # Start of JSON array
            first_batch = True # Flag to handle the first batch (no comma)
            skip = 0 # Number of documents to skip (for batching)

            while True:
                # Retrieve a batch of documents, excluding the _id field
                cursor = collection.find({}, {"_id": 0}).skip(skip).limit(batch_size)
                documents = list(cursor)

                # Exit the loop if no more documents are found
                if not documents:
                    break
                
                # Write each document to the blob as a JSON object
                for i, doc in enumerate(documents):
                    # Add a comma before each document except the first one
                    if not first_batch or i > 0:
                        f.write(',')
                    # Convert the document to JSON string and write to the file
                    json.dump(doc, f, default=str)

                # Reset the first_batch flag after the first batch is processed
                first_batch = False
                # Increment the skip counter for the next batch
                skip += batch_size

            f.write(']')

        logging.info(f"Uploaded data from '{main_collection_name}' to gs://{bucket_name}/{output_blob_name}")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if 'client' in locals():
            client.close()
        logging.info("Disconnected from MongoDB")


if __name__ == "__main__":
    export_to_gcs()
