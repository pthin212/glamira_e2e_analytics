import pymongo
import logging
import requests
import time
from tqdm import tqdm
import os
import multiprocessing
from bs4 import BeautifulSoup

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler('crawler.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# MongoDB connection details
mongodb_uri = "mongodb://localhost:27017/"
db_name = "glamiradb"
main_collection_name = "userbeh"
location_collection_name = "product_details"
batch_size = 100  # Checkpoint every 100 records
checkpoint_file = "checkpoint.txt"

def process_url(doc):
    """
    Processes a single URL to extract product name.

    Args:
        doc (dict): Document containing 'product_id' and 'current_url'.

    Returns:
        dict: A dictionary containing product details if successful, None otherwise.
    """

    product_id = doc.get("product_id")
    current_url = doc.get("current_url")
    try:
        response = requests.get(current_url, timeout=10)
        response.raise_for_status()
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            product_name_element = soup.select_one('span.base[data-ui-id="page-title-wrapper"]')
            product_name = product_name_element.text.strip() if product_name_element else "N/A"
            return {"product_id": product_id, "product_name": product_name, "url": current_url}
        else:
            logger.warning(f"URL {current_url} returned status code {response.status_code}. Skipping.")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching URL {current_url}: {e}")
        return None
    except AttributeError:
        logger.error(f"Could not find product name in URL {current_url}")
        return None
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        return None

def crawl_product_details(mongodb_uri, db_name, main_collection_name, location_collection_name):
    """
    Crawls product details from URLs stored in MongoDB and saves them to another collection.
    """
    
    try:
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        main_collection = db[main_collection_name]
        new_collection = db[location_collection_name]

        pipeline = [
            {
                "$match": {
                    "collection": {
                        "$in": ["view_product_detail", "select_product_option", "select_product_option_quality"]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$product_id",
                    "current_url": {"$first": "$current_url"} 
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "product_id": "$_id",
                    "current_url": 1
                }
            }
        ]

        # Get the count efficiently
        cursor = main_collection.aggregate(pipeline)
        total_count = len(list(cursor))

        try:
            with open(checkpoint_file, 'r') as f:
                total_processed = int(f.read())
        except FileNotFoundError:
            total_processed = 0

        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            with tqdm(total=total_count, initial=total_processed, desc="Processing", unit="record") as pbar:
                # Re-execute the aggregation!
                for result in pool.imap_unordered(process_url, main_collection.aggregate(pipeline)):
                    if result:
                        if new_collection.find_one({"product_id": result["product_id"]}) is None:
                             new_collection.insert_one(result)

                        total_processed += 1
                        pbar.update(1)
                        if total_processed % batch_size == 0:
                            with open(checkpoint_file, 'w') as f:
                                f.write(str(total_processed))

        logger.info(f"Total processed: {total_processed} records.")

    except pymongo.errors.ConnectionFailure as e:
        logger.critical(f"Could not connect to MongoDB: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        client.close()
        try:
            os.remove(checkpoint_file)
        except OSError as e:
            logger.warning(f"Error removing checkpoint file: {e}")

if __name__ == "__main__":
    crawl_product_details(mongodb_uri, db_name, main_collection_name, location_collection_name)
    logger.info("Crawling completed.")
