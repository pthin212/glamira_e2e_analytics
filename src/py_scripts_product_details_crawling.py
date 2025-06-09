import configparser
import pymongo
import logging
import requests
import time
from tqdm import tqdm
import os
import multiprocessing
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Read MongoDB and crawling configs
config = configparser.ConfigParser()
config.read([
    "configs/mongodb_config.ini",
    "configs/crawling_config.ini"
])

# Setup logger with file and console handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('crawler.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# MongoDB params and collection names
mongodb_uri = config["mongodb"]["uri"]
db_name = config["mongodb"]["database"]
main_collection_name = "userbeh"
location_collection_name = "product_details"

# Crawling params
batch_size = int(config["crawling"]["batch_size"])
checkpoint_file = config["crawling"]["checkpoint_file"]

def process_url(doc):
    # Extract product_id and URL from document, then scrape product name
    product_id = doc.get("product_id")
    current_url = doc.get("current_url")
    try:
        response = requests.get(current_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        product_name_element = soup.select_one('span.base[data-ui-id="page-title-wrapper"]')
        product_name = product_name_element.text.strip() if product_name_element else "N/A"
        return {"product_id": product_id, "product_name": product_name, "url": current_url}
    except Exception as e:
        logger.error(f"Error fetching {current_url}: {e}")
        return None

def crawl_product_details(mongodb_uri, db_name, main_collection_name, location_collection_name):
    try:
        # Connect to MongoDB collections
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        main_collection = db[main_collection_name]
        new_collection = db[location_collection_name]

        # Aggregate unique products with URLs from main collection events
        pipeline = [
            {"$match": {"collection": {"$in": ["view_product_detail", "select_product_option", "select_product_option_quality"]}}},
            {"$group": {"_id": "$product_id", "current_url": {"$first": "$current_url"}}},
            {"$project": {"_id": 0, "product_id": "$_id", "current_url": 1}}
        ]

        total_docs = list(main_collection.aggregate(pipeline))
        total_count = len(total_docs)

        # Read checkpoint file to resume progress if exists
        try:
            with open(checkpoint_file, 'r') as f:
                total_processed = int(f.read())
        except FileNotFoundError:
            total_processed = 0

        # Use multiprocessing pool and tqdm progress bar for concurrent crawling
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            with tqdm(total=total_count, initial=total_processed, desc="Processing", unit="record") as pbar:
                for result in pool.imap_unordered(process_url, total_docs):
                    if result:
                        # Insert new product details if not exists
                        if new_collection.find_one({"product_id": result["product_id"]}) is None:
                            new_collection.insert_one(result)
                        total_processed += 1
                        pbar.update(1)

                        # Save progress every batch_size
                        if total_processed % batch_size == 0:
                            with open(checkpoint_file, 'w') as f:
                                f.write(str(total_processed))

        logger.info(f"Total processed: {total_processed} records.")

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
    finally:
        # Close MongoDB client
        if 'client' in locals():
            client.close()

        # Remove checkpoint file after completion
        try:
            os.remove(checkpoint_file)
        except OSError as e:
            logger.warning(f"Error removing checkpoint file: {e}")

if __name__ == "__main__":
    crawl_product_details(mongodb_uri, db_name, main_collection_name, location_collection_name)
    logger.info("Crawling completed.")
