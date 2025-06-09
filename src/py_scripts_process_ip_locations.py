import configparser
import pymongo
import IP2Location
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Read MongoDB and IP2Location configs
config = configparser.ConfigParser()
config.read([
    "configs/mongodb_config.ini",
    "configs/ip2location_config.ini"
])

# MongoDB parameters and collections
mongodb_uri = config["mongodb"]["uri"]
db_name = config["mongodb"]["database"]
main_collection_name = "userbeh"
location_collection_name = "user_ip_locations"

# IP2Location database path
ip2location_db_path = config["ip2location"]["db_path"]

def process_ip_locations(mongodb_uri, db_name, main_collection_name, location_collection_name, ip2location_db_path):
    try:
        # Connect to MongoDB and open IP2Location DB
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        main_collection = db[main_collection_name]
        location_collection = db[location_collection_name]

        ip2loc_obj = IP2Location.IP2Location()
        ip2loc_obj.open(ip2location_db_path)

        # Aggregate unique IPs from main collection
        pipeline = [{"$group": {"_id": "$ip"}}, {"$project": {"ip": "$_id", "_id": 0}}]

        bulk_operations = []
        processed_count = 0

        # Process each unique IP: query IP2Location DB and prepare insert
        for doc in main_collection.aggregate(pipeline, allowDiskUse=True):
            ip = doc["ip"]
            try:
                record = ip2loc_obj.get_all(ip)
                location_data = {
                    "ipAddress": ip,
                    "country_code": record.country_short,
                    "country_name": record.country_long,
                    "region": record.region,
                    "city": record.city
                }
                bulk_operations.append(pymongo.InsertOne(location_data))

                # Bulk write in chunks of 100,000
                if len(bulk_operations) >= 100000:
                    location_collection.bulk_write(bulk_operations)
                    bulk_operations = []
                    processed_count += 100000
                    print(f"Processed {processed_count} IPs.")

            except Exception as e:
                print(f"Error processing IP {ip}: {e}")

        # Write any remaining operations
        if bulk_operations:
            location_collection.bulk_write(bulk_operations)
            print(f"Processed {processed_count + len(bulk_operations)} IPs.")

    except Exception as e:
        print(f"Main error: {e}")
    finally:
        # Cleanup: close MongoDB and IP2Location DB connections
        if 'client' in locals():
            client.close()
        if 'ip2loc_obj' in locals():
            ip2loc_obj.close()

if __name__ == "__main__":
    process_ip_locations(mongodb_uri, db_name, main_collection_name, location_collection_name, ip2location_db_path)
