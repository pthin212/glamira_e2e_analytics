import pymongo
import IP2Location

def process_ip_locations(mongodb_uri, db_name, main_collection_name, location_collection_name, ip2location_db_path):
    """
    Processes IP addresses from a MongoDB collection and enriches them with location data using the IP2Location database.  
    The enriched data is then inserted into a separate MongoDB collection.
    Processses for IPV4 only.
    """

    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        main_collection = db[main_collection_name]
        location_collection = db[location_collection_name]

        # Initialize IP2Location object
        ip2loc_obj = IP2Location.IP2Location()
        ip2loc_obj.open(ip2location_db_path)

        # Get unique IPs using aggregation
        pipeline = [
            {"$group": {"_id": "$ip"}},
            {"$project": {"ip": "$_id", "_id": 0}}
        ]

        bulk_operations = []
        processed_count = 0
        for doc in main_collection.aggregate(pipeline, allowDiskUse=True):
            ip = doc["ip"]
            try:
                # Lookup location data
                record = ip2loc_obj.get_all(ip)

                location_data = {
                    "ipAddress": ip,
                    "country_code": record.country_short,
                    "country_name": record.country_long,
                    "region": record.region,
                    "city": record.city
                }

                bulk_operations.append(pymongo.InsertOne(location_data))

                if len(bulk_operations) >= 100000:
                    location_collection.bulk_write(bulk_operations)
                    bulk_operations = []
                    processed_count += 100000
                    print(f"Processed {processed_count} IPs.")

            except IP2Location.IP2LocationError as e:
                print(f"IP2Location error for {ip}: {e}")
            except pymongo.errors.PyMongoError as e:
                print(f"MongoDB error: {e}")
            except Exception as e:
                print(f"Error processing IP {ip}: {e}")

        # Insert any remaining operations after the loop
        if bulk_operations:
            location_collection.bulk_write(bulk_operations)
            print(f"Processed {processed_count + len(bulk_operations)} IPs.")


    except pymongo.errors.PyMongoError as e:
        print(f"MongoDB connection error: {e}")
    except Exception as e:
        print(f"Main error: {e}")
    finally:
        if client:
            client.close()
        if ip2loc_obj:
            ip2loc_obj.close()


if __name__ == "__main__":
    mongodb_uri = "mongodb://localhost:27017/"
    db_name = "glamiradb"
    main_collection_name = "userbeh"
    location_collection_name = "user_ip_locations"
    ip2location_db_path = "/home/thinhduong2123/IP-COUNTRY-REGION-CITY.BIN"
    process_ip_locations(mongodb_uri, db_name, main_collection_name, location_collection_name, ip2location_db_path)
