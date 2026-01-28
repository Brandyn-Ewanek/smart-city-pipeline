import json
import os
from kafka import KafkaConsumer
from pymongo import MongoClient

# Configuration
KAFKA_BROKER = 'kafka:29092'
TOPIC_NAME = 'sensor-data'
MONGO_URI = 'mongodb://admin:password123@mongodb:27017'
DB_NAME = 'smart_city_db'
COLLECTION_NAME = 'sensor_readings'

def get_mongo_collection():
    """Connects to MongoDB and returns the collection."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        print(f"Connected to MongoDb: {DB_NAME}", flush=True)
        return db[COLLECTION_NAME]
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}", flush=True)
        return None

def validate_data(data):
    """
    Checks if the data has the required fields.
    Returns True if valid, False otherwise.
    """
    # These are the fields we absolutely need
    required_fields = ["sensor_id", "timestamp", "metrics", "event_mode"]
    
    # Check 1: Do we have all the keys?
    for field in required_fields:
        if field not in data:
            print(f"Validation Failed: Missing field '{field}'", flush=True)
            return False
            
    # Check 2: Is the temperature missing? (Data integrity check)
    if data.get("metrics", {}).get("temperature_c") is None:
        print("Validation Failed: Missing temperature reading", flush=True)
        return False
        
    return True

def run_consumer():
    print(f"Connecting to Kafka Consumer at {KAFKA_BROKER}...", flush=True)
    
    # Initialize consumer
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='city-data-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Connect to database
    collection = get_mongo_collection()

    print("Waiting for messages...", flush=True)

    # Consumption loop
    for message in consumer:
        try:
            data = message.value
            
            # --- VALIDATION STEP ---
            # We only save the data if it passes our check
            if validate_data(data):
                # Insert into mongodb
                if collection is not None:
                    collection.insert_one(data)
                    
                    # Print a status update to the logs
                    event = data.get('event_mode', 'UNKNOWN')
                    loc = data.get('location_name', 'Unknown Loc')
                    print(f"Stored record from {loc} [Event: {event}]", flush=True)
                    
                    # Alert if a zombie apocalypse is detected
                    if event == "ZOMBIE_APOCALYPSE":
                        print(f"!!! ALERT: Bio-hazard detected in {loc} !!!", flush=True)
            else:
                # If validation fails, we log it but don't crash
                print(f"Skipping malformed record: {data}", flush=True)

        except Exception as e:
            print(f"Error processing message: {e}", flush=True)

if __name__ == "__main__":
    run_consumer()