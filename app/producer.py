import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

# Kafka
KAFKA_BROKER = 'kafka:29092'
TOPIC_NAME = 'sensor-data'

# Locations 
LOCATIONS = [
    {"id": "LOC-01", "name": "Downtown Core", "type": "urban"},
    {"id": "LOC-02", "name": "Industrial District", "type": "industrial"},
    {"id": "LOC-03", "name": "Greenwood Suburbs", "type": "residential"},
    {"id": "LOC-04", "name": "City Park", "type": "nature"}
]

# Events
EVENTS = [
    "NORMAL", 
    "ZOMBIE_APOCALYPSE", 
    "COVID_LOCKDOWN", 
    "HEAT_DOME", 
    "MUSIC_FESTIVAL"
]

# Initialize 
fake = Faker()

def get_event_modifiers(event_name):
    """
    Returns data modifiers based on the active simulation event.
    Returns a dictionary of multipliers/offsets for sensor values.
    """
    # Defaults
    mods = {
        "temp_add": 0, "noise_add": 0, "traffic_mult": 1.0, 
        "aqi_add": 0, "bio_hazard": 0, "status": "OK"
    }

    if event_name == "ZOMBIE_APOCALYPSE":
        mods["noise_add"] = 40        # Zombies/Screaming
        mods["traffic_mult"] = 0.1    # Roads blocked
        mods["bio_hazard"] = 95       # Infection high
        mods["status"] = "CRITICAL_FAILURE"
        
    elif event_name == "COVID_LOCKDOWN":
        mods["noise_add"] = -20       # Quiet streets
        mods["traffic_mult"] = 0.05   # Empty streets
        mods["aqi_add"] = -30         # Clean air 
        mods["status"] = "RESTRICTED"
        
    elif event_name == "HEAT_DOME":
        mods["temp_add"] = 15         # Extreme heat
        mods["aqi_add"] = 40          # Stagnant air
        mods["bio_hazard"] = 10
        mods["status"] = "WARNING"
        
    elif event_name == "MUSIC_FESTIVAL":
        mods["noise_add"] = 50        #  loud
        mods["traffic_mult"] = 5.0    # Crowds
        mods["temp_add"] = 5          # Body heat / Lights
        mods["status"] = "CONGESTION"

    return mods

def create_sensor_data(event_mode):
    """Generates a sensor reading affected by the current event."""
    
    # Pick a random location
    location = random.choice(LOCATIONS)
    
    # Get the event modifiers
    mods = get_event_modifiers(event_mode)
    
    # Random Base Values
    base_temp = random.uniform(20.0, 25.0)
    base_noise = random.uniform(30.0, 50.0)
    base_traffic = random.randint(100, 500)
    base_aqi = random.randint(20, 50)
    
    # Apply Event Modifiers
    final_temp = base_temp + mods["temp_add"]
    final_noise = base_noise + mods["noise_add"]
    final_traffic = int(base_traffic * mods["traffic_mult"])
    final_aqi = base_aqi + mods["aqi_add"]
    
    # Cap values
    final_aqi = max(0, min(500, final_aqi))
    
    return {
        "sensor_id": fake.uuid4(),
        "timestamp": time.time(),
        "location_id": location["id"],
        "location_name": location["name"],
        "event_mode": event_mode,
        "metrics": {
            "temperature_c": round(final_temp, 2),
            "noise_level_db": round(final_noise, 2),
            "pedestrian_traffic": final_traffic,
            "air_quality_index": final_aqi,
            "bio_hazard_level": mods["bio_hazard"]
        },
        "system_status": mods["status"]
    }

def run_producer():
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...", flush=True)
    
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka!", flush=True)
        except Exception as e:
            print(f"Waiting for Kafka... {e}", flush=True)
            time.sleep(5)

    # SIMULATION LOOP
    current_event_index = 0
    cycles_per_event = 20 # How many data points before switching events
    counter = 0

    try:
        while True:
            # Switch events every 20 cycles to demonstrate data drift
            if counter >= cycles_per_event:
                current_event_index = (current_event_index + 1) % len(EVENTS)
                counter = 0
            
            current_event = EVENTS[current_event_index]
            
            # Generate Data
            data = create_sensor_data(current_event)
            
            # Send to Kafka
            producer.send(TOPIC_NAME, data)
            
            # Print for logs
            print(f"[{current_event}] Sent data for {data['location_name']}: Temp={data['metrics']['temperature_c']}C, BioHazard={data['metrics']['bio_hazard_level']}", flush=True)
            
            counter += 1
            time.sleep(2)

    except KeyboardInterrupt:
        producer.close()

if __name__ == "__main__":
    time.sleep(10)
    run_producer()