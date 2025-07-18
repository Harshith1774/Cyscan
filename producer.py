import time
import json
import psutil
from kafka import KafkaProducer

# --- Configuration ---
# This is the address of our Kafka broker running in Docker.
KAFKA_BROKER = 'localhost:9092'
# This is the name of the Kafka topic we will send our data to.
KAFKA_TOPIC = 'osquery-events'

def get_process_data():
    """
    Gathers data for all running processes using psutil.
    Formats the data to look similar to OSquery's process_events.
    """
    process_list = []
    for proc in psutil.process_iter(['pid', 'name', 'username', 'create_time', 'ppid']):
        try:
            # Get process information as a dictionary
            pinfo = proc.info
            
            # We will create a JSON object that mimics the structure of an OSquery event.
            # This makes it easier for our detection engine later.
            event = {
                "name": "process_events",
                "columns": {
                    "pid": pinfo['pid'],
                    "name": pinfo['name'],
                    "username": pinfo['username'],
                    "create_time": pinfo['create_time'],
                    "parent": pinfo['ppid']
                },
                "action": "snapshot" # We'll call it a 'snapshot' event
            }
            process_list.append(event)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Some processes might disappear while we are iterating, so we just ignore them.
            pass
    return process_list

def main():
    """
    Main function to run the producer.
    """
    print("Starting CyScan Producer...")
    
    # Create the Kafka producer client.
    # It will automatically try to connect to the broker.
    # The value_serializer tells the producer how to convert our Python dictionary to bytes.
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Connected to Kafka broker at {KAFKA_BROKER}")

    # The main loop
    while True:
        try:
            # Get the list of all running processes
            processes = get_process_data()
            
            print(f"Found {len(processes)} processes. Sending to Kafka topic '{KAFKA_TOPIC}'...")

            # Send each process's data as a separate message to Kafka
            for process_data in processes:
                producer.send(KAFKA_TOPIC, process_data)
            
            # Flush the producer to make sure all messages are sent.
            producer.flush()
            
            print("Data sent successfully. Waiting for 5 seconds...")
            # Wait for 10 seconds before collecting data again.
            time.sleep(5)

        except Exception as e:
            print(f"An error occurred: {e}")
            # If something goes wrong (e.g., Kafka is down), wait a bit and try again.
            time.sleep(10)

if __name__ == "__main__":
    main()
