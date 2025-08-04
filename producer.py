import time
import json
import psutil
import os
import csv
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'osquery-events'
BASELINE_CSV_FILE = 'baseline_data.csv' # The file to store our training data
CSV_HEADER = ['pid', 'ppid', 'create_time'] # The features we will use

def get_process_data():
    """
    Gathers data for all running processes using psutil.
    """
    process_list = []
    for proc in psutil.process_iter(['pid', 'name', 'username', 'create_time', 'ppid']):
        try:
            pinfo = proc.info
            event = {
                "name": "process_events",
                "columns": {
                    "pid": pinfo['pid'],
                    "name": pinfo['name'],
                    "username": pinfo['username'],
                    "create_time": pinfo['create_time'],
                    "parent": pinfo['ppid']
                },
                "action": "snapshot"
            }
            process_list.append(event)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return process_list

def write_to_csv(processes):
    """
    Writes the selected features for each process to a CSV file.
    """
    # Check if the file already exists to decide whether to write the header.
    file_exists = os.path.isfile(BASELINE_CSV_FILE)

    # Open the file in 'append' mode ('a'). This adds new rows without deleting old ones.
    with open(BASELINE_CSV_FILE, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADER)

        # If the file is new, write the header row.
        if not file_exists:
            writer.writeheader()

        # Loop through all processes and write their data.
        for process_data in processes:
            columns = process_data.get("columns", {})
            # We only write the row if all our required features are present.
            if all(key in columns for key in ['pid', 'parent', 'create_time']):
                writer.writerow({
                    'pid': columns['pid'],
                    'ppid': columns['parent'],
                    'create_time': columns['create_time']
                })

def main():
    """
    Main function to run the producer.
    """
    print("Starting CyScan Producer (with data logging)...")
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"Connected to Kafka broker at {KAFKA_BROKER}")
    print(f"Logging baseline data to '{BASELINE_CSV_FILE}'")

    while True:
        try:
            processes = get_process_data()
            
            # --- Send to Kafka (no change here) ---
            print(f"Found {len(processes)} processes. Sending to Kafka...")
            for process_data in processes:
                producer.send(KAFKA_TOPIC, process_data)
            producer.flush()
            
            # --- NEW: Write to CSV for training ---
            write_to_csv(processes)
            print("Logged data to CSV. Waiting for 5 seconds...")
            
            time.sleep(5)

        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(15)

if __name__ == "__main__":
    main()
