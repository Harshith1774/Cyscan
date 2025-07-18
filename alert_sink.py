import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

# --- Configuration ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'security-alerts'
# We now define the Elasticsearch connection as a single URL.
ELASTICSEARCH_URL = 'http://localhost:9200'
ELASTICSEARCH_INDEX = 'cyscan-alerts'

def main():
    """
    Main function to run the alert sink.
    """
    print("Starting Alert Sink...")

    # --- Connect to Elasticsearch ---
    # THE FIX IS HERE: We now use a simpler, more modern connection method.
    try:
        es_client = Elasticsearch(ELASTICSEARCH_URL)
        if es_client.ping():
            print("Connected to Elasticsearch successfully!")
        else:
            print("Could not connect to Elasticsearch. Please check if it's running.")
            return
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        return

    # --- Ensure the index exists ---
    # This is good practice. We check if our 'cyscan-alerts' index exists,
    # and if not, we create it.
    if not es_client.indices.exists(index=ELASTICSEARCH_INDEX):
        es_client.indices.create(index=ELASTICSEARCH_INDEX)
        print(f"Created Elasticsearch index: {ELASTICSEARCH_INDEX}")


    # --- Connect to Kafka ---
    # Create a Kafka consumer to listen to the 'security-alerts' topic.
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        group_id='elasticsearch-sink-group'
    )
    
    print(f"Listening for alerts on Kafka topic '{KAFKA_TOPIC}'...")

    # --- The Main Loop ---
    for message in consumer:
        try:
            alert_str = message.value.decode('utf-8')
            alert_json = json.loads(alert_str)
            
            print(f"Received alert: {alert_json}")

            # --- Index the alert into Elasticsearch ---
            response = es_client.index(
                index=ELASTICSEARCH_INDEX,
                document=alert_json
            )
            
            print(f"Alert indexed into Elasticsearch. ID: {response['_id']}")

        except json.JSONDecodeError:
            print(f"Warning: Could not decode message, skipping: {message.value}")
        except Exception as e:
            print(f"An error occurred while processing message: {e}")


if __name__ == "__main__":
    main()
