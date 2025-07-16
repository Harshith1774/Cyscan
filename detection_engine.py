import faust
import json

# --- Configuration ---
# Define the Kafka broker address.
KAFKA_BROKER = 'kafka://localhost:9092'

# --- Faust Application Setup ---
# 1. Create the Faust 'app'. This is the main object for our stream processor.
#    We give it a name ('cyscan-engine') and tell it where to find Kafka.
app = faust.App('cyscan-engine', broker=KAFKA_BROKER)

# 2. Define the "topics". A topic is a channel of data in Kafka.
#    Faust needs to know about the topics we're reading from and writing to.
#    We define the input topic where our process data arrives.
input_topic = app.topic('osquery-events')

#    We define the output topic where we will send our generated alerts.
output_topic = app.topic('security-alerts')


# --- The Detection Logic ---
# 3. Define the "agent". An agent is a function that processes messages from a topic.
#    The decorator '@app.agent(input_topic)' tells Faust that this function
#    should be run for every single message that arrives on the 'input_topic'.
@app.agent(input_topic)
async def process_event(events):
    # The 'events' parameter is a stream of messages from Kafka.
    # We can loop through them asynchronously.
    async for event in events:
        try:
            # The event from Kafka is in bytes, so we need to decode and parse it as JSON.
            # Note: Faust can do this automatically with "models", but we'll do it manually
            # for clarity in this first version.
            data = event

            # --- THIS IS OUR DETECTION RULE ---
            # We will check if the process name is 'Calculator'.
            # This is a very simple rule for testing purposes.
            process_name = data.get("columns", {}).get("name")
            
            if process_name and "Calculator" in process_name:
                
                # If the rule matches, we create an alert.
                alert = {
                    "alert_type": "Suspicious Process Detected",
                    "process_name": process_name,
                    "pid": data.get("columns", {}).get("pid"),
                    "message": "A process matching a suspicious name was found: Calculator.app"
                }

                print(f"!!! ALERT: Found suspicious process: {process_name} !!!")

                # 4. Send the alert to our output topic.
                #    The '.send()' method is asynchronous, so we use 'await'.
                await output_topic.send(value=json.dumps(alert).encode('utf-8'))

        except json.JSONDecodeError:
            # If a message isn't valid JSON, we'll just ignore it.
            print("Warning: Could not decode message.")
        except Exception as e:
            print(f"An error occurred in the agent: {e}")

# --- To run this application ---
# 1. Make sure producer.py is running.
# 2. In a new terminal (with venv active), run:
#    faust -A detection_engine worker -l info
