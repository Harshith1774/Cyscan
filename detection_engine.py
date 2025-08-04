import faust
import json
import joblib
import pandas as pd
import os

# --- Configuration ---
KAFKA_BROKER = 'kafka://localhost:9092'
MODEL_FILE = 'isolation_forest_model.joblib'
# The features must be in the exact same order as when we trained the model.
FEATURES = ['pid', 'ppid', 'create_time']
# NEW: We now define a threshold for our anomaly score.
# Only scores BELOW this number will trigger an alert.
# We can tune this value to make the detector more or less sensitive.
ALERT_THRESHOLD = -0.15

# --- Faust Application Setup ---
app = faust.App('cyscan-engine-ml', broker=KAFKA_BROKER, value_serializer='json')

input_topic = app.topic('osquery-events')
output_topic = app.topic('security-alerts')

# --- Load the ML Model ---
# We load the model once when the application starts.
if not os.path.exists(MODEL_FILE):
    print(f"FATAL: Model file not found at '{MODEL_FILE}'")
    print("Please run train_model.py first.")
    exit()

print(f"Loading model from {MODEL_FILE}...")
model = joblib.load(MODEL_FILE)
print("Model loaded successfully.")


# --- The Detection Logic ---
@app.agent(input_topic)
async def process_event(events):
    async for event in events:
        try:
            data = event
            columns = data.get("columns", {})

            # --- 1. Feature Preparation ---
            if all(key in columns for key in ['pid', 'parent', 'create_time']):
                feature_values = [
                    columns['pid'],
                    columns['parent'],
                    columns['create_time']
                ]
                df_point = pd.DataFrame([feature_values], columns=FEATURES)

                # --- 2. Anomaly Scoring ---
                # THE FIX IS HERE: We now get a continuous score instead of a binary prediction.
                # Lower scores mean more anomalous.
                score = model.score_samples(df_point)
                
                # --- 3. Alert Generation based on Threshold ---
                if score[0] < ALERT_THRESHOLD:
                    process_name = columns.get("name", "N/A")
                    alert = {
                        "alert_type": "Anomaly Detected (Isolation Forest)",
                        "process_name": process_name,
                        "pid": columns.get("pid"),
                        "anomaly_score": score[0],
                        "message": f"A process with a high anomaly score was detected: {process_name}"
                    }
                    # We also print the score for easier debugging.
                    print(f"!!! ANOMALY ALERT: Process '{process_name}' | Score: {score[0]:.4f} !!!")
                    await output_topic.send(value=alert)

        except Exception as e:
            print(f"An error occurred in the agent: {e}")

# --- To run this application ---
# 1. Make sure producer.py is running.
# 2. In a new terminal (with venv active), run:
#    faust -A detection_engine worker -l info
