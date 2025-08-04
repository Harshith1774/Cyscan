import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib
import os

# --- Configuration ---
BASELINE_CSV_FILE = 'baseline_data.csv'
MODEL_FILE = 'isolation_forest_model.joblib'
# These are the columns from our CSV that we will use to train the model.
FEATURES = ['pid', 'ppid', 'create_time']

def main():
    """
    Main function to train and save the Isolation Forest model.
    """
    print("--- Starting Model Training ---")

    # --- 1. Load Data ---
    # Check if the baseline data file exists.
    if not os.path.exists(BASELINE_CSV_FILE):
        print(f"Error: Baseline data file not found at '{BASELINE_CSV_FILE}'")
        print("Please run producer.py first to collect data.")
        return

    # Use pandas to read the CSV data into a DataFrame.
    # A DataFrame is like a powerful, in-memory spreadsheet.
    try:
        df = pd.read_csv(BASELINE_CSV_FILE)
        print(f"Successfully loaded {len(df)} records from '{BASELINE_CSV_FILE}'")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return

    # --- 2. Prepare Data ---
    # Ensure all required feature columns exist in the DataFrame.
    if not all(feature in df.columns for feature in FEATURES):
        print(f"Error: CSV file is missing one of the required columns: {FEATURES}")
        return
        
    # Select only the columns we want to use for training.
    X = df[FEATURES]

    # --- 3. Train the Model ---
    print("Training Isolation Forest model...")
    # Initialize the Isolation Forest model.
    # 'n_estimators' is the number of trees in the forest.
    # 'contamination' is an estimate of the proportion of outliers in the data. 'auto' is a good default.
    # 'random_state' ensures we get the same result every time we run it.
    model = IsolationForest(
        n_estimators=100,
        contamination='auto',
        random_state=42
    )

    # The .fit() method is where the actual training happens.
    # The model learns the patterns of "normal" from our data X.
    model.fit(X)
    print("Model training complete.")

    # --- 4. Save the Model ---
    # Use joblib to save the trained model object to a file.
    # This process is called "serialization".
    joblib.dump(model, MODEL_FILE)
    print(f"Model saved successfully to '{MODEL_FILE}'")
    print("--- Model Training Finished ---")


if __name__ == "__main__":
    main()
