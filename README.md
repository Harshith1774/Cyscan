# CyScan — Real-time Anomaly Detection from System Logs

**CyScan** is an end-to-end, real-time host monitoring and anomaly detection pipeline designed to detect suspicious activity from **system logs** using **unsupervised anomaly detection**.  
It collects live process data, analyzes it with a machine learning model, and stores generated alerts for visualization in an interactive dashboard.

---

## 📌 Overview
CyScan uses a modern, event-driven architecture inspired by scalable security data platforms.  
It ingests host process data, applies an **Isolation Forest** anomaly detection model, and pushes alerts to an **Elasticsearch–Kibana** stack for visualization.

**Data Flow**:  
```
Host System Logs
   ↓
Producer (Python)
   ↓
Kafka Message Bus
   ↓
Faust-based Detection Engine (Python)
   ↓
Kafka Message Bus
   ↓
Alert Sink (Python)
   ↓
Elasticsearch
   ↓
Kibana Dashboard
```

---

## 🛠 Architecture Components

### 1. **Containerized Infrastructure** (`docker-compose.yml`)
- Runs **Kafka**, **Zookeeper**, **Elasticsearch**, and **Kibana** inside Docker containers.
- Ensures reproducible, isolated environments for development and deployment.

### 2. **Data Producer** (`producer.py`)
- Collects live process information every 5 seconds using `psutil`.
- Logs numerical features (`pid`, `ppid`, `create_time`) to `baseline_data.csv` for model training.
- Publishes real-time events to Kafka topic `osquery-events`.

### 3. **Machine Learning Model**
- **Training Script:** (`train_model.py`)
  - Reads `baseline_data.csv`.
  - Trains an **Isolation Forest** model (`scikit-learn`) for anomaly detection.
  - Saves trained model as `isolation_forest_model.joblib`.
- **Model Type:** Isolation Forest (unsupervised, tree-based anomaly detection).

### 4. **Detection Engine** (`detection_engine.py`)
- Real-time stream processing using `faust-streaming`.
- Loads pre-trained `isolation_forest_model.joblib`.
- Consumes process events from Kafka topic `osquery-events`.
- Computes anomaly scores; if score > threshold (`-0.15`), generates alert.
- Publishes structured alerts to Kafka topic `security-alerts`.

### 5. **Alert Sink** (`alert_sink.py`)
- Consumes alerts from Kafka topic `security-alerts`.
- Indexes alerts into Elasticsearch (`cyscan-alerts` index).
- Ensures durable storage for querying & visualization.

---

## 🚀 Features
- **Real-time anomaly detection** from live system logs.
- **Unsupervised ML approach** — no labeled data needed.
- **Scalable architecture** with decoupled microservices.
- **Fully containerized** deployment with Docker.
- **Interactive visualization** via Kibana dashboard.

---

## 📦 Requirements
- **Python** ≥ 3.8
- **Docker** & **Docker Compose**
- Python dependencies:
  - `psutil`
  - `pandas`
  - `scikit-learn`
  - `faust-streaming`
  - `elasticsearch`
  - `joblib`

Install Python dependencies:
```bash
pip install -r requirements.txt
```

---

## ⚙️ Setup & Usage

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Train the Model
```bash
python train_model.py
```

### 3. Start the Data Producer
```bash
python producer.py
```

### 4. Start the Detection Engine
```bash
faust -A detection_engine worker -l info
```

### 5. Start the Alert Sink
```bash
python alert_sink.py
```

### 6. View Alerts
- Open **Kibana** at `http://localhost:5601`
- Connect to `cyscan-alerts` index
- Visualize anomalies in dashboard

---

## 📂 Project Structure
```
.
├── alert_sink.py                 # Stores alerts into Elasticsearch
├── baseline_data.csv             # Baseline process data for training
├── detection_engine.py           # Real-time anomaly detection logic
├── docker-compose.yml            # Containerized infrastructure setup
├── isolation_forest_model.joblib # Pre-trained ML model
├── producer.py                    # Collects and streams system log data
├── train_model.py                 # Model training script
└── README.md                      # Project documentation
```

---

## 📊 Example Workflow
1. **Producer** collects process data from system logs.
2. **Detection Engine** analyzes each event with the Isolation Forest model.
3. Anomalous events trigger **alerts**.
4. **Alert Sink** stores alerts in Elasticsearch.
5. **Kibana Dashboard** visualizes anomalies for review.


