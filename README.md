# Predictive SDN Dynamic Load Balancer

An end-to-end **MLOps-powered Software-Defined Networking (SDN)** project for predicting network traffic and proactively managing congestion through dynamic routing.

The project combines **network telemetry, time-series machine learning, MLOps, and SDN control** to move from reactive congestion handling toward predictive traffic engineering.

---

## Problem Statement

Traditional network load balancing is largely reactive: routing decisions are made after congestion has already developed.

This project aims to build a predictive system that can:

* Forecast near-future network traffic
* Identify links that are likely to become congested
* Support proactive traffic rerouting
* Monitor model and network performance
* Detect data/model drift
* Continuously retrain and improve the ML model

The initial forecasting horizon is **5–15 minutes ahead**.

---

## Project Objective

Build an end-to-end MLOps pipeline that connects:

```text
Network Data
     ↓
Data Ingestion & Validation
     ↓
Feature Engineering
     ↓
Model Training & Evaluation
     ↓
Model Registry
     ↓
Model Serving
     ↓
Traffic Prediction
     ↓
SDN Routing Decision
     ↓
Ryu Controller
     ↓
OpenFlow
     ↓
Open vSwitch / Mininet
```

The project will be developed incrementally, with MLOps practices introduced throughout the lifecycle rather than added only at the end.

---

## High-Level Architecture

```text
┌─────────────────────┐
│   Network / Dataset │
│  Abilene Traffic    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Data Ingestion      │
│ Validation          │
│ Preprocessing       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Feature Engineering │
│ Time-series Features│
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ ML Training         │
│ Baseline → XGBoost  │
│ → LSTM              │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ MLflow              │
│ Experiment Tracking │
│ Model Registry      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Model Serving       │
│ Prediction API      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Congestion / Traffic│
│ Prediction          │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ SDN Decision Engine │
│ Routing & Policies  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Ryu Controller      │
└──────────┬──────────┘
           │ OpenFlow
           ▼
┌─────────────────────┐
│ Open vSwitch        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Mininet Network     │
└─────────────────────┘
```

---

## Dataset

### Primary Dataset

**Abilene Internet2 Traffic Matrix Dataset**

The project uses the Abilene traffic matrix as its primary dataset for network traffic forecasting and traffic-engineering experiments.

The dataset provides traffic measurements at approximately **5-minute intervals**, making it suitable for short-term forecasting.

**Official source:** SNDlib Dynamic Traffic Matrices

**Alternative source:** Kaggle — Abilene Traffic Matrix Dataset

The dataset will be downloaded separately and will **not be committed to this repository**.

---

## Technology Stack

### Networking

* Mininet
* Open vSwitch (OVS)
* OpenFlow
* Ryu

### Machine Learning

* Python
* Pandas
* NumPy
* Scikit-learn
* XGBoost
* PyTorch

### MLOps

* MLflow
* FastAPI
* Docker
* Prometheus
* Grafana

### Development & Automation

* Git
* GitHub
* GitHub Actions
* pytest

---

## Development Approach

The project follows an incremental MLOps lifecycle.

### Phase 1 — Data Engineering

* [ ] Dataset acquisition
* [ ] Data understanding
* [ ] Data ingestion
* [ ] Data validation
* [ ] Data cleaning
* [ ] Data preprocessing
* [ ] Exploratory data analysis

### Phase 2 — Feature Engineering

* [ ] Time-series features
* [ ] Lag features
* [ ] Rolling statistics
* [ ] Traffic growth features
* [ ] Train/validation/test split

### Phase 3 — Machine Learning

* [ ] Establish baseline
* [ ] Train XGBoost model
* [ ] Train LSTM model
* [ ] Compare models
* [ ] Evaluate forecasting performance

### Phase 4 — MLOps

* [ ] Experiment tracking
* [ ] Data versioning
* [ ] Model versioning
* [ ] Model registry
* [ ] Model validation
* [ ] Model serving
* [ ] Monitoring
* [ ] Drift detection
* [ ] Automated retraining

### Phase 5 — SDN Integration

* [ ] Build Mininet topology
* [ ] Configure OVS
* [ ] Implement Ryu controller
* [ ] Collect network telemetry
* [ ] Implement baseline routing
* [ ] Implement dynamic routing

### Phase 6 — Predictive Traffic Engineering

* [ ] Integrate ML inference with the controller
* [ ] Predict future congestion
* [ ] Calculate alternative paths
* [ ] Proactively reroute traffic
* [ ] Prevent route oscillation
* [ ] Compare predictive and reactive routing

### Phase 7 — Productionization

* [ ] Containerize services
* [ ] Add CI/CD
* [ ] Add observability
* [ ] Test failure scenarios
* [ ] Document deployment
* [ ] End-to-end testing

---

## Repository Structure

```text
predictive-sdn-load-balancer/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── notebooks/
│
├── src/
│   ├── data/
│   ├── features/
│   ├── models/
│   ├── inference/
│   └── utils/
│
├── tests/
│
├── configs/
│
├── models/
│
├── scripts/
│
├── docker/
│
├── monitoring/
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Evaluation

The project will evaluate both **ML performance** and **network performance**.

### ML Metrics

* MAE
* RMSE
* MAPE / sMAPE

### Network Metrics

* Peak link utilization
* Average link utilization
* Congestion events
* Congestion duration
* Packet loss
* Latency
* Throughput
* Number of route changes
* Control-plane overhead

The predictive approach will eventually be compared against a conventional reactive routing strategy.

---

## Current Status

**Phase 1 — Data Engineering**

🚧 In Progress

The current focus is on acquiring, understanding, validating, and preparing the Abilene traffic dataset.

---

## Future Direction

The long-term goal is to demonstrate a complete feedback loop:

```text
Observe
   ↓
Predict
   ↓
Decide
   ↓
Act
   ↓
Monitor
   ↓
Learn
   ↺
```

This will enable the SDN environment to move from **reactive congestion management** toward **predictive and adaptive traffic engineering**.

---

## Project Principles

* Build incrementally
* Keep components modular
* Separate ML decisions from network safety policies
* Track experiments and models
* Validate data before training
* Monitor models after deployment
* Prefer reproducible pipelines
* Measure network impact, not only ML accuracy
* Introduce production complexity only when required

---

## License

This project is intended for educational, research, and portfolio purposes.
