# TriMet Streaming Data Pipeline

> A real-time transit analytics pipeline that streams, validates, transforms, and visualizes live TriMet bus telemetry using Google Cloud Pub/Sub and PostgreSQL.

## Overview

Public transit systems generate a huge amount of real-time telemetry, but raw vehicle data tends to be noisy, inconsistent, and hard to work with directly. This project turns live TriMet bus telemetry into clean, structured data that can actually be queried, visualized, and analyzed.

The pipeline continuously ingests live vehicle locations and stop-event data, runs automated validation and anomaly detection, computes derived metrics like vehicle speed, and loads the cleaned data into PostgreSQL. From there, the processed data powers interactive visualizations and supports service analysis, incident investigation, and operational planning.

## Features

- Real-time ingestion of live TriMet telemetry
- Streaming architecture using Google Cloud Pub/Sub
- Automated data validation and anomaly detection
- GPS integrity and odometer consistency checks
- Vehicle speed calculation from telemetry
- PostgreSQL data warehouse for structured analysis
- Interactive Mapbox visualization of bus movement and speeds
- Daily raw data archives for auditing and replay

## Architecture
        TriMet APIs
              │
              ▼
              
      Publisher Service
              │
              ▼
              
    Google Cloud Pub-Sub
              │
              ▼
              
     Subscriber Service
              │
              ▼
              
    Validation & Cleaning
              │
              ▼
              
        Speed Calculation / ETL
              │
              ▼
              
        PostgreSQL
              │
              ▼
              
  Mapbox Visualization

## How the Pipeline Works

### 1. Data Collection

The publisher continuously polls TriMet's live APIs for vehicle breadcrumb telemetry and stop event metadata, then publishes each dataset to Google Cloud Pub/Sub. That keeps producers and consumers fully decoupled from each other.

### 2. Streaming & Processing

Subscriber services consume Pub/Sub messages in batches and run a set of validation checks before anything reaches the database: missing identifier detection, GPS coordinate validation, satellite integrity checks, odometer consistency verification, service metadata validation, and timestamp normalization. Invalid records get filtered out automatically before they ever enter the pipeline.

### 3. Data Transformation

Once validated, the pipeline derives additional data, vehicle speed, standardized timestamps, route metadata, service day normalization, and direction normalization, then transforms everything into relational tables optimized for querying.

### 4. Storage

Processed data lands in PostgreSQL across two main tables:

**Trip** — route, vehicle, service day, and trip metadata.

**BreadCrumb** — timestamped GPS positions, calculated speed, and trip relationships.

Raw daily JSON archives are also kept for auditing, replay, and offline analysis.

### 5. Visualization

Cleaned telemetry gets converted into GeoJSON and rendered with Mapbox GL JS, letting users explore vehicle locations, inspect calculated speeds, spot slow corridors, and see operational trends.

## Technology Stack

**Languages**
Python

**Cloud**
Google Cloud Pub/Sub

**Database**
PostgreSQL

**Data Engineering**
Pandas, SQLAlchemy

**Parsing**
BeautifulSoup

**Visualization**
Mapbox GL JS, GeoJSON

**Configuration**
python-dotenv

## Technical Highlights

- Designed a distributed publisher/subscriber architecture for real-time data ingestion
- Implemented automated validation and anomaly detection to keep data quality high
- Calculated vehicle speed directly from telemetry and odometer readings
- Built a complete ETL pipeline from ingestion through visualization
- Used Google Cloud Pub/Sub to decouple producers and consumers
- Loaded cleaned data into PostgreSQL for efficient downstream analysis
- Generated interactive geographic visualizations with Mapbox GL JS
- Archived raw datasets for reproducibility and auditing

## Running Locally

Install dependencies:

```bash
pip install -r requirements.txt
```

Configure the required environment variables:

```text
USERNAME=
PASSWORD=
HOST=
PORT=
DB_NAME=
```

Run the publisher:

```bash
python publisher/main.py
```

Run the subscriber:

```bash
python subscriber/main.py
```

Start the visualization server:

```bash
cd map
python server.py
```

## Future Improvements

- Live operational dashboard
- Containerized deployment with Docker Compose
- Real-time analytics and alerting
- Historical trend analysis
- Interactive route filtering
- Performance metrics dashboard

## License

This repository is provided for educational and portfolio purposes.
