# Energy Market Intelligence Data Platform

Semester Data Engineering Project.

## Architecture

EIA API → Kafka → MinIO (Data Lake) → PostgreSQL (Warehouse) → Metabase

## Infrastructure

Services run using Docker Compose:

- Kafka (streaming)
- MinIO (data lake)
- PostgreSQL (data warehouse)
- Prefect (orchestration)
- Metabase (dashboards)

## Setup

1. Clone repository

git clone https://github.com/USER/energy-data-platform.git

2. Start infrastructure

docker compose up -d

3. Install Python dependencies

pip install -r requirements.txt

4. Run producer

python services/producer/producer.py

5. Run consumer

python services/consumer/kafka_to_minio.py

## Access Interfaces

MinIO  
http://localhost:9001

Metabase  
http://localhost:3000

Prefect  
http://localhost:4200
