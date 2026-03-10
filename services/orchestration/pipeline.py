from prefect import flow, task
import subprocess
import sys

@task
def run_producer():
    print("Running EIA → Kafka producer")
    subprocess.run([sys.executable, "services/producers/producer.py"], check=True)

@task
def run_kafka_to_minio():
    print("Running Kafka → MinIO batch consumer")
    subprocess.run([sys.executable, "services/consumers/kafka_to_minio_batch.py"], check=True)


@task
def run_minio_to_postgres():
    print("Running MinIO → PostgreSQL ETL")
    subprocess.run([sys.executable, "services/etl/minio_to_postgres.py"], check=True)


@flow
def energy_pipeline():

    run_producer()

    run_kafka_to_minio()

    run_minio_to_postgres()


if __name__ == "__main__":
    energy_pipeline()