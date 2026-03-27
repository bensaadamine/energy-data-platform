from prefect import flow, task

from services.producers.producer import run_producer
from services.consumers.kafka_to_minio_batch import run_kafka_to_minio_batch
from services.etl.minio_to_postgres import run_minio_to_postgres


@task
def producer_task():
    run_producer()


@task
def kafka_to_minio_task():
    run_kafka_to_minio_batch()


@task
def etl_task():
    run_minio_to_postgres()


@flow
def energy_pipeline():

    producer_task()
    kafka_to_minio_task()
    etl_task()


if __name__ == "__main__":
    energy_pipeline()