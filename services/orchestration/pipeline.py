import logging
from prefect import flow, task, get_run_logger

from services.producers.producer import run_producer
from services.producers.producer_alphavantage import run_producer_alphavantage
from services.producers.producer_news import run_producer_news
from services.consumers.kafka_to_minio_batch import run_kafka_to_minio_batch
from services.etl.minio_to_postgres import run_minio_to_postgres


# ── Producers (run in parallel) ───────────────────────────────────────────────

@task(name="producer-eia", retries=2, retry_delay_seconds=30)
def producer_eia_task():
    logger = get_run_logger()
    result = run_producer()
    logger.info("Producer EIA : %s", result)
    return result


@task(name="producer-alphavantage", retries=2, retry_delay_seconds=30)
def producer_alphavantage_task():
    logger = get_run_logger()
    result = run_producer_alphavantage()
    logger.info("Producer Alpha Vantage : %s", result)
    return result


@task(name="producer-news", retries=2, retry_delay_seconds=30)
def producer_news_task():
    logger = get_run_logger()
    result = run_producer_news()
    logger.info("Producer News : %s", result)
    return result


# ── Consumer (waits for ALL producers) ───────────────────────────────────────

@task(name="kafka-to-minio", retries=2, retry_delay_seconds=30)
def kafka_to_minio_task():
    logger = get_run_logger()
    result = run_kafka_to_minio_batch()
    logger.info("Consumer : %s", result)
    return result


# ── ETL (waits for consumer) ──────────────────────────────────────────────────

@task(name="etl", retries=1, retry_delay_seconds=60)
def etl_task():
    logger = get_run_logger()
    result = run_minio_to_postgres()
    logger.info("ETL : %s", result)
    return result


# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(name="energy-pipeline", log_prints=True)
def energy_pipeline():

    # Producers run in parallel
    r_eia   = producer_eia_task.submit()
    r_alpha = producer_alphavantage_task.submit()
    r_news  = producer_news_task.submit()

    # Consumer waits for ALL producers
    r_consumer = kafka_to_minio_task.submit(
        wait_for=[r_eia, r_alpha, r_news]
    )

    # ETL waits for consumer
    r_etl = etl_task.submit(wait_for=[r_consumer])

    return r_etl.result()


if __name__ == "__main__":
    energy_pipeline()