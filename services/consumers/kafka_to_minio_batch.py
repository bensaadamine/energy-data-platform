import json
import uuid
import os
import logging
from datetime import datetime
from io import BytesIO
from kafka import KafkaConsumer
from minio import Minio
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("kafka_to_minio")

def run_kafka_to_minio_batch():
    load_dotenv()

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")  # ← fix
    MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "minio:9000")            # ← fix
    MINIO_USER      = os.getenv("MINIO_USER", "admin")
    MINIO_PASSWORD  = os.getenv("MINIO_PASSWORD", "password123")
    BUCKET          = "energy-raw"
    BATCH_SIZE      = 200
    MAX_MESSAGES    = 2000

    log.info("Connexion à Kafka : %s", KAFKA_BOOTSTRAP)
    consumer = KafkaConsumer(
        "energy_prices",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,   # ← stoppe si plus de messages pendant 10s
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    log.info("Connexion à MinIO : %s", MINIO_ENDPOINT)
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        log.info("Bucket '%s' créé", BUCKET)

    buffer = []
    count = 0
    batches_written = 0

    def flush_buffer(buf):
        nonlocal batches_written
        fname = f"energy_prices_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
        data_bytes = json.dumps(buf).encode("utf-8")
        minio_client.put_object(
            BUCKET, fname, BytesIO(data_bytes),
            length=len(data_bytes), content_type="application/json"
        )
        batches_written += 1
        log.info("Batch écrit dans MinIO : %s (%d records)", fname, len(buf))

    for message in consumer:
        buffer.append(message.value)
        count += 1

        if len(buffer) >= BATCH_SIZE:
            flush_buffer(buffer)
            buffer = []

        if count >= MAX_MESSAGES:
            log.info("MAX_MESSAGES atteint (%d)", MAX_MESSAGES)
            break

    if buffer:
        flush_buffer(buffer)

    consumer.close()
    log.info("Consumer terminé — %d messages, %d batches écrits", count, batches_written)
    return {"messages_consumed": count, "batches_written": batches_written}

if __name__ == "__main__":
    run_kafka_to_minio_batch()