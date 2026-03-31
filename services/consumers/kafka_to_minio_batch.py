import json
import uuid
import os
import logging
from datetime import datetime , UTC
from io import BytesIO
from confluent_kafka import Consumer
from minio import Minio
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("kafka_to_minio")

def run_kafka_to_minio_batch(topics=None):
    load_dotenv()
    if topics is None:
        topics = ["energy_prices", "commodity_spot_prices"]

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
    MINIO_USER      = os.getenv("MINIO_USER", "admin")
    MINIO_PASSWORD  = os.getenv("MINIO_PASSWORD", "password123")
    BUCKET          = "energy-raw"
    BATCH_SIZE      = 200
    MAX_MESSAGES    = 7000

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'energy-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000
    })
    consumer.subscribe(topics)
    log.info("Consumer abonné aux topics : %s", topics)

    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_USER,
                         secret_key=MINIO_PASSWORD, secure=False)

    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)

    buffer = []
    count = 0
    batches_written = 0
    empty_polls = 0
    MAX_EMPTY_POLLS = 5

    def flush_buffer(buf):
        nonlocal batches_written
        fname = f"energy_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
        data_bytes = json.dumps(buf).encode("utf-8")
        minio_client.put_object(BUCKET, fname, BytesIO(data_bytes),
                                length=len(data_bytes), content_type="application/json")
        batches_written += 1
        log.info("Batch écrit : %s (%d records)", fname, len(buf))

    while count < MAX_MESSAGES:
        msg = consumer.poll(timeout=5.0)

        if msg is None:
            empty_polls += 1
            log.info("Pas de message (%d/%d)", empty_polls, MAX_EMPTY_POLLS)
            if empty_polls >= MAX_EMPTY_POLLS:
                log.info("Timeout — arrêt du consumer")
                break
            continue

        if msg.error():
            log.error("Erreur consumer : %s", msg.error())
            break

        empty_polls = 0
        buffer.append(json.loads(msg.value().decode("utf-8")))
        count += 1

        if len(buffer) >= BATCH_SIZE:
            flush_buffer(buffer)
            buffer = []

    if buffer:
        flush_buffer(buffer)

    consumer.commit()
    consumer.close()
    log.info("Consumer terminé — %d messages, %d batches", count, batches_written)
    return {"messages_consumed": count, "batches_written": batches_written}

if __name__ == "__main__":
    run_kafka_to_minio_batch()