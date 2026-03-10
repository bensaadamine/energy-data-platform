from datetime import datetime
import json
import uuid
from kafka import KafkaConsumer
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv
import os

def run_kafka_to_minio_batch():
    load_dotenv()

    # Kafka consumer
    consumer = KafkaConsumer(
        "energy_prices",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    # MinIO client
    minio_client = Minio(
        "localhost:9000",
        access_key=os.getenv("MINIO_USER"),
        secret_key=os.getenv("MINIO_PASSWORD"),
        secure=False
    )

    bucket_name = "energy-raw"

    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    BATCH_SIZE = 200
    MAX_MESSAGES = 2000
    buffer = []

    count = 0

    for message in consumer:

        buffer.append(message.value)
        count += 1

        if len(buffer) >= BATCH_SIZE:

            file_name = f"energy_prices_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}.json"

            data_bytes = json.dumps(buffer).encode("utf-8")

            minio_client.put_object(
                bucket_name,
                file_name,
                BytesIO(data_bytes),
                length=len(data_bytes),
                content_type="application/json"
            )

            print(f"Stored batch with {len(buffer)} records")

            buffer = []

        if count >= MAX_MESSAGES:
            break


    if buffer:
        file_name = f"energy_prices_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}.json"

        data_bytes = json.dumps(buffer).encode("utf-8")

        minio_client.put_object(
            bucket_name,
            file_name,
            BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="application/json"
        )

        print(f"Stored final batch with {len(buffer)} records")

    consumer.close()

    print("Consumer finished successfully")

if __name__ == "__main__":
    run_kafka_to_minio_batch()