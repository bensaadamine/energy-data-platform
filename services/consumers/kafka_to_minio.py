import json
import uuid
from kafka import KafkaConsumer
from minio import Minio
from io import BytesIO

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
    access_key="admin",
    secret_key="password123",
    secure=False
)

bucket_name = "energy-raw"

if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

for message in consumer:

    record = message.value

    json_bytes = json.dumps(record).encode("utf-8")

    file_name = f"{uuid.uuid4()}.json"

    minio_client.put_object(
        bucket_name,
        file_name,
        BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json"
    )

    print("Stored:", file_name)