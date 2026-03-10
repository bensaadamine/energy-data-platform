import json
import psycopg2
from minio import Minio
from dotenv import load_dotenv
import os


def run_minio_to_postgres():
    load_dotenv()

    # MinIO connection
    minio_client = Minio(
        "localhost:9000",
        access_key=os.getenv("MINIO_USER"),
        secret_key=os.getenv("MINIO_PASSWORD"),
        secure=False
    )

    bucket = "energy-raw"

    # PostgreSQL connection
    conn = psycopg2.connect(
        host="localhost",
        database="energy_warehouse",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cursor = conn.cursor()


    for obj in minio_client.list_objects(bucket):

        response = minio_client.get_object(bucket, obj.object_name)

        data_bytes = response.read()

        if not data_bytes:
            print(f"Skipping empty object: {obj.object_name}")
            continue

        try:
            records = json.loads(data_bytes.decode())
        except json.JSONDecodeError:
            print(f"Skipping invalid JSON file: {obj.object_name}")
            continue

        for data in records:

            cursor.execute(
                """
                INSERT INTO energy_prices (period, area, product, process, series)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    data["period"],
                    data["area"],
                    data["product"],
                    data["process"],
                    data["series"]
                )
            )


    conn.commit()

    cursor.close()
    conn.close()

    print("Data loaded into PostgreSQL")

if __name__ == "__main__":
    run_minio_to_postgres()