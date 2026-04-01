import csv
import json
import os
import logging
from confluent_kafka import Producer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("producer_petrol_batch")


def run_producer_petrol_batch():
    load_dotenv()

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    TOPIC = "fuel_prices"

    FILE_PATH = "data/Petrol.csv" 

    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'acks': 'all'
    })

    total = 0

    with open(FILE_PATH, newline='', encoding="latin-1") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            country = row.get("Country")
            price = row.get("Price Per Liter (USD)")

            if not country or not price:
                continue

            try:
                message = {
                    "source": "fuel_prices",
                    "country": country,
                    "fuel_type": "petrol",
                    "price_usd": float(price),
                    "date": "2022-06-23"  # static snapshot
                }

                producer.produce(
                    TOPIC,
                    value=json.dumps(message).encode("utf-8")
                )
                total += 1

            except ValueError:
                continue
    producer.flush()
    log.info("Petrol batch terminé — %d messages envoyés", total)

    return {"messages_sent": total}


if __name__ == "__main__":
    run_producer_petrol_batch()