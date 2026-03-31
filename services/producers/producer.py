import requests
import json
import os
import logging
from confluent_kafka import Producer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("producer_eia")

def run_producer():
    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    URL = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={API_KEY}"
    TOPIC = "energy_prices"

    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP, 'acks': 'all'})

    log.info("Appel API EIA...")
    response = requests.get(URL)
    response.raise_for_status()
    records = response.json()["response"]["data"]
    log.info("%d enregistrements récupérés", len(records))

    for r in records:
        message = {
            "source": "energy_prices",
            "period": r["period"],
            "area": r["area-name"],
            "product": r["product-name"],
            "process": r["process-name"],
            "series": r["series"]
        }
        producer.produce(TOPIC, value=json.dumps(message).encode("utf-8"))

    producer.flush()
    log.info("Producer EIA terminé — %d messages envoyés", len(records))
    return {"messages_sent": len(records)}

if __name__ == "__main__":
    run_producer()