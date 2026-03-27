import requests
import json
import os
import logging
from kafka import KafkaProducer
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("producer")

def run_producer():
    load_dotenv()

    API_KEY = os.getenv("API_KEY")
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")  # ← fix
    URL = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={API_KEY}"
    TOPIC = "energy_prices"

    log.info("Connexion à Kafka : %s", KAFKA_BOOTSTRAP)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    log.info("Appel API EIA...")
    response = requests.get(URL)
    response.raise_for_status()
    data = response.json()
    records = data["response"]["data"]
    log.info("%d enregistrements récupérés depuis EIA", len(records))

    sent = 0
    for r in records:
        message = {
            "period":  r["period"],
            "area":    r["area-name"],
            "product": r["product-name"],
            "process": r["process-name"],
            "series":  r["series"]
        }
        producer.send(TOPIC, value=message)
        sent += 1

    producer.flush()
    producer.close()
    log.info("Producer terminé — %d messages envoyés vers '%s'", sent, TOPIC)
    return {"messages_sent": sent}

if __name__ == "__main__":
    run_producer()