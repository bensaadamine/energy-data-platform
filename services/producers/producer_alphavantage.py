import requests
import json
import os
import logging
from confluent_kafka import Producer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("producer_alphavantage")

COMMODITIES = {
    "WTI": "dollars per barrel",
    "NATURAL_GAS": "dollars per million BTU",
    "COPPER": "dollars per metric ton",
}

def fetch_commodity(api_key, function):
    url = f"https://www.alphavantage.co/query?function={function}&interval=monthly&apikey={api_key}"
    data = requests.get(url, timeout=15).json()
    if "data" not in data:
        log.warning("Pas de données pour %s", function)
        return []
    records = []
    for entry in data["data"]:
        if entry.get("value") in (None, ".", ""):
            continue
        try:
            records.append({
                "source": "commodity_spot_prices",
                "commodity": function,
                "date": entry["date"],
                "price": float(entry["value"]),
                "unit": COMMODITIES.get(function, "unknown")
            })
        except (ValueError, KeyError):
            continue
    log.info("  %s → %d enregistrements", function, len(records))
    return records

def run_producer_alphavantage():
    load_dotenv()
    API_KEY = os.getenv("API_KEY_ALPHA_V")
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    TOPIC = "commodity_spot_prices"

    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP, 'acks': 'all'})

    total = 0
    for function in COMMODITIES:
        for rec in fetch_commodity(API_KEY, function):
            producer.produce(TOPIC, value=json.dumps(rec).encode("utf-8"))
            total += 1

    producer.flush()
    log.info("Alpha Vantage terminé — %d messages envoyés", total)
    return {"messages_sent": total}

if __name__ == "__main__":
    run_producer_alphavantage()