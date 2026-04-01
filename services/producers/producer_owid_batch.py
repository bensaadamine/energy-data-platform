import pandas as pd
import json
import os
import logging
from confluent_kafka import Producer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("producer_owid_batch")


def run_producer_owid_batch():
    load_dotenv()

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    TOPIC = "energy_global"

    FILE_PATH = "data/owid-energy-data.csv"

    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'acks': 'all'
    })

    df = pd.read_csv(FILE_PATH)

    # Keep only needed columns
    df = df[[
        "country",
        "year",
        "iso_code",
        "population",
        "gdp",
        "oil_consumption",
        "gas_consumption",
        "primary_energy_consumption",
        "greenhouse_gas_emissions"
    ]]


    # Remove aggregated regions
    df = df[df["iso_code"].notna()]

    # Keep rows with signal
    df = df[
        df["oil_consumption"].notna() |
        df["gas_consumption"].notna()
    ]

    total = 0

    for _, row in df.iterrows():
        message = {
            "source": "energy_global",
            "country": row["country"],
            "year": int(row["year"]),
            "population": row["population"],
            "gdp": row["gdp"],
            "oil_consumption": row["oil_consumption"],
            "gas_consumption": row["gas_consumption"],
            "energy_consumption": row["primary_energy_consumption"],
            "co2_emissions": row["greenhouse_gas_emissions"]
        }

        producer.produce(
            TOPIC,
            value=json.dumps(message).encode("utf-8")
        )
        total += 1

    producer.flush()
    log.info("OWID batch terminé — %d messages envoyés", total)

    return {"messages_sent": total}


if __name__ == "__main__":
    run_producer_owid_batch()