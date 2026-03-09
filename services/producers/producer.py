import requests
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("API_KEY")
URL = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={API_KEY}"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

response = requests.get(URL)
data = response.json()

records = data["response"]["data"]

for r in records:

    message = {
        "period": r["period"],
        "area": r["area-name"],
        "product": r["product-name"],
        "process": r["process-name"],
        "series": r["series"]
    }

    producer.send("energy_prices", value=message)

producer.flush()

print("Messages sent to Kafka")