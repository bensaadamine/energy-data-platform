from kafka import KafkaConsumer
import json
def run_consumer():
    TOPIC = "energy_prices"
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    for message in consumer:
        print(message.value)
        
if __name__ == "__main__":
    run_consumer()