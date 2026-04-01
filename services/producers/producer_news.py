import requests
import json
import os
import logging
from datetime import datetime, timedelta , UTC
from confluent_kafka import Producer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("producer_news")


def run_producer_news():
    load_dotenv()

    API_KEY = os.getenv("NEWS_API_KEY")
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    TOPIC = "energy_news"

    # last 6 hours
    from_date = (datetime.now(UTC) - timedelta(hours=6)).isoformat()

    URL = (
        f"https://newsapi.org/v2/everything?"
        f"q=energy OR oil&"
        f"from={from_date}&"
        f"sortBy=publishedAt&"
        f"apiKey={API_KEY}"
    )

    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'acks': 'all'
    })

    log.info("Appel NewsAPI...")
    response = requests.get(URL, timeout=15)
    response.raise_for_status()

    data = response.json()

    articles = data.get("articles", [])
    total = 0

    for article in articles:
        if not article.get("title"):
            continue

        message = {
            "source": "energy_news",
            "title": article["title"],
            "source_name": article["source"]["name"],
            "published_at": article["publishedAt"],
            "url": article["url"],
            "content": article.get("content")
        }

        producer.produce(
            TOPIC,
            value=json.dumps(message).encode("utf-8")
        )
        total += 1

    producer.flush()
    log.info("NewsAPI terminé — %d messages envoyés", total)

    return {"messages_sent": total}


if __name__ == "__main__":
    run_producer_news()