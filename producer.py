import json
import requests
from kafka import KafkaProducer

KAFKA_BROKER_URL = "localhost:9092"
KAFKA_TOPIC = "wikimedia_topic_1"
WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )

    headers = {
        "User-Agent": "BigDataCourse/1.0 (student project)"
    }

    print("Connecting to Wikimedia stream...")
    response = requests.get(WIKIMEDIA_URL, headers=headers, stream=True)
    response.raise_for_status()

    for line in response.iter_lines():
        if line:
            decoded = line.decode("utf-8")
            if decoded.startswith("data:"):
                data = decoded.replace("data:", "").strip()
                try:
                    event = json.loads(data)
                    producer.send(KAFKA_TOPIC, event)
                except json.JSONDecodeError:
                    pass

if __name__ == "__main__":
    main()
